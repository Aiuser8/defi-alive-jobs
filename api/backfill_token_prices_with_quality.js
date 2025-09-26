// api/backfill_token_prices_with_quality.js (CommonJS)
// Live token price collection with data quality gates and scrub table routing
// UPDATED: Now uses current prices endpoint for real-time data
// Force Node runtime (pg not supported on Edge)
module.exports.config = { runtime: 'nodejs18.x' };

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const {
  generateJobRunId,
  validateTokenPrice,
  getPreviousPrice,
  insertIntoScrubTable,
  updateQualitySummary
} = require('./data_validation');

const EVM_CHAINS = new Set([
  'ethereum','unichain','base','arbitrum','optimism','polygon','bsc','avalanche',
  'linea','scroll','blast','fantom','celo','gnosis','zksync era','metis','mantle','aurora'
]);

function isValidAddress(chain, address) {
  if (!address || typeof address !== 'string') return false;
  const a = address.trim();
  if (EVM_CHAINS.has(chain)) return /^0x[0-9a-fA-F]{40}$/.test(a);
  return /^[A-Za-z0-9:_-]{4,}$/.test(a); // non-EVM: basic sanity, let API decide
}

function chunk(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

function yesterdayMidnightUtcUnix() {
  const now = new Date();
  const todayMidnight = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  return Math.floor((todayMidnight - 24*60*60*1000) / 1000);
}

function currentTimestampUnix() {
  return Math.floor(Date.now() / 1000);
}

async function fetchCurrentPricesForBatch(coinIds, apiKey) {
  const coinsParam = encodeURIComponent(coinIds.join(','));
  const url = `https://pro-api.llama.fi/${apiKey}/coins/chart/${coinsParam}`;
  const res = await fetch(url); // Node 18+ global fetch
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText}${text ? ` | ${text}` : ''}`);
  }
  return res.json(); // { coins: { "<id>": { symbol, decimals, confidence, prices: [{timestamp, price}] } } }
}

async function upsertCleanBatch(client, records) {
  for (const r of records) {
    const tsIso = new Date(r.tsSec * 1000).toISOString();
    await client.query(
      `INSERT INTO "update".token_price_daily
         (coin_id, symbol, confidence, decimals, price_timestamp, price_usd)
       VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT (coin_id, price_timestamp)
       DO UPDATE SET
         price_usd  = EXCLUDED.price_usd,
         confidence = EXCLUDED.confidence,
         decimals   = EXCLUDED.decimals,
         symbol     = EXCLUDED.symbol`,
      [r.coinId, r.symbol, r.confidence, r.decimals, tsIso, r.price]
    );
  }
}

// Build a pg Pool from SUPABASE_DB_URL or PG* parts
function makePoolFromEnv() {
  const { SUPABASE_DB_URL } = process.env;
  if (SUPABASE_DB_URL) {
    return new Pool({
      connectionString: SUPABASE_DB_URL,
      ssl: { rejectUnauthorized: false },
      statement_timeout: 0,
      query_timeout: 0
    });
  }
  const {
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE
  } = process.env;

  if (!PGHOST || !PGPORT || !PGDATABASE || !PGUSER || !PGPASSWORD) {
    throw new Error('Missing DB env: need SUPABASE_DB_URL or PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD');
  }

  const sslRequired = (PGSSLMODE || '').toLowerCase() === 'require';
  return new Pool({
    host: PGHOST,
    port: Number(PGPORT),
    database: PGDATABASE,
    user: PGUSER,
    password: PGPASSWORD,
    ssl: sslRequired ? { rejectUnauthorized: false } : undefined,
    statement_timeout: 0,
    query_timeout: 0
  });
}

module.exports = async (req, res) => {
  const jobRunId = generateJobRunId();
  const startTime = Date.now();
  
  // Quality metrics tracking
  let totalRecords = 0;
  let cleanRecords = 0;
  let scrubbedRecords = 0;
  let errorRecords = 0;
  let outlierRecords = 0;
  const errorSummary = {};

  try {
    const { DEFILLAMA_API_KEY } = process.env;
    if (!DEFILLAMA_API_KEY) {
      return res.status(500).json({ error: 'Missing DEFILLAMA_API_KEY' });
    }

    // Parse offset/limit
    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const offset = Math.max(0, parseInt(url.searchParams.get('offset') || '0', 10));
    const limit  = Math.max(1, Math.min(2000, parseInt(url.searchParams.get('limit') || '500', 10)));

    // Load ACTIVE token list (filtered for tokens with fresh price data)
    const filePath = path.join(process.cwd(), 'token_list_active.json');
    const entries = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
    const slice = entries.slice(offset, offset + limit);

    if (!slice.length) {
      return res.status(200).json({ message: 'No tokens in this slice.', offset, limit });
    }

    // Build coin IDs
    const coinIds = [];
    let skipped = 0;
    for (const item of slice) {
      const chain = String(item.chain || '').trim().toLowerCase();
      const address = String(item.address || '').trim();
      if (!chain || !isValidAddress(chain, address)) { skipped++; continue; }
      const addrForSlug = EVM_CHAINS.has(chain) ? address.toLowerCase() : address;
      coinIds.push(`${chain}:${addrForSlug}`);
    }
    if (!coinIds.length) {
      return res.status(200).json({ message: 'All tokens in slice invalid.', offset, limit, skipped });
    }

    const BATCH_SIZE = 25;
    const SLEEP_MS = 120;
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));

    const pool = makePoolFromEnv();
    const client = await pool.connect();
    
    try {
      // Ensure scrub tables exist
      await client.query(`
        CREATE SCHEMA IF NOT EXISTS scrub;
        CREATE TABLE IF NOT EXISTS scrub.token_price_scrub (
          id SERIAL PRIMARY KEY,
          coin_id TEXT NOT NULL,
          symbol TEXT,
          price_usd NUMERIC,
          confidence DECIMAL(3,2),
          decimals INTEGER,
          price_timestamp TIMESTAMPTZ,
          validation_errors TEXT[],
          quality_score INTEGER,
          is_outlier BOOLEAN DEFAULT FALSE,
          outlier_reason TEXT,
          original_data JSONB,
          processed_at TIMESTAMPTZ DEFAULT NOW(),
          job_run_id TEXT,
          retry_count INTEGER DEFAULT 0
        );
      `);

      for (const group of chunk(coinIds, BATCH_SIZE)) {
        try {
          const data = await fetchCurrentPricesForBatch(group, DEFILLAMA_API_KEY);
          const nodes = data?.coins || {};
          const cleanBatch = [];
          const scrubBatch = [];

          for (const id of group) {
            totalRecords++;
            const node = nodes[id];
            
            // Chart endpoint returns prices array, get the latest price
            const latestPrice = node?.prices?.[node.prices.length - 1];
            
        if (!node || !latestPrice || typeof latestPrice.price !== 'number') {
          // No data from API - insert into scrub table
          errorRecords++;
          errorSummary['no_api_data'] = (errorSummary['no_api_data'] || 0) + 1;
          
          // Create scrub data for tokens with no API data
          const noDataScrub = {
            coinId: id,
            symbol: null,
            confidence: null,
            decimals: null,
            tsSec: Math.floor(Date.now() / 1000),
            price: null,
            timestamp: Math.floor(Date.now() / 1000)
          };
          
          const noDataValidation = {
            isValid: false,
            errors: ['no_api_data'],
            qualityScore: 0,
            isOutlier: false,
            outlierReason: 'No price data available from API'
          };
          
          try {
            await insertIntoScrubTable(client, 'token_price_scrub', noDataScrub, noDataValidation, jobRunId, noDataScrub);
            scrubbedRecords++;
          } catch (scrubError) {
            console.error(`Failed to insert no-data token into scrub:`, scrubError.message);
          }
          continue;
        }

            const tsSec = Number.isFinite(latestPrice.timestamp) ? latestPrice.timestamp : Math.floor(Date.now() / 1000);
            const priceData = {
              coinId: id,
              symbol: node.symbol ?? null,
              confidence: node.confidence ?? null,
              decimals: node.decimals ?? null,
              tsSec,
              price: latestPrice.price,
              timestamp: tsSec
            };

            // Get previous price for outlier detection
            const previousPrice = await getPreviousPrice(client, id, tsSec);
            
            // Validate the data
            const validation = validateTokenPrice(priceData, previousPrice);
            
            if (validation.isValid) {
              // Clean data - goes to main table
              cleanBatch.push(priceData);
              cleanRecords++;
            } else {
              // Invalid data - goes to scrub table
              await insertIntoScrubTable(
                client, 
                'token_price_scrub', 
                priceData, 
                validation, 
                jobRunId, 
                node
              );
              
              scrubbedRecords++;
              
              // Track error types
              validation.errors.forEach(error => {
                errorSummary[error] = (errorSummary[error] || 0) + 1;
              });
              
              if (validation.isOutlier) {
                outlierRecords++;
              }
            }
          }

          // Insert clean data into main table
          if (cleanBatch.length) {
            await upsertCleanBatch(client, cleanBatch);
          }

        } catch (e) {
          // Whole group failed
          errorRecords += group.length;
          errorSummary['api_fetch_error'] = (errorSummary['api_fetch_error'] || 0) + group.length;
          console.error(`Batch failed for group:`, group, e.message);
        }
        
        await sleep(SLEEP_MS);
      }

      // Update quality summary
      await updateQualitySummary(client, 'backfill_token_prices', jobRunId, {
        totalRecords,
        cleanRecords,
        scrubbedRecords,
        errorRecords,
        outlierRecords,
        processingTimeMs: Date.now() - startTime,
        errorSummary
      });

    } finally {
      client.release();
      await pool.end();
    }

    // Consider job successful if we processed some tokens (even if some went to scrub)
    const jobSuccess = totalRecords > 0 && (cleanRecords > 0 || scrubbedRecords > 0);
    
    return res.status(200).json({
      success: jobSuccess,
      job_run_id: jobRunId,
      date: new Date().toISOString(),
      offset, limit,
      
      // Quality metrics
      total_records: totalRecords,
      clean_records: cleanRecords,
      scrubbed_records: scrubbedRecords,
      error_records: errorRecords,
      outlier_records: outlierRecords,
      overall_quality_score: totalRecords > 0 ? (cleanRecords / totalRecords) * 100 : 0,
      
      // Processing time
      processing_time_ms: Date.now() - startTime,
      
      // Error summary
      error_summary: errorSummary,
      
      // Success message
      message: jobSuccess ? 
        `Successfully processed ${totalRecords} tokens (${cleanRecords} clean, ${scrubbedRecords} scrubbed)` :
        `Failed to process any tokens successfully`
    });
    
  } catch (e) {
    // Update quality summary with error
    try {
      const pool = makePoolFromEnv();
      const client = await pool.connect();
      await updateQualitySummary(client, 'backfill_token_prices', jobRunId, {
        totalRecords,
        cleanRecords,
        scrubbedRecords,
        errorRecords: errorRecords + 1,
        outlierRecords,
        processingTimeMs: Date.now() - startTime,
        errorSummary: { ...errorSummary, fatal_error: e.message }
      });
      client.release();
      await pool.end();
    } catch (summaryError) {
      console.error('Failed to update quality summary:', summaryError.message);
    }
    
    return res.status(500).json({ 
      success: false,
      job_run_id: jobRunId,
      error: e.message, 
      stack: e.stack 
    });
  }
};
