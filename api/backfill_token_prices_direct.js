// api/backfill_token_prices_direct.js
// Direct token price collection - EXACT COPY of working job without scrubbing logic
// Force Node runtime (pg not supported on Edge)
module.exports.config = { runtime: 'nodejs18.x' };

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

function generateJobRunId() {
  return `job_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

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
  
  // Simple metrics tracking
  let totalRecords = 0;
  let insertedRecords = 0;
  let skippedRecords = 0;

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
      // No scrub tables needed - direct insertion only

      for (const group of chunk(coinIds, BATCH_SIZE)) {
        try {
          const data = await fetchCurrentPricesForBatch(group, DEFILLAMA_API_KEY);
          const nodes = data?.coins || {};
          const validRecords = [];

          for (let i = 0; i < group.length; i++) {
            const id = group[i];
            totalRecords++;
            const node = nodes[id];
            
            // Chart endpoint returns prices array, get the latest price
            const latestPrice = node?.prices?.[node.prices.length - 1];
            
            // Skip if no valid price data
            if (!node || !latestPrice || typeof latestPrice.price !== 'number' || latestPrice.price <= 0) {
              skippedRecords++;
              continue;
            }

            // Add microsecond offset to prevent timestamp collisions from parallel batches
            // Use current time with microsecond offset to guarantee unique timestamps
            const nowMs = Date.now();
            const tsSec = (nowMs / 1000) + (i * 0.001); // Current time + millisecond offset per token
            
            const priceData = {
              coinId: id,
              symbol: node.symbol ?? null,
              confidence: node.confidence ?? null,
              decimals: node.decimals ?? null,
              tsSec,
              price: latestPrice.price,
              timestamp: tsSec
            };

            validRecords.push(priceData);
          }

          // Insert all valid records directly into main table
          if (validRecords.length) {
            await upsertCleanBatch(client, validRecords);
            insertedRecords += validRecords.length;
          }

        } catch (e) {
          // Whole group failed
          skippedRecords += group.length;
          console.error(`Batch failed for group:`, group, e.message);
        }
        
        await sleep(SLEEP_MS);
      }

    } finally {
      client.release();
      await pool.end();
    }

    // Consider job successful if we inserted some records
    const jobSuccess = insertedRecords > 0;
    
    return res.status(200).json({
      success: jobSuccess,
      job_run_id: jobRunId,
      date: new Date().toISOString(),
      offset, limit,
      
      // Simple metrics
      total_records: totalRecords,
      inserted_records: insertedRecords,
      skipped_records: skippedRecords,
      success_rate: totalRecords > 0 ? (insertedRecords / totalRecords) * 100 : 0,
      
      // Processing time
      processing_time_ms: Date.now() - startTime,
      
      // Success message
      message: jobSuccess ? 
        `Successfully inserted ${insertedRecords}/${totalRecords} token prices (${skippedRecords} skipped)` :
        `Failed to insert any token prices`
    });
    
  } catch (e) {
    console.error('Token price job failed:', e.message);
    
    return res.status(500).json({ 
      success: false,
      job_run_id: jobRunId,
      error: e.message,
      total_records: totalRecords,
      inserted_records: insertedRecords,
      skipped_records: skippedRecords,
      processing_time_ms: Date.now() - startTime
    });
  }
};