// api/job_lending_pro_api.js
// Enhanced lending job using DeFiLlama Pro API /yields/poolsBorrow endpoint
// Provides live, real-time lending market data with comprehensive metrics

module.exports.config = { runtime: 'nodejs18.x' };

const { Pool } = require('pg');
const { 
  generateJobRunId,
  validateLendingMarket,
  insertIntoScrubTable,
  updateQualitySummary 
} = require('./data_validation');

function makePoolFromEnv() {
  return new Pool({
    connectionString: process.env.SUPABASE_DB_URL,
    ssl: { rejectUnauthorized: false },
  });
}

async function ensureTables(client) {
  // Ensure the updated table structure exists
  await client.query(`
    CREATE SCHEMA IF NOT EXISTS update;
    
    -- The table should already exist with the new schema from the migration
    -- This is just a safety check
    CREATE TABLE IF NOT EXISTS update.lending_market_history (
      id SERIAL PRIMARY KEY,
      market_id TEXT NOT NULL,
      ts TIMESTAMPTZ NOT NULL,
      project TEXT,
      chain TEXT,
      symbol TEXT,
      total_supply_usd NUMERIC,
      total_borrow_usd NUMERIC,
      debt_ceiling_usd NUMERIC,
      apy_base_supply NUMERIC,
      apy_reward_supply NUMERIC,
      apy_base_borrow NUMERIC,
      apy_reward_borrow NUMERIC,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      
      -- New Pro API fields
      pool_id TEXT,
      tvl_usd NUMERIC,
      apy NUMERIC,
      apy_pct_1d NUMERIC,
      apy_pct_7d NUMERIC,
      apy_pct_30d NUMERIC,
      stablecoin BOOLEAN,
      il_risk TEXT,
      exposure TEXT,
      ltv NUMERIC,
      borrowable BOOLEAN,
      mu NUMERIC,
      sigma NUMERIC,
      count INTEGER,
      outlier BOOLEAN,
      apy_mean_30d NUMERIC,
      predictions JSONB,
      reward_tokens JSONB,
      underlying_tokens JSONB,
      pool_meta TEXT,
      data_timestamp TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  
  // Add unique constraint if missing
  await client.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uniq_pool_data_timestamp'
          AND conrelid = 'update.lending_market_history'::regclass
      ) THEN
        ALTER TABLE update.lending_market_history
        ADD CONSTRAINT uniq_pool_data_timestamp UNIQUE (pool_id, data_timestamp);
      END IF;
    END$$;
  `);
}

async function upsertLendingData(client, lendingData) {
  const query = `
    INSERT INTO update.lending_market_history (
      market_id, ts, project, chain, symbol,
      total_supply_usd, total_borrow_usd, debt_ceiling_usd,
      apy_base_supply, apy_reward_supply, apy_base_borrow, apy_reward_borrow,
      pool_id, tvl_usd, apy, apy_pct_1d, apy_pct_7d, apy_pct_30d,
      stablecoin, il_risk, exposure, ltv, borrowable,
      mu, sigma, count, outlier, apy_mean_30d,
      predictions, reward_tokens, underlying_tokens, pool_meta,
      data_timestamp
    )
    VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
      $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28,
      $29, $30, $31, $32, $33
    )
    ON CONFLICT (pool_id, data_timestamp) DO UPDATE SET
      market_id = EXCLUDED.market_id,
      ts = EXCLUDED.ts,
      project = EXCLUDED.project,
      chain = EXCLUDED.chain,
      symbol = EXCLUDED.symbol,
      total_supply_usd = EXCLUDED.total_supply_usd,
      total_borrow_usd = EXCLUDED.total_borrow_usd,
      debt_ceiling_usd = EXCLUDED.debt_ceiling_usd,
      apy_base_supply = EXCLUDED.apy_base_supply,
      apy_reward_supply = EXCLUDED.apy_reward_supply,
      apy_base_borrow = EXCLUDED.apy_base_borrow,
      apy_reward_borrow = EXCLUDED.apy_reward_borrow,
      tvl_usd = EXCLUDED.tvl_usd,
      apy = EXCLUDED.apy,
      apy_pct_1d = EXCLUDED.apy_pct_1d,
      apy_pct_7d = EXCLUDED.apy_pct_7d,
      apy_pct_30d = EXCLUDED.apy_pct_30d,
      stablecoin = EXCLUDED.stablecoin,
      il_risk = EXCLUDED.il_risk,
      exposure = EXCLUDED.exposure,
      ltv = EXCLUDED.ltv,
      borrowable = EXCLUDED.borrowable,
      mu = EXCLUDED.mu,
      sigma = EXCLUDED.sigma,
      count = EXCLUDED.count,
      outlier = EXCLUDED.outlier,
      apy_mean_30d = EXCLUDED.apy_mean_30d,
      predictions = EXCLUDED.predictions,
      reward_tokens = EXCLUDED.reward_tokens,
      underlying_tokens = EXCLUDED.underlying_tokens,
      pool_meta = EXCLUDED.pool_meta,
      data_timestamp = EXCLUDED.data_timestamp;
  `;
  
  const values = [
    lendingData.market_id || lendingData.pool_id, // market_id (use pool_id as fallback)
    lendingData.data_timestamp, // ts
    lendingData.project,
    lendingData.chain,
    lendingData.symbol,
    lendingData.total_supply_usd,
    lendingData.total_borrow_usd,
    lendingData.debt_ceiling_usd,
    lendingData.apy_base_supply,
    lendingData.apy_reward_supply,
    lendingData.apy_base_borrow,
    lendingData.apy_reward_borrow,
    lendingData.pool_id,
    lendingData.tvl_usd,
    lendingData.apy,
    lendingData.apy_pct_1d,
    lendingData.apy_pct_7d,
    lendingData.apy_pct_30d,
    lendingData.stablecoin,
    lendingData.il_risk,
    lendingData.exposure,
    lendingData.ltv,
    lendingData.borrowable,
    lendingData.mu,
    lendingData.sigma,
    lendingData.count,
    lendingData.outlier,
    lendingData.apy_mean_30d,
    lendingData.predictions ? JSON.stringify(lendingData.predictions) : null,
    lendingData.reward_tokens ? JSON.stringify(lendingData.reward_tokens) : null,
    lendingData.underlying_tokens ? JSON.stringify(lendingData.underlying_tokens) : null,
    lendingData.pool_meta,
    lendingData.data_timestamp
  ];
  
  await client.query(query, values);
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

  const API_KEY = process.env.DEFILLAMA_API_KEY;
  if (!API_KEY) {
    return res.status(500).json({ 
      success: false, 
      error: "Missing DEFILLAMA_API_KEY" 
    });
  }

  // Batching controls
  const { offset = 0, limit = 500 } = req.query || {};
  const offsetNum = parseInt(offset, 10);
  const limitNum = parseInt(limit, 10);

  try {
    console.log(`🏦 Starting Pro API lending collection job: ${jobRunId}`);
    console.log(`📦 Batch: offset=${offsetNum}, limit=${limitNum}`);

    // Fetch live lending data from Pro API
    const apiUrl = `https://pro-api.llama.fi/${API_KEY}/yields/poolsBorrow`;
    console.log(`📡 Fetching live lending data from: ${apiUrl}`);

    const response = await fetch(apiUrl);
    if (!response.ok) {
      throw new Error(`API request failed with status ${response.status}: ${response.statusText}`);
    }

    const data = await response.json();
    if (!data.data || !Array.isArray(data.data)) {
      throw new Error('Invalid API response structure');
    }

    // Apply batching to the lending pools
    const allPools = data.data;
    const pools = allPools.slice(offsetNum, offsetNum + limitNum);
    totalRecords = pools.length;

    console.log(`📊 Processing batch: ${pools.length} lending pools (${offsetNum}-${offsetNum + limitNum - 1} of ${allPools.length} total)`);

    const pool = makePoolFromEnv();
    const client = await pool.connect();

    try {
      await ensureTables(client);
      
      const currentTimestamp = new Date().toISOString();
      
      for (const poolData of pools) {
        try {
          // Map Pro API data to our enhanced schema
          const lendingData = {
            market_id: poolData.pool, // Use pool ID as market ID
            pool_id: poolData.pool,
            ts: currentTimestamp,
            data_timestamp: currentTimestamp,
            project: poolData.project,
            chain: poolData.chain,
            symbol: poolData.symbol,
            
            // Financial metrics
            total_supply_usd: poolData.totalSupplyUsd,
            total_borrow_usd: poolData.totalBorrowUsd,
            debt_ceiling_usd: poolData.debtCeilingUsd,
            tvl_usd: poolData.tvlUsd,
            
            // APY metrics
            apy: poolData.apy,
            apy_base_supply: poolData.apyBase, // Supply APY
            apy_reward_supply: poolData.apyReward, // Supply reward APY
            apy_base_borrow: poolData.apyBaseBorrow,
            apy_reward_borrow: poolData.apyRewardBorrow,
            apy_pct_1d: poolData.apyPct1D,
            apy_pct_7d: poolData.apyPct7D,
            apy_pct_30d: poolData.apyPct30D,
            apy_mean_30d: poolData.apyMean30d,
            
            // Risk and classification
            stablecoin: poolData.stablecoin,
            il_risk: poolData.ilRisk,
            exposure: poolData.exposure,
            outlier: poolData.outlier,
            
            // Lending-specific
            ltv: poolData.ltv,
            borrowable: poolData.borrowable,
            
            // Statistical
            mu: poolData.mu,
            sigma: poolData.sigma,
            count: poolData.count,
            
            // Complex data as JSONB
            predictions: poolData.predictions,
            reward_tokens: poolData.rewardTokens,
            underlying_tokens: poolData.underlyingTokens,
            pool_meta: poolData.poolMeta,
          };

          // Validate the data
          const validation = validateLendingMarket(lendingData);
          
          if (validation.isValid) {
            // Clean data - goes to main table
            await upsertLendingData(client, lendingData);
            cleanRecords++;
          } else {
            // Invalid data - goes to scrub table
            await insertIntoScrubTable(
              client,
              'lending_market_scrub',
              lendingData,
              validation,
              jobRunId,
              poolData
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
          
        } catch (poolError) {
          errorRecords++;
          errorSummary['pool_processing_error'] = (errorSummary['pool_processing_error'] || 0) + 1;
          console.error(`Error processing pool ${poolData.pool}:`, poolError.message);
        }
      }
      
    } finally {
      client.release();
    }
    
    const processingTime = Date.now() - startTime;
    const successRate = totalRecords > 0 ? (cleanRecords / totalRecords) * 100 : 0;
    
    // Update quality summary
    try {
      const pool = makePoolFromEnv();
      const client = await pool.connect();
      try {
        await updateQualitySummary(client, 'job_lending_pro_api', jobRunId, {
          totalRecords,
          cleanRecords,
          scrubbedRecords,
          errorRecords,
          outlierRecords,
          processingTimeMs: processingTime,
          errorSummary
        });
      } finally {
        client.release();
      }
    } catch (summaryError) {
      console.warn('Quality summary update failed:', summaryError.message);
    }
    
    console.log(`✅ Pro API lending collection completed successfully`);
    console.log(`📊 Results: ${totalRecords} total, ${cleanRecords} clean, ${scrubbedRecords} scrubbed`);
    console.log(`⏱️  Processing time: ${processingTime}ms`);
    console.log(`🎯 Success rate: ${successRate.toFixed(1)}%`);

    res.status(200).json({
      success: true,
      jobRunId,
      metrics: {
        totalRecords,
        cleanRecords,
        scrubbedRecords,
        errorRecords,
        outlierRecords,
        successRate: parseFloat(successRate.toFixed(1)),
        avgQualityScore: successRate,
        processingTime,
        poolsProcessed: pools.length,
        batchInfo: {
          offset: offsetNum,
          limit: limitNum,
          totalPools: allPools.length
        }
      },
      message: `Processed ${pools.length} lending pools with ${successRate.toFixed(1)}% success rate using Pro API`
    });

  } catch (error) {
    const processingTime = Date.now() - startTime;
    console.error(`❌ Pro API lending collection job failed:`, error.message);
    
    res.status(500).json({
      success: false,
      jobRunId,
      error: error.message,
      metrics: {
        totalRecords,
        cleanRecords,
        scrubbedRecords,
        errorRecords,
        processingTime
      }
    });
  }
};
