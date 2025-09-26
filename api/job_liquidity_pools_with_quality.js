// api/job_liquidity_pools_with_quality.js - Liquidity pool data collection with quality gates
// Collects pool data from DeFiLlama Pro API with comprehensive validation
module.exports.config = { runtime: 'nodejs18.x' };

require('dotenv').config();
const { Pool } = require('pg');
const {
  generateJobRunId,
  insertIntoScrubTable,
  updateQualitySummary
} = require('./data_validation');

const API_KEY = 'f162bf7f5a9432db5b75f30ebabc2d8d6b94cc51297549662caf571f4ad307ca';

function makePoolFromEnv() {
  const {
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE
  } = process.env;

  if (!PGHOST || !PGPORT || !PGDATABASE || !PGUSER || !PGPASSWORD) {
    throw new Error('Missing DB env: need PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD');
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

/**
 * Validate liquidity pool data
 */
function validatePoolData(poolData) {
  const errors = [];
  let qualityScore = 100;
  let isOutlier = false;
  let outlierReason = null;

  // Basic field validation
  if (!poolData.pool) {
    errors.push('missing_pool_id');
    qualityScore -= 40;
  }

  if (!poolData.project) {
    errors.push('missing_project');
    qualityScore -= 20;
  }

  if (!poolData.chain) {
    errors.push('missing_chain');
    qualityScore -= 20;
  }

  // TVL validation
  if (typeof poolData.tvlUsd !== 'number' || poolData.tvlUsd <= 0) {
    errors.push('invalid_tvl');
    qualityScore -= 30;
  } else {
    const tvl = poolData.tvlUsd;
    
    // Negative TVL is impossible
    if (tvl < 0) {
      errors.push('negative_tvl');
      qualityScore -= 50;
    }
    
    // TVL > $100B is suspicious (but possible for major protocols)
    if (tvl > 100000000000) {
      isOutlier = true;
      outlierReason = `extremely_high_tvl_${(tvl / 1000000000).toFixed(1)}B`;
      qualityScore -= 10;
    }
    
    // Very low TVL might be test pools
    if (tvl < 1000) {
      qualityScore -= 5;
    }
  }

  // APY validation - much more lenient than before
  if (typeof poolData.apy === 'number') {
    const apy = poolData.apy;
    
    // Negative APY is possible but unusual
    if (apy < -50) {
      errors.push('extreme_negative_apy');
      qualityScore -= 20;
    }
    
    // APY > 500% (5x) is suspicious but possible for new/volatile pools
    if (apy > 500) {
      errors.push('extreme_apy_high');
      isOutlier = true;
      outlierReason = `extreme_apy_${apy.toFixed(1)}%`;
      qualityScore -= 30;
    }
    
    // APY > 10000% (100x) is almost certainly wrong
    if (apy > 10000) {
      errors.push('impossible_apy');
      qualityScore -= 60;
    }
  }

  // Base APY validation
  if (typeof poolData.apyBase === 'number' && poolData.apyBase > 1000) {
    errors.push('extreme_base_apy');
    qualityScore -= 20;
  }

  // Reward APY validation  
  if (typeof poolData.apyReward === 'number' && poolData.apyReward > 5000) {
    errors.push('extreme_reward_apy');
    qualityScore -= 20;
  }

  // Data freshness check (pools should be relatively current)
  const now = Date.now();
  if (poolData.lastUpdated && typeof poolData.lastUpdated === 'number') {
    const ageHours = (now - poolData.lastUpdated * 1000) / (1000 * 60 * 60);
    if (ageHours > 48) {
      errors.push('stale_pool_data');
      qualityScore -= 15;
    }
  }

  return {
    isValid: errors.length === 0 && qualityScore >= 60,
    errors,
    qualityScore: Math.max(0, qualityScore),
    isOutlier,
    outlierReason
  };
}

async function insertCleanPool(client, poolData) {
  const timestamp = Date.now();
  await client.query(`
    INSERT INTO update.cl_pool_hist (
      pool_id, ts, project, chain, symbol, tvl_usd, apy, apy_base, url, inserted_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    ON CONFLICT (pool_id, ts) DO UPDATE SET
      tvl_usd = EXCLUDED.tvl_usd,
      apy = EXCLUDED.apy,
      apy_base = EXCLUDED.apy_base,
      inserted_at = EXCLUDED.inserted_at
  `, [
    poolData.pool,
    Math.floor(timestamp / 1000), // Unix timestamp
    poolData.project,
    poolData.chain,
    poolData.symbol,
    poolData.tvlUsd,
    poolData.apy,
    poolData.apyBase,
    poolData.url || null,
    new Date().toISOString()
  ]);
}

module.exports = async (req, res) => {
  const jobRunId = generateJobRunId();
  const startTime = Date.now();
  
  // Parse query parameters for batching
  const { offset = 0, limit = 500 } = req.query || {};
  const offsetNum = parseInt(offset, 10);
  const limitNum = parseInt(limit, 10);
  
  // Quality metrics tracking
  let totalRecords = 0;
  let cleanRecords = 0;
  let scrubbedRecords = 0;
  let errorRecords = 0;
  let outlierRecords = 0;
  const errorSummary = {};

  try {
    console.log(`🏊 Starting liquidity pools collection job: ${jobRunId}`);
    console.log(`📦 Batch: offset=${offsetNum}, limit=${limitNum}`);
    
    // Fetch pool data from DeFiLlama Pro API
    const apiUrl = `https://pro-api.llama.fi/${API_KEY}/yields/pools`;
    console.log(`📡 Fetching pool data from: ${apiUrl}`);
    
    const response = await fetch(apiUrl);
    if (!response.ok) {
      throw new Error(`API request failed: ${response.status} ${response.statusText}`);
    }
    
    const data = await response.json();
    if (!data.data || !Array.isArray(data.data)) {
      throw new Error('Invalid API response format');
    }
    
    // Apply batching
    const allPools = data.data;
    const pools = allPools.slice(offsetNum, offsetNum + limitNum);
    totalRecords = pools.length;
    
    console.log(`📊 Processing batch: ${pools.length} pools (${offsetNum}-${offsetNum + limitNum - 1} of ${allPools.length} total)`);
    
    const pool = makePoolFromEnv();
    const client = await pool.connect();
    
    try {
      await client.query('BEGIN');
      
      for (const poolData of pools) {
        try {
          // Validate pool data
          const validation = validatePoolData(poolData);
          
          if (validation.isValid) {
            // Insert clean data
            await insertCleanPool(client, poolData);
            cleanRecords++;
            
            if (validation.isOutlier) {
              outlierRecords++;
            }
          } else {
            // Insert into scrub table
            await insertIntoScrubTable(
              client,
              'cl_pool_hist_scrub',
              {
                pool_id: poolData.pool,
                ts: Math.floor(Date.now() / 1000),
                project: poolData.project,
                chain: poolData.chain,
                symbol: poolData.symbol,
                tvl_usd: poolData.tvlUsd,
                apy: poolData.apy,
                apy_base: poolData.apyBase,
                url: poolData.url
              },
              validation,
              jobRunId,
              poolData
            );
            
            scrubbedRecords++;
            
            if (validation.isOutlier) {
              outlierRecords++;
            }
            
            // Track error types
            validation.errors.forEach(error => {
              errorSummary[error] = (errorSummary[error] || 0) + 1;
              errorRecords++;
            });
          }
        } catch (error) {
          console.error(`Failed to process pool ${poolData.pool}:`, error.message);
          errorRecords++;
          errorSummary['processing_error'] = (errorSummary['processing_error'] || 0) + 1;
        }
      }
      
      // Update quality summary
      const processingTime = Date.now() - startTime;
      const overallQualityScore = totalRecords > 0 ? (cleanRecords / totalRecords) * 100 : 0;
      
      await updateQualitySummary(client, 'liquidity_pools', jobRunId, {
        totalRecords,
        cleanRecords,
        scrubbedRecords,
        errorRecords,
        outlierRecords,
        overallQualityScore,
        processingTime,
        errorSummary
      });
      
      await client.query('COMMIT');
      
      console.log(`✅ Pool collection completed:`, {
        totalRecords,
        cleanRecords,
        scrubbedRecords,
        qualityScore: overallQualityScore.toFixed(2),
        processingTime
      });
      
      res.status(200).json({
        success: true,
        jobRunId,
        metrics: {
          totalRecords,
          cleanRecords,
          scrubbedRecords,
          errorRecords,
          outlierRecords,
          qualityScore: overallQualityScore,
          processingTimeMs: processingTime
        },
        message: `Processed ${totalRecords} pools, ${cleanRecords} clean, ${scrubbedRecords} scrubbed`
      });
      
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
      await pool.end();
    }
    
  } catch (error) {
    console.error('💥 Pool collection job failed:', error.message);
    console.error('Stack:', error.stack);
    
    res.status(500).json({
      success: false,
      jobRunId,
      error: error.message,
      metrics: {
        totalRecords,
        cleanRecords,
        scrubbedRecords,
        errorRecords
      }
    });
  }
};
