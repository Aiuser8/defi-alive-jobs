// api/backfill_liquidity_pools_historical.js - Backfill historical pool data gaps
// Fills the gap between historical data (Sep 5) and current live collection (Sep 26)
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
 * Validate historical pool data - more lenient than live data
 */
function validateHistoricalPoolData(poolData, timestamp) {
  const errors = [];
  let qualityScore = 100;
  let isOutlier = false;
  let outlierReason = null;

  // Basic validation
  if (typeof poolData.tvlUsd !== 'number' || poolData.tvlUsd < 0) {
    errors.push('invalid_tvl');
    qualityScore -= 30;
  }

  // APY validation - historical data can be more volatile
  if (typeof poolData.apy === 'number') {
    const apy = poolData.apy;
    
    if (apy < -100) {
      errors.push('extreme_negative_apy');
      qualityScore -= 20;
    }
    
    if (apy > 1000) {
      errors.push('extreme_apy_high');
      isOutlier = true;
      outlierReason = `extreme_apy_${apy.toFixed(1)}%`;
      qualityScore -= 40;
    }
  }

  // Timestamp validation
  if (!timestamp) {
    errors.push('missing_timestamp');
    qualityScore -= 50;
  }

  return {
    isValid: errors.length === 0 && qualityScore >= 50, // More lenient for historical
    errors,
    qualityScore: Math.max(0, qualityScore),
    isOutlier,
    outlierReason
  };
}

async function insertHistoricalPool(client, poolId, project, chain, symbol, historyData, timestamp) {
  await client.query(`
    INSERT INTO clean.cl_pool_hist (
      pool_id, ts, project, chain, symbol, tvl_usd, apy, apy_base, url, inserted_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    ON CONFLICT (pool_id, ts) DO UPDATE SET
      tvl_usd = EXCLUDED.tvl_usd,
      apy = EXCLUDED.apy,
      apy_base = EXCLUDED.apy_base,
      inserted_at = EXCLUDED.inserted_at
  `, [
    poolId,
    Math.floor(timestamp / 1000), // Unix timestamp
    project,
    chain,
    symbol,
    historyData.tvlUsd,
    historyData.apy,
    historyData.apyBase,
    null, // url - not available in historical data
    new Date().toISOString()
  ]);
}

module.exports = async (req, res) => {
  const jobRunId = generateJobRunId();
  const startTime = Date.now();
  
  // Parse query parameters for batching
  const { offset = 0, limit = 100, startDate, endDate } = req.query || {};
  const offsetNum = parseInt(offset, 10);
  const limitNum = parseInt(limit, 10);
  
  // Date range for backfill - default to gap period
  const gapStartDate = startDate || '2025-09-05'; // Latest data in clean table
  const gapEndDate = endDate || '2025-09-26'; // Current date
  
  // Quality metrics tracking
  let totalRecords = 0;
  let cleanRecords = 0;
  let scrubbedRecords = 0;
  let errorRecords = 0;
  let outlierRecords = 0;
  let poolsProcessed = 0;
  const errorSummary = {};

  try {
    console.log(`🏊 Starting historical pool backfill job: ${jobRunId}`);
    console.log(`📦 Batch: offset=${offsetNum}, limit=${limitNum}`);
    console.log(`📅 Date range: ${gapStartDate} to ${gapEndDate}`);
    
    // Step 1: Get current pool list for metadata
    const poolsUrl = `https://pro-api.llama.fi/${API_KEY}/yields/pools`;
    console.log(`📡 Fetching current pools for metadata...`);
    
    const poolsResponse = await fetch(poolsUrl);
    if (!poolsResponse.ok) {
      throw new Error(`Pools API request failed: ${poolsResponse.status}`);
    }
    
    const poolsData = await poolsResponse.json();
    if (!poolsData.data || !Array.isArray(poolsData.data)) {
      throw new Error('Invalid pools API response format');
    }
    
    // Create pool metadata lookup
    const poolMetadata = {};
    poolsData.data.forEach(pool => {
      poolMetadata[pool.pool] = {
        project: pool.project,
        chain: pool.chain,
        symbol: pool.symbol
      };
    });
    
    // Apply batching to pool list
    const allPoolIds = Object.keys(poolMetadata);
    const batchPoolIds = allPoolIds.slice(offsetNum, offsetNum + limitNum);
    
    console.log(`📊 Processing ${batchPoolIds.length} pools (${offsetNum}-${offsetNum + limitNum - 1} of ${allPoolIds.length} total)`);
    
    const pool = makePoolFromEnv();
    const client = await pool.connect();
    
    try {
      await client.query('BEGIN');
      
      // Step 2: Process each pool's historical data
      for (const poolId of batchPoolIds) {
        try {
          poolsProcessed++;
          const metadata = poolMetadata[poolId];
          
          // Fetch historical chart data for this pool
          const chartUrl = `https://pro-api.llama.fi/${API_KEY}/yields/chart/${poolId}`;
          console.log(`📈 [${poolsProcessed}/${batchPoolIds.length}] Fetching history for: ${metadata.project}/${metadata.symbol} (${poolId.substring(0, 8)}...)`);
          
          const chartResponse = await fetch(chartUrl);
          if (!chartResponse.ok) {
            console.warn(`⚠️ Failed to fetch chart for pool ${poolId}: ${chartResponse.status}`);
            errorSummary['chart_fetch_error'] = (errorSummary['chart_fetch_error'] || 0) + 1;
            continue;
          }
          
          const chartData = await chartResponse.json();
          if (!chartData.data || !Array.isArray(chartData.data)) {
            console.warn(`⚠️ Invalid chart data for pool ${poolId}`);
            errorSummary['invalid_chart_data'] = (errorSummary['invalid_chart_data'] || 0) + 1;
            continue;
          }
          
          // Filter data for gap period
          const gapStart = new Date(gapStartDate);
          const gapEnd = new Date(gapEndDate);
          
          const relevantData = chartData.data.filter(entry => {
            const entryDate = new Date(entry.timestamp);
            return entryDate >= gapStart && entryDate <= gapEnd;
          });
          
          console.log(`  📅 Found ${relevantData.length} records in gap period for ${metadata.project}/${metadata.symbol}`);
          
          if (relevantData.length > 0) {
            console.log(`  🔄 Processing ${relevantData.length} historical data points...`);
          }
          
          // Process each historical data point
          for (const historyData of relevantData) {
            try {
              const timestamp = new Date(historyData.timestamp).getTime();
              
              // Validate historical data
              const validation = validateHistoricalPoolData(historyData, timestamp);
              totalRecords++;
              
              if (validation.isValid) {
                // Insert clean historical data
                await insertHistoricalPool(
                  client,
                  poolId,
                  metadata.project,
                  metadata.chain,
                  metadata.symbol,
                  historyData,
                  timestamp
                );
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
                    pool_id: poolId,
                    ts: Math.floor(timestamp / 1000),
                    project: metadata.project,
                    chain: metadata.chain,
                    symbol: metadata.symbol,
                    tvl_usd: historyData.tvlUsd,
                    apy: historyData.apy,
                    apy_base: historyData.apyBase
                  },
                  validation,
                  jobRunId,
                  historyData
                );
                
                scrubbedRecords++;
                
                // Track error types
                validation.errors.forEach(error => {
                  errorSummary[error] = (errorSummary[error] || 0) + 1;
                  errorRecords++;
                });
              }
            } catch (error) {
              console.error(`Failed to process history entry for pool ${poolId}:`, error.message);
              errorRecords++;
              errorSummary['processing_error'] = (errorSummary['processing_error'] || 0) + 1;
            }
          }
          
          // Progress update every 10 pools
          if (poolsProcessed % 10 === 0) {
            console.log(`🎯 PROGRESS: ${poolsProcessed}/${batchPoolIds.length} pools completed. Total records: ${totalRecords} (${cleanRecords} clean, ${scrubbedRecords} scrubbed)`);
          }
          
          // Small delay to avoid overwhelming the API
          await new Promise(resolve => setTimeout(resolve, 100));
          
        } catch (error) {
          console.error(`Failed to process pool ${poolId}:`, error.message);
          errorSummary['pool_fetch_error'] = (errorSummary['pool_fetch_error'] || 0) + 1;
        }
      }
      
      // Update quality summary
      const processingTime = Date.now() - startTime;
      const overallQualityScore = totalRecords > 0 ? (cleanRecords / totalRecords) * 100 : 0;
      
      await updateQualitySummary(client, 'pool_backfill', jobRunId, {
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
      
      console.log(`✅ Historical pool backfill completed:`, {
        poolsProcessed,
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
          poolsProcessed,
          totalRecords,
          cleanRecords,
          scrubbedRecords,
          errorRecords,
          outlierRecords,
          qualityScore: overallQualityScore,
          processingTimeMs: processingTime
        },
        dateRange: { start: gapStartDate, end: gapEndDate },
        message: `Backfilled ${poolsProcessed} pools, ${cleanRecords} clean records, ${scrubbedRecords} scrubbed`
      });
      
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
      await pool.end();
    }
    
  } catch (error) {
    console.error('💥 Historical pool backfill failed:', error.message);
    console.error('Stack:', error.stack);
    
    res.status(500).json({
      success: false,
      jobRunId,
      error: error.message,
      metrics: {
        poolsProcessed,
        totalRecords,
        cleanRecords,
        scrubbedRecords,
        errorRecords
      }
    });
  }
};
