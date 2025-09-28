// api/job_lending_direct.js
// Direct lending job using DeFiLlama Pro API - no validation, direct data landing
// Fast data collection for later cleaning/normalization

module.exports.config = { runtime: 'nodejs18.x' };

const { Pool } = require('pg');

function makePoolFromEnv() {
  return new Pool({
    connectionString: process.env.SUPABASE_DB_URL,
    ssl: { rejectUnauthorized: false },
  });
}

/**
 * Fetches lending market data from DeFiLlama Pro API
 */
async function fetchLendingData() {
  const { DEFILLAMA_API_KEY } = process.env;
  
  console.log(`🔑 API Key loaded: ${DEFILLAMA_API_KEY ? 'YES' : 'NO'} (length: ${DEFILLAMA_API_KEY?.length || 0})`);
  
  if (!DEFILLAMA_API_KEY) {
    throw new Error('DEFILLAMA_API_KEY environment variable is required');
  }

  const url = `https://pro-api.llama.fi/${DEFILLAMA_API_KEY}/yields/poolsBorrow`;
  
  console.log(`📡 Fetching lending data from DeFiLlama Pro API`);
  
  const response = await fetch(url);
  if (!response.ok) {
    const errorText = await response.text().catch(() => '');
    throw new Error(`API request failed: ${response.status} ${response.statusText} - ${errorText}`);
  }
  
  const data = await response.json();
  if (!data.data || !Array.isArray(data.data)) {
    throw new Error('Invalid API response format');
  }
  
  console.log(`✅ Fetched ${data.data.length} lending markets`);
  return data.data;
}

/**
 * Insert lending data directly into update table
 */
async function insertLendingData(client, lendingData) {
  let insertedCount = 0;
  let errorCount = 0;
  
  console.log(`📝 Inserting ${lendingData.length} lending records...`);
  
  for (const pool of lendingData) {
    try {
      const insertQuery = `
        INSERT INTO update.lending_market_history (
          market_id, ts, project, chain, symbol,
          total_supply_usd, total_borrow_usd, debt_ceiling_usd,
          apy_base_supply, apy_reward_supply, apy_base_borrow, apy_reward_borrow,
          pool_id, tvl_usd, apy, apy_pct_1d, apy_pct_7d, apy_pct_30d,
          stablecoin, il_risk, exposure, ltv, borrowable,
          mu, sigma, count, outlier, apy_mean_30d,
          predictions, reward_tokens, underlying_tokens, pool_meta,
          data_timestamp
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
          $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28,
          $29, $30, $31
        )
      `;
      
      const values = [
        pool.pool || null,                              // market_id
        new Date(),                                     // ts
        pool.project || null,                           // project
        pool.chain || null,                             // chain
        pool.symbol || null,                            // symbol
        pool.tvlUsd || null,                           // total_supply_usd
        pool.totalBorrowUsd || null,                   // total_borrow_usd
        pool.debtCeilingUsd || null,                   // debt_ceiling_usd
        pool.apyBase || null,                          // apy_base_supply
        pool.apyReward || null,                        // apy_reward_supply
        pool.apyBaseBorrow || null,                    // apy_base_borrow
        pool.apyRewardBorrow || null,                  // apy_reward_borrow
        pool.pool || null,                             // pool_id
        pool.tvlUsd || null,                           // tvl_usd
        pool.apy || null,                              // apy
        pool.apyPct1D || null,                         // apy_pct_1d
        pool.apyPct7D || null,                         // apy_pct_7d
        pool.apyPct30D || null,                        // apy_pct_30d
        pool.stablecoin || false,                      // stablecoin
        pool.ilRisk || null,                           // il_risk
        pool.exposure || null,                         // exposure
        pool.ltv || null,                              // ltv
        pool.borrowable || false,                      // borrowable
        pool.mu || null,                               // mu
        pool.sigma || null,                            // sigma
        pool.count || null,                            // count
        pool.outlier || false,                         // outlier
        pool.apyMean30d || null,                       // apy_mean_30d
        pool.predictions ? JSON.stringify(pool.predictions) : null,  // predictions
        pool.rewardTokens ? JSON.stringify(pool.rewardTokens) : null, // reward_tokens
        pool.underlyingTokens ? JSON.stringify(pool.underlyingTokens) : null, // underlying_tokens
        pool.poolMeta || null,                         // pool_meta
        new Date()                                     // data_timestamp
      ];
      
      await client.query(insertQuery, values);
      insertedCount++;
      
    } catch (error) {
      console.error(`❌ Error inserting lending record ${pool.pool}:`, error.message);
      errorCount++;
    }
  }
  
  return { insertedCount, errorCount };
}

/**
 * Main function
 */
module.exports = async function(req, res) {
  const startTime = Date.now();
  let client;
  
  try {
    console.log('🚀 Starting Direct Lending Data Collection...');
    
    // Create database connection
    const pool = makePoolFromEnv();
    client = await pool.connect();
    
    // Fetch lending data
    const lendingData = await fetchLendingData();
    
    // Insert data directly
    const { insertedCount, errorCount } = await insertLendingData(client, lendingData);
    
    const duration = Date.now() - startTime;
    
    console.log('🎉 Direct Lending Collection Complete!');
    console.log(`✅ Inserted: ${insertedCount} records`);
    console.log(`❌ Errors: ${errorCount} records`);
    console.log(`⏱️  Processing time: ${duration}ms`);
    
    res.status(200).json({
      success: true,
      message: 'Direct lending data collection completed',
      insertedRecords: insertedCount,
      errorRecords: errorCount,
      totalProcessed: lendingData.length,
      processingTimeMs: duration
    });
    
  } catch (error) {
    console.error('❌ Direct Lending Collection failed:', error);
    
    res.status(500).json({
      success: false,
      error: error.message,
      processingTimeMs: Date.now() - startTime
    });
    
  } finally {
    if (client) {
      client.release();
    }
  }
};
