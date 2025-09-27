// api/job_liquidity_pools_direct.js
// Direct liquidity pool data collection - no validation, direct data landing
// Fast data collection for later cleaning/normalization

module.exports.config = { runtime: 'nodejs18.x' };

require('dotenv').config();
const { Pool } = require('pg');

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
 * Fetch liquidity pool data from DeFiLlama Pro API
 */
async function fetchPoolData(offset, limit) {
  const url = `https://pro-api.llama.fi/${API_KEY}/yields?offset=${offset}&limit=${limit}`;
  
  console.log(`📡 Fetching pools ${offset}-${offset + limit - 1} from: ${url}`);
  
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`API request failed: ${response.status} ${response.statusText}`);
  }
  
  const data = await response.json();
  if (!data.data || !Array.isArray(data.data)) {
    throw new Error('Invalid API response format');
  }
  
  console.log(`✅ Fetched ${data.data.length} pools`);
  return data.data;
}

/**
 * Insert pool data directly into update table
 */
async function insertPoolData(client, poolData) {
  let insertedCount = 0;
  let errorCount = 0;
  
  console.log(`📝 Inserting ${poolData.length} pool records...`);
  
  for (const pool of poolData) {
    try {
      const insertQuery = `
        INSERT INTO update.cl_pool_hist (
          pool_id, ts, project, chain, symbol, tvl_usd, apy, apy_base, url
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      `;
      
      const values = [
        pool.pool || null,                    // pool_id
        Math.floor(Date.now() / 1000),       // ts (unix timestamp)
        pool.project || null,                // project
        pool.chain || null,                  // chain
        pool.symbol || null,                 // symbol
        pool.tvlUsd || null,                 // tvl_usd
        pool.apy || null,                    // apy
        pool.apyBase || null,                // apy_base
        pool.url || null                     // url
      ];
      
      await client.query(insertQuery, values);
      insertedCount++;
      
    } catch (error) {
      console.error(`❌ Error inserting pool record ${pool.pool}:`, error.message);
      errorCount++;
    }
  }
  
  return { insertedCount, errorCount };
}

/**
 * Main function - processes a batch of pools
 */
module.exports = async function(req, res) {
  const startTime = Date.now();
  let client;
  
  try {
    // Get batch parameters from query string
    const offset = parseInt(req.query.offset) || 0;
    const limit = parseInt(req.query.limit) || 1600;
    
    console.log(`🚀 Starting Direct Pool Collection: offset=${offset}, limit=${limit}`);
    
    // Create database connection
    const pool = makePoolFromEnv();
    client = await pool.connect();
    
    // Fetch pool data
    const poolData = await fetchPoolData(offset, limit);
    
    // Insert data directly
    const { insertedCount, errorCount } = await insertPoolData(client, poolData);
    
    const duration = Date.now() - startTime;
    
    console.log('🎉 Direct Pool Collection Complete!');
    console.log(`✅ Inserted: ${insertedCount} records`);
    console.log(`❌ Errors: ${errorCount} records`);
    console.log(`⏱️  Processing time: ${duration}ms`);
    
    res.status(200).json({
      success: true,
      message: 'Direct pool data collection completed',
      insertedRecords: insertedCount,
      errorRecords: errorCount,
      totalProcessed: poolData.length,
      batchInfo: { offset, limit },
      processingTimeMs: duration
    });
    
  } catch (error) {
    console.error('❌ Direct Pool Collection failed:', error);
    
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
