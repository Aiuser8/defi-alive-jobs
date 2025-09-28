// api/backfill_token_prices_direct.js
// Direct token price collection - no validation, direct data landing
// Fast data collection for later cleaning/normalization

module.exports.config = { runtime: 'nodejs18.x' };

require('dotenv').config();
const { Pool } = require('pg');

// Create database connection pool
function makePoolFromEnv() {
  return new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
  });
}

/**
 * Fetch current token prices from DeFiLlama Pro API
 */
async function fetchTokenPrices(coinIds) {
  const { DEFILLAMA_API_KEY } = process.env;
  
  if (!DEFILLAMA_API_KEY) {
    throw new Error('DEFILLAMA_API_KEY environment variable is required');
  }

  const coinsParam = encodeURIComponent(coinIds.join(','));
  const url = `https://pro-api.llama.fi/${DEFILLAMA_API_KEY}/coins/chart/${coinsParam}`;
  
  console.log(`📡 Fetching prices for ${coinIds.length} tokens...`);
  
  const response = await fetch(url);
  if (!response.ok) {
    const errorText = await response.text().catch(() => '');
    throw new Error(`API request failed: ${response.status} ${response.statusText} - ${errorText}`);
  }
  
  const data = await response.json();
  if (!data || typeof data !== 'object') {
    throw new Error('Invalid API response format');
  }
  
  console.log(`✅ Received price data for ${Object.keys(data).length} tokens`);
  return data;
}

/**
 * Insert token prices directly into update table
 */
async function insertTokenPrices(client, chartData, coinIds) {
  let insertedCount = 0;
  let skippedCount = 0;
  
  console.log(`📝 Processing ${coinIds.length} token prices...`);
  
  const nodes = chartData?.coins || chartData || {};
  
  for (let i = 0; i < coinIds.length; i++) {
    const coinId = coinIds[i];
    const node = nodes[coinId];
    
    // Chart endpoint returns prices array, get the latest price
    const latestPrice = node?.prices?.[node.prices.length - 1];
    
    // Skip if no valid price data
    if (!node || !latestPrice || typeof latestPrice.price !== 'number' || latestPrice.price <= 0) {
      skippedCount++;
      continue;
    }
    
    try {
      // Use current time with microsecond offset to guarantee unique timestamps
      const nowMs = Date.now();
      const tsSec = (nowMs / 1000) + (i * 0.001); // Current time + millisecond offset per token
      const tsIso = new Date(tsSec * 1000).toISOString();
      
      const insertQuery = `
        INSERT INTO "update".token_price_daily
          (coin_id, symbol, confidence, decimals, price_timestamp, price_usd)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (coin_id, price_timestamp)
        DO UPDATE SET
          price_usd = EXCLUDED.price_usd,
          confidence = EXCLUDED.confidence,
          decimals = EXCLUDED.decimals,
          symbol = EXCLUDED.symbol
      `;
      
      const values = [
        coinId,                           // coin_id
        node.symbol || null,              // symbol
        node.confidence || 0.99,          // confidence
        node.decimals || 18,              // decimals
        tsIso,                           // price_timestamp
        latestPrice.price                 // price_usd
      ];
      
      await client.query(insertQuery, values);
      insertedCount++;
      
    } catch (error) {
      console.error(`❌ Error inserting price for ${coinId}:`, error.message);
      skippedCount++;
    }
  }
  
  return { insertedCount, skippedCount };
}

/**
 * Main function - processes a batch of token prices
 */
module.exports = async function(req, res) {
  const startTime = Date.now();
  let client;
  
  try {
    // Get batch parameters from query string
    const offset = parseInt(req.query.offset) || 0;
    const limit = parseInt(req.query.limit) || 818;
    
    console.log(`🚀 Starting Direct Token Price Collection: offset=${offset}, limit=${limit}`);
    
    // Load token list
    const fs = require('fs');
    const path = require('path');
    const tokenListPath = path.join(process.cwd(), 'token_list_active.json');
    const tokenList = JSON.parse(fs.readFileSync(tokenListPath, 'utf8'));
    
    // Get batch of coin IDs
    const coinIds = tokenList.slice(offset, offset + limit);
    
    if (coinIds.length === 0) {
      return res.status(200).json({
        success: true,
        message: 'No tokens to process in this batch',
        insertedRecords: 0,
        skippedRecords: 0,
        batchInfo: { offset, limit },
        processingTimeMs: Date.now() - startTime
      });
    }
    
    // Create database connection
    const pool = makePoolFromEnv();
    client = await pool.connect();
    
    // Fetch token prices
    const chartData = await fetchTokenPrices(coinIds);
    
    // Insert prices directly
    const { insertedCount, skippedCount } = await insertTokenPrices(client, chartData, coinIds);
    
    const duration = Date.now() - startTime;
    
    console.log('🎉 Direct Token Price Collection Complete!');
    console.log(`✅ Inserted: ${insertedCount} records`);
    console.log(`⚠️ Skipped: ${skippedCount} records`);
    console.log(`⏱️  Processing time: ${duration}ms`);
    
    res.status(200).json({
      success: true,
      message: 'Direct token price collection completed',
      insertedRecords: insertedCount,
      skippedRecords: skippedCount,
      totalProcessed: coinIds.length,
      batchInfo: { offset, limit },
      processingTimeMs: duration
    });
    
  } catch (error) {
    console.error('❌ Direct Token Price Collection failed:', error);
    
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
