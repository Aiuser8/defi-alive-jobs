// test_backfill_token_prices.js
// Quick test of the historical token price backfill (just 1 day, 5 tokens)

const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

const API_KEY = process.env.DEFILLAMA_API_KEY;

const pool = new Pool({
  connectionString: process.env.SUPABASE_DB_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

async function testBackfill() {
  console.log('🧪 Testing Historical Token Price Backfill...');
  
  try {
    // Test with just 5 tokens for 1 day ago
    const testTokens = [
      'ethereum:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', // USDC
      'ethereum:0xdac17f958d2ee523a2206206994597c13d831ec7', // USDT  
      'ethereum:0x2260fac5e5542a773aa44fbcfedf7c193bc2c599', // WBTC
      'ethereum:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', // WETH
      'ethereum:0x1f9840a85d5af5bf1d1762f925bdaddc4201f984'  // UNI
    ];
    
    // Test timestamp from 1 day ago at noon UTC
    const testDate = new Date();
    testDate.setDate(testDate.getDate() - 1);
    testDate.setUTCHours(12, 0, 0, 0);
    const timestamp = Math.floor(testDate.getTime() / 1000);
    
    console.log(`📅 Test date: ${testDate.toISOString()}`);
    console.log(`⏰ Timestamp: ${timestamp}`);
    
    // Fetch test data
    const coinsParam = encodeURIComponent(testTokens.join(','));
    const url = `https://pro-api.llama.fi/${API_KEY}/coins/prices/historical/${timestamp}/${coinsParam}`;
    
    console.log(`📡 Fetching: ${url}`);
    
    const res = await fetch(url);
    if (!res.ok) {
      throw new Error(`API request failed: ${res.status} ${res.statusText}`);
    }
    
    const data = await res.json();
    console.log(`✅ API Response:`, JSON.stringify(data, null, 2));
    
    // Test database insertion
    const client = await pool.connect();
    let insertCount = 0;
    
    try {
      for (const [coinId, priceInfo] of Object.entries(data.coins || {})) {
        if (typeof priceInfo?.price === 'number' && priceInfo.price > 0) {
          await client.query(`
            INSERT INTO update.token_price_daily (
              coin_id, symbol, confidence, decimals, price_timestamp, price_usd
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (coin_id, price_timestamp) DO UPDATE SET
              symbol = EXCLUDED.symbol,
              confidence = EXCLUDED.confidence,
              decimals = EXCLUDED.decimals,
              price_usd = EXCLUDED.price_usd
          `, [
            coinId,
            priceInfo.symbol || null,
            priceInfo.confidence || null,
            priceInfo.decimals || null,
            new Date(timestamp * 1000),
            priceInfo.price
          ]);
          
          insertCount++;
          console.log(`✅ Inserted: ${coinId} = $${priceInfo.price} (${priceInfo.symbol})`);
        }
      }
    } finally {
      client.release();
    }
    
    console.log(`\n🎉 Test completed successfully!`);
    console.log(`📊 Records inserted: ${insertCount}`);
    console.log(`✅ Ready for full backfill`);
    
  } catch (error) {
    console.error(`❌ Test failed:`, error.message);
    process.exit(1);
  }
}

testBackfill().catch(console.error);
