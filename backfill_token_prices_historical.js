// backfill_token_prices_historical.js
// Historical token price backfill for update.token_price_daily
// Fills gap from 18+ days ago to current using DeFiLlama historical API

const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

const API_KEY = process.env.DEFILLAMA_API_KEY;
const BATCH_SIZE = 20; // Tokens per API call (reduced to avoid URL length limits)
const DELAY_BETWEEN_BATCHES_MS = 2000; // 2 seconds delay
const MAX_CONCURRENCY = 3; // Parallel API calls

// Date range for backfill (18+ days of missing data)
const END_DATE = new Date(); // Now
const START_DATE = new Date();
START_DATE.setDate(END_DATE.getDate() - 20); // Go back 20 days to be safe

const pool = new Pool({
  connectionString: process.env.SUPABASE_DB_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

function generateJobRunId() {
  return `backfill_historical_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

// Get timestamps for each day in the backfill range
function getBackfillTimestamps() {
  const timestamps = [];
  const current = new Date(START_DATE);
  
  while (current <= END_DATE) {
    // Use noon UTC for each day to get daily prices
    const noonUTC = new Date(current);
    noonUTC.setUTCHours(12, 0, 0, 0);
    timestamps.push(Math.floor(noonUTC.getTime() / 1000));
    current.setDate(current.getDate() + 1);
  }
  
  return timestamps;
}

async function fetchHistoricalPrices(coinIds, timestamp, apiKey) {
  const coinsParam = encodeURIComponent(coinIds.join(','));
  // Use the historical endpoint format you suggested
  const url = `https://pro-api.llama.fi/${apiKey}/coins/prices/historical/${timestamp}/${coinsParam}`;
  
  console.log(`📡 Fetching ${coinIds.length} tokens for ${new Date(timestamp * 1000).toISOString()}`);
  
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText}${text ? ` | ${text}` : ''}`);
  }
  
  return res.json();
}

async function insertHistoricalPrices(client, priceData, timestamp) {
  const insertPromises = [];
  
  for (const [coinId, priceInfo] of Object.entries(priceData.coins || {})) {
    if (typeof priceInfo?.price === 'number' && priceInfo.price > 0) {
      const insertPromise = client.query(`
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
      
      insertPromises.push(insertPromise);
    }
  }
  
  await Promise.all(insertPromises);
  return insertPromises.length;
}

async function runBackfill() {
  const jobRunId = generateJobRunId();
  const startTime = Date.now();
  
  console.log(`🚀 Starting Historical Token Price Backfill: ${jobRunId}`);
  console.log(`📅 Date range: ${START_DATE.toISOString()} → ${END_DATE.toISOString()}`);
  
  try {
    // Load token list
    const tokenListPath = path.join(process.cwd(), 'token_list_active.json'); // Use our optimized list
    const tokens = JSON.parse(fs.readFileSync(tokenListPath, 'utf-8'));
    console.log(`📋 Loaded ${tokens.length} active tokens`);
    
    // Convert to DeFiLlama coin ID format
    const coinIds = tokens.map(token => `${token.chain}:${token.address}`);
    
    // Get all timestamps to backfill
    const timestamps = getBackfillTimestamps();
    console.log(`⏰ Processing ${timestamps.length} days of historical data`);
    
    const client = await pool.connect();
    let totalRecords = 0;
    let totalErrors = 0;
    
    try {
      // Process each timestamp
      for (let i = 0; i < timestamps.length; i++) {
        const timestamp = timestamps[i];
        const date = new Date(timestamp * 1000).toISOString().split('T')[0];
        
        console.log(`\n📅 Day ${i + 1}/${timestamps.length}: ${date}`);
        
        // Process tokens in batches for this timestamp
        const batches = [];
        for (let j = 0; j < coinIds.length; j += BATCH_SIZE) {
          batches.push(coinIds.slice(j, j + BATCH_SIZE));
        }
        
        const batchPromises = batches.map(async (batch, batchIndex) => {
          try {
            await new Promise(resolve => setTimeout(resolve, batchIndex * 200)); // Stagger requests
            const priceData = await fetchHistoricalPrices(batch, timestamp, API_KEY);
            const recordsInserted = await insertHistoricalPrices(client, priceData, timestamp);
            console.log(`  ✅ Batch ${batchIndex + 1}: ${recordsInserted} records`);
            return recordsInserted;
          } catch (error) {
            console.error(`  ❌ Batch ${batchIndex + 1}: ${error.message}`);
            return 0;
          }
        });
        
        // Wait for all batches for this day with concurrency limit
        const results = [];
        for (let k = 0; k < batchPromises.length; k += MAX_CONCURRENCY) {
          const chunk = batchPromises.slice(k, k + MAX_CONCURRENCY);
          const chunkResults = await Promise.all(chunk);
          results.push(...chunkResults);
        }
        
        const dayRecords = results.reduce((sum, count) => sum + count, 0);
        totalRecords += dayRecords;
        
        console.log(`📊 Day ${i + 1} total: ${dayRecords} records`);
        
        // Delay between days
        if (i < timestamps.length - 1) {
          await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_BATCHES_MS));
        }
      }
      
    } finally {
      client.release();
    }
    
    const processingTime = Date.now() - startTime;
    
    console.log(`\n🎉 HISTORICAL BACKFILL COMPLETE!`);
    console.log(`📊 Total records inserted: ${totalRecords}`);
    console.log(`📅 Days processed: ${timestamps.length}`);
    console.log(`⏱️  Total time: ${(processingTime / 1000 / 60).toFixed(1)} minutes`);
    console.log(`🎯 Average: ${(totalRecords / timestamps.length).toFixed(0)} records per day`);
    
  } catch (error) {
    console.error(`❌ Backfill failed:`, error.message);
    process.exit(1);
  }
}

// Run if called directly
if (require.main === module) {
  runBackfill().catch(console.error);
}

module.exports = { runBackfill };
