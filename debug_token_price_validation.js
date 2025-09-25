// debug_token_price_validation.js - Debug token price validation issues
const { Client } = require('pg');
require('dotenv').config();

function makePoolFromEnv() {
  const {
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE
  } = process.env;

  const sslRequired = (PGSSLMODE || '').toLowerCase() === 'require';
  return new Client({
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

async function debugTokenPriceValidation() {
  const client = makePoolFromEnv();
  
  try {
    await client.connect();
    console.log('🔗 Connected to Supabase database\n');
    
    console.log('🔍 Debugging Token Price Validation Issues\n');
    
    // 1. Check the most recent scrubbed data with original data
    console.log('📊 Recent Scrubbed Data with Original Data:');
    const scrubbedData = await client.query(`
      SELECT 
        coin_id,
        price_usd,
        price_timestamp,
        quality_score,
        validation_errors,
        original_data,
        processed_at
      FROM scrub.token_price_scrub
      WHERE processed_at >= NOW() - INTERVAL '1 hour'
      ORDER BY processed_at DESC
      LIMIT 5;
    `);
    
    if (scrubbedData.rows.length > 0) {
      for (const row of scrubbedData.rows) {
        console.log('\n--- Record ---');
        console.log(`Coin ID: ${row.coin_id}`);
        console.log(`Price USD: ${row.price_usd}`);
        console.log(`Price Timestamp: ${row.price_timestamp}`);
        console.log(`Quality Score: ${row.quality_score}`);
        console.log(`Validation Errors: ${row.validation_errors?.join(', ')}`);
        console.log(`Processed At: ${row.processed_at}`);
        
        if (row.original_data) {
          console.log('Original Data:');
          console.log(`  - coinId: ${row.original_data.coinId}`);
          console.log(`  - tsSec: ${row.original_data.tsSec}`);
          console.log(`  - price: ${row.original_data.price}`);
          console.log(`  - symbol: ${row.original_data.symbol}`);
          
          // Calculate age if we have tsSec
          if (row.original_data.tsSec) {
            const ageMinutes = (Date.now() - row.original_data.tsSec * 1000) / (1000 * 60);
            console.log(`  - Age: ${ageMinutes.toFixed(1)} minutes`);
          }
        }
      }
    } else {
      console.log('No recent scrubbed data found');
    }
    
    // 2. Check if there are any clean records being inserted
    console.log('\n✅ Checking for Recent Clean Records:');
    const cleanRecords = await client.query(`
      SELECT 
        coin_id,
        symbol,
        price_usd,
        price_timestamp,
        EXTRACT(EPOCH FROM (NOW() - price_timestamp))/60 as age_minutes
      FROM update.token_price_daily
      WHERE price_timestamp >= NOW() - INTERVAL '24 hours'
      ORDER BY price_timestamp DESC
      LIMIT 5;
    `);
    
    if (cleanRecords.rows.length > 0) {
      console.table(cleanRecords.rows.map(row => ({
        coin_id: row.coin_id?.substring(0, 20) + '...',
        symbol: row.symbol,
        price: row.price_usd,
        timestamp: row.price_timestamp,
        age_minutes: Math.round(row.age_minutes)
      })));
    } else {
      console.log('No recent clean records found in update.token_price_daily');
    }
    
    // 3. Check the validation threshold
    console.log('\n⚙️ Validation Threshold Analysis:');
    console.log('Current validation flags data as "stale_data" if older than 60 minutes');
    console.log('This might be too strict for API data that could be delayed');
    
    // 4. Check what the API is actually returning
    console.log('\n🌐 API Data Analysis:');
    const apiDataSample = await client.query(`
      SELECT 
        original_data->>'tsSec' as ts_sec,
        original_data->>'coinId' as coin_id,
        original_data->>'price' as price,
        original_data->>'symbol' as symbol,
        processed_at,
        EXTRACT(EPOCH FROM (NOW() - processed_at))/60 as processed_minutes_ago
      FROM scrub.token_price_scrub
      WHERE processed_at >= NOW() - INTERVAL '1 hour'
        AND original_data->>'tsSec' IS NOT NULL
      ORDER BY processed_at DESC
      LIMIT 3;
    `);
    
    if (apiDataSample.rows.length > 0) {
      for (const row of apiDataSample.rows) {
        if (row.ts_sec) {
          const apiTimestamp = new Date(parseInt(row.ts_sec) * 1000);
          const ageMinutes = (Date.now() - parseInt(row.ts_sec) * 1000) / (1000 * 60);
          console.log(`\nAPI Data Sample:`);
          console.log(`  Coin ID: ${row.coin_id?.substring(0, 30)}...`);
          console.log(`  API Timestamp: ${apiTimestamp}`);
          console.log(`  Age: ${ageMinutes.toFixed(1)} minutes`);
          console.log(`  Price: ${row.price}`);
          console.log(`  Processed: ${row.processed_minutes_ago.toFixed(1)} minutes ago`);
        }
      }
    }
    
    console.log('\n💡 Recommendations:');
    console.log('1. The 60-minute stale data threshold might be too strict');
    console.log('2. Consider increasing to 120-180 minutes for API data');
    console.log('3. Or make it configurable based on data source');
    console.log('4. Check if the API is actually returning fresh data');
    
  } catch (error) {
    console.error('❌ Debug failed:', error.message);
  } finally {
    await client.end();
  }
}

debugTokenPriceValidation();
