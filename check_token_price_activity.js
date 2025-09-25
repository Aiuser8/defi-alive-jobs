// check_token_price_activity.js - Check if token price cron jobs are working
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

async function checkTokenPriceActivity() {
  const client = makePoolFromEnv();
  
  try {
    await client.connect();
    console.log('🔗 Connected to Supabase database\n');
    
    console.log('🪙 Checking Token Price Collection Activity\n');
    
    // 1. Check recent token price data
    console.log('📊 Recent Token Price Data:');
    const recentPrices = await client.query(`
      SELECT 
        coin_id,
        symbol,
        price_usd,
        price_timestamp,
        EXTRACT(EPOCH FROM (NOW() - price_timestamp))/60 as minutes_ago
      FROM update.token_price_daily
      ORDER BY price_timestamp DESC
      LIMIT 10;
    `);
    
    if (recentPrices.rows.length > 0) {
      console.table(recentPrices.rows.map(row => ({
        coin_id: row.coin_id.substring(0, 20) + '...',
        symbol: row.symbol,
        price: row.price_usd,
        timestamp: row.price_timestamp,
        minutes_ago: Math.round(row.minutes_ago)
      })));
    } else {
      console.log('No recent token price data found');
    }
    
    // 2. Check data collection frequency
    console.log('\n📈 Data Collection Frequency Analysis:');
    const frequencyAnalysis = await client.query(`
      SELECT 
        DATE(price_timestamp) as date,
        COUNT(*) as records_per_day,
        MIN(price_timestamp) as first_record,
        MAX(price_timestamp) as last_record
      FROM update.token_price_daily
      WHERE price_timestamp >= NOW() - INTERVAL '7 days'
      GROUP BY DATE(price_timestamp)
      ORDER BY date DESC;
    `);
    
    if (frequencyAnalysis.rows.length > 0) {
      console.table(frequencyAnalysis.rows.map(row => ({
        date: row.date,
        records: row.records_per_day,
        first: row.first_record?.toISOString().substring(11, 19) || 'N/A',
        last: row.last_record?.toISOString().substring(11, 19) || 'N/A'
      })));
    } else {
      console.log('No data in the last 7 days');
    }
    
    // 3. Check quality summary for token price jobs
    console.log('\n🔍 Token Price Job Quality Summary:');
    const qualitySummary = await client.query(`
      SELECT 
        job_run_id,
        run_timestamp,
        total_records,
        clean_records,
        scrubbed_records,
        error_records,
        outlier_records,
        overall_quality_score,
        processing_time_ms,
        error_summary
      FROM scrub.data_quality_summary
      WHERE job_name = 'backfill_token_prices'
        AND run_timestamp >= NOW() - INTERVAL '24 hours'
      ORDER BY run_timestamp DESC
      LIMIT 10;
    `);
    
    if (qualitySummary.rows.length > 0) {
      console.table(qualitySummary.rows.map(row => ({
        job_run_id: row.job_run_id.substring(0, 15) + '...',
        timestamp: row.run_timestamp.toISOString().substring(11, 19),
        total: row.total_records,
        clean: row.clean_records,
        scrubbed: row.scrubbed_records,
        errors: row.error_records,
        outliers: row.outlier_records,
        quality_score: row.overall_quality_score,
        time_ms: row.processing_time_ms
      })));
    } else {
      console.log('No token price job runs found in the last 24 hours');
    }
    
    // 4. Check scrubbed token price data
    console.log('\n🧹 Recent Scrubbed Token Price Data:');
    const scrubbedData = await client.query(`
      SELECT 
        coin_id,
        price_usd,
        quality_score,
        validation_errors,
        outlier_reason,
        processed_at,
        EXTRACT(EPOCH FROM (NOW() - processed_at))/60 as minutes_ago
      FROM scrub.token_price_scrub
      WHERE processed_at >= NOW() - INTERVAL '24 hours'
      ORDER BY processed_at DESC
      LIMIT 10;
    `);
    
    if (scrubbedData.rows.length > 0) {
      console.table(scrubbedData.rows.map(row => ({
        coin_id: row.coin_id?.substring(0, 20) + '...' || 'N/A',
        price: row.price_usd,
        quality_score: row.quality_score,
        errors: row.validation_errors?.join(', ') || 'none',
        outlier_reason: row.outlier_reason || 'none',
        minutes_ago: Math.round(row.minutes_ago)
      })));
    } else {
      console.log('No scrubbed token price data found in the last 24 hours');
    }
    
    // 5. Check for any recent errors
    console.log('\n⚠️ Recent Error Analysis:');
    const errorAnalysis = await client.query(`
      SELECT 
        key as error_type,
        SUM(value::integer) as error_count
      FROM scrub.data_quality_summary,
           jsonb_each_text(error_summary) as kv(key, value)
      WHERE job_name = 'backfill_token_prices'
        AND run_timestamp >= NOW() - INTERVAL '24 hours'
      GROUP BY key
      ORDER BY error_count DESC;
    `);
    
    if (errorAnalysis.rows.length > 0) {
      console.table(errorAnalysis.rows);
    } else {
      console.log('No errors found in recent token price jobs');
    }
    
    // 6. Summary
    console.log('\n📋 Token Price Collection Status:');
    const latestRecord = await client.query(`
      SELECT MAX(price_timestamp) as latest_timestamp,
             EXTRACT(EPOCH FROM (NOW() - MAX(price_timestamp)))/60 as minutes_since_last
      FROM update.token_price_daily;
    `);
    
    if (latestRecord.rows[0].latest_timestamp) {
      const minutesSince = Math.round(latestRecord.rows[0].minutes_since_last);
      console.log(`Latest record: ${latestRecord.rows[0].latest_timestamp}`);
      console.log(`Minutes since last record: ${minutesSince}`);
      
      if (minutesSince <= 10) {
        console.log('✅ Token price collection appears to be working (very recent data)');
      } else if (minutesSince <= 60) {
        console.log('⚠️ Token price collection may have issues (data is ${minutesSince} minutes old)');
      } else {
        console.log('❌ Token price collection appears to be down (data is ${minutesSince} minutes old)');
      }
    } else {
      console.log('❌ No token price data found');
    }
    
  } catch (error) {
    console.error('❌ Check failed:', error.message);
  } finally {
    await client.end();
  }
}

checkTokenPriceActivity();
