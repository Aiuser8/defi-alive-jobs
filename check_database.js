// check_database.js - Check the database for quality metrics and scrubbed data
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

async function checkDatabase() {
  const client = makePoolFromEnv();
  
  try {
    await client.connect();
    console.log('🔗 Connected to Supabase database\n');
    
    // Check data quality summary
    console.log('📊 Data Quality Summary:');
    const summaryResult = await client.query(`
      SELECT 
        job_name,
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
      ORDER BY run_timestamp DESC
      LIMIT 5;
    `);
    
    if (summaryResult.rows.length > 0) {
      console.table(summaryResult.rows.map(row => ({
        job: row.job_name,
        timestamp: row.run_timestamp,
        total: row.total_records,
        clean: row.clean_records,
        scrubbed: row.scrubbed_records,
        errors: row.error_records,
        outliers: row.outlier_records,
        quality_score: row.overall_quality_score,
        time_ms: row.processing_time_ms
      })));
    } else {
      console.log('No quality summary data found yet.');
    }
    
    // Check scrubbed token prices
    console.log('\n🧹 Scrubbed Token Prices:');
    const scrubResult = await client.query(`
      SELECT 
        coin_id,
        price_usd,
        quality_score,
        validation_errors,
        outlier_reason,
        processed_at
      FROM scrub.token_price_scrub
      ORDER BY processed_at DESC
      LIMIT 10;
    `);
    
    if (scrubResult.rows.length > 0) {
      console.table(scrubResult.rows.map(row => ({
        coin_id: row.coin_id,
        price: row.price_usd,
        quality_score: row.quality_score,
        errors: row.validation_errors?.join(', ') || 'none',
        outlier_reason: row.outlier_reason || 'none',
        processed_at: row.processed_at
      })));
    } else {
      console.log('No scrubbed token price data found yet.');
    }
    
    // Check clean token prices
    console.log('\n✅ Clean Token Prices (recent):');
    const cleanResult = await client.query(`
      SELECT 
        coin_id,
        symbol,
        price_usd,
        price_timestamp
      FROM update.token_price_daily
      ORDER BY price_timestamp DESC
      LIMIT 10;
    `);
    
    if (cleanResult.rows.length > 0) {
      console.table(cleanResult.rows.map(row => ({
        coin_id: row.coin_id,
        symbol: row.symbol,
        price: row.price_usd,
        timestamp: row.price_timestamp
      })));
    } else {
      console.log('No clean token price data found yet.');
    }
    
    // Check if scrub tables exist
    console.log('\n🗄️ Database Schema Check:');
    const schemaResult = await client.query(`
      SELECT 
        schemaname,
        tablename,
        tableowner
      FROM pg_tables 
      WHERE schemaname IN ('scrub', 'update')
      ORDER BY schemaname, tablename;
    `);
    
    if (schemaResult.rows.length > 0) {
      console.table(schemaResult.rows);
    } else {
      console.log('No scrub or update tables found.');
    }
    
  } catch (error) {
    console.error('❌ Database check failed:', error.message);
  } finally {
    await client.end();
  }
}

checkDatabase();
