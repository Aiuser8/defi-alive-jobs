// check_recent_job_performance.js - Check the most recent job performance in detail
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

async function checkRecentJobPerformance() {
  const client = makePoolFromEnv();
  
  try {
    await client.connect();
    console.log('🔗 Connected to Supabase database\n');
    
    console.log('🔍 Analyzing Recent Token Price Job Performance\n');
    
    // 1. Check the most recent job run in detail
    console.log('📊 Most Recent Job Run Details:');
    const recentJob = await client.query(`
      SELECT 
        job_run_id,
        run_timestamp,
        total_records,
        clean_records,
        scrubbed_records,
        error_records,
        outlier_records,
        overall_quality_score,
        error_summary
      FROM scrub.data_quality_summary
      WHERE job_name = 'backfill_token_prices'
      ORDER BY run_timestamp DESC
      LIMIT 1;
    `);
    
    if (recentJob.rows.length > 0) {
      const job = recentJob.rows[0];
      console.log(`Job Run ID: ${job.job_run_id}`);
      console.log(`Timestamp: ${job.run_timestamp}`);
      console.log(`Total Records: ${job.total_records}`);
      console.log(`Clean Records: ${job.clean_records}`);
      console.log(`Scrubbed Records: ${job.scrubbed_records}`);
      console.log(`Error Records: ${job.error_records}`);
      console.log(`Quality Score: ${job.overall_quality_score}`);
      
      if (job.error_summary) {
        console.log('Error Breakdown:');
        for (const [error, count] of Object.entries(job.error_summary)) {
          console.log(`  ${error}: ${count}`);
        }
      }
    }
    
    // 2. Check what data was actually inserted vs scrubbed in the last hour
    console.log('\n📈 Data Insertion vs Scrubbing (Last Hour):');
    
    const cleanInserted = await client.query(`
      SELECT COUNT(*) as count
      FROM update.token_price_daily
      WHERE price_timestamp >= NOW() - INTERVAL '1 hour';
    `);
    
    const scrubbed = await client.query(`
      SELECT COUNT(*) as count
      FROM scrub.token_price_scrub
      WHERE processed_at >= NOW() - INTERVAL '1 hour';
    `);
    
    console.log(`Clean data inserted: ${cleanInserted.rows[0].count}`);
    console.log(`Data scrubbed: ${scrubbed.rows[0].count}`);
    
    // 3. Sample some recent scrubbed data to see why it's being scrubbed
    console.log('\n🔍 Sample Recent Scrubbed Data (with reasons):');
    const scrubbedSample = await client.query(`
      SELECT 
        coin_id,
        price_usd,
        price_timestamp,
        validation_errors,
        quality_score,
        original_data,
        processed_at
      FROM scrub.token_price_scrub
      WHERE processed_at >= NOW() - INTERVAL '30 minutes'
      ORDER BY processed_at DESC
      LIMIT 3;
    `);
    
    for (const row of scrubbedSample.rows) {
      console.log('\n--- Scrubbed Record ---');
      console.log(`Coin ID: ${row.coin_id?.substring(0, 30)}...`);
      console.log(`Price: ${row.price_usd}`);
      console.log(`Price Timestamp: ${row.price_timestamp}`);
      console.log(`Quality Score: ${row.quality_score}`);
      console.log(`Validation Errors: ${row.validation_errors?.join(', ')}`);
      console.log(`Processed: ${row.processed_at}`);
      
      if (row.original_data && row.original_data.timestamp) {
        const apiTime = new Date(row.original_data.timestamp * 1000);
        const ageMinutes = (Date.now() - row.original_data.timestamp * 1000) / (1000 * 60);
        console.log(`API Timestamp: ${apiTime}`);
        console.log(`Age (minutes): ${ageMinutes.toFixed(1)}`);
      }
    }
    
    // 4. Check if the updated job is actually being deployed
    console.log('\n🚀 Deployment Status Check:');
    console.log('The fix has been pushed to main branch.');
    console.log('Vercel should automatically deploy the updated job.');
    console.log('If stale_data errors persist, the deployment may not be live yet.');
    
    // 5. Expected behavior
    console.log('\n✅ Expected Behavior After Fix:');
    console.log('- Clean records should be > 0');
    console.log('- Stale_data errors should decrease significantly');
    console.log('- Quality scores should improve to 80-100');
    console.log('- Data age should be < 60 minutes');
    
  } catch (error) {
    console.error('❌ Check failed:', error.message);
  } finally {
    await client.end();
  }
}

checkRecentJobPerformance();
