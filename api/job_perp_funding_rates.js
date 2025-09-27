// api/job_perp_funding_rates.js
// Collect perpetual futures funding rates and store in update.perp_funding_rates

module.exports.config = { runtime: 'nodejs18.x' };

require('dotenv').config();
const { Pool } = require('pg');

function makePoolFromEnv() {
  const { SUPABASE_DB_URL } = process.env;
  if (SUPABASE_DB_URL) {
    return new Pool({
      connectionString: SUPABASE_DB_URL,
      ssl: { rejectUnauthorized: false },
      statement_timeout: 0,
      query_timeout: 0
    });
  }
  
  const { PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE } = process.env;
  if (!PGHOST || !PGPORT || !PGDATABASE || !PGUSER || !PGPASSWORD) {
    throw new Error('Missing DB env: need SUPABASE_DB_URL or PG* variables');
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

function generateJobRunId() {
  return `perp_funding_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

module.exports = async (req, res) => {
  const jobRunId = generateJobRunId();
  const startTime = Date.now();
  
  let totalRecords = 0;
  let insertedRecords = 0;
  let skippedRecords = 0;

  try {
    const { DEFILLAMA_API_KEY } = process.env;
    if (!DEFILLAMA_API_KEY) {
      return res.status(500).json({ error: 'Missing DEFILLAMA_API_KEY' });
    }

    console.log(`💰 Starting Perp Funding Rates collection: ${jobRunId}`);

    // Fetch perp funding rates from DeFiLlama Pro API
    const apiUrl = `https://pro-api.llama.fi/${DEFILLAMA_API_KEY}/yields/perps`;
    console.log(`📡 Fetching perp funding data from: ${apiUrl}`);
    
    const response = await fetch(apiUrl);
    if (!response.ok) {
      throw new Error(`API request failed: ${response.status} ${response.statusText}`);
    }
    
    const response_data = await response.json();
    
    if (response_data.status !== 'success' || !Array.isArray(response_data.data)) {
      throw new Error('Invalid API response format - expected success status with data array');
    }

    const data = response_data.data;
    console.log(`📊 Received ${data.length} perp funding rate records`);
    totalRecords = data.length;

    const pool = makePoolFromEnv();
    const client = await pool.connect();
    
    try {
      // Process each funding rate record
      for (const item of data) {
        // Validate required fields
        if (!item.perp_id || !item.timestamp || !item.marketplace || !item.market || !item.baseAsset) {
          skippedRecords++;
          continue;
        }

        // Insert into update.perp_funding_rates table
        await client.query(`
          INSERT INTO update.perp_funding_rates (
            perp_id, timestamp, marketplace, market, base_asset,
            funding_rate, funding_rate_previous, funding_time_previous,
            open_interest, index_price,
            funding_rate_7d_average, funding_rate_7d_sum,
            funding_rate_30d_average, funding_rate_30d_sum
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
          ON CONFLICT (perp_id, timestamp) DO UPDATE SET
            marketplace = EXCLUDED.marketplace,
            market = EXCLUDED.market,
            base_asset = EXCLUDED.base_asset,
            funding_rate = EXCLUDED.funding_rate,
            funding_rate_previous = EXCLUDED.funding_rate_previous,
            funding_time_previous = EXCLUDED.funding_time_previous,
            open_interest = EXCLUDED.open_interest,
            index_price = EXCLUDED.index_price,
            funding_rate_7d_average = EXCLUDED.funding_rate_7d_average,
            funding_rate_7d_sum = EXCLUDED.funding_rate_7d_sum,
            funding_rate_30d_average = EXCLUDED.funding_rate_30d_average,
            funding_rate_30d_sum = EXCLUDED.funding_rate_30d_sum,
            inserted_at = NOW()
        `, [
          item.perp_id,
          item.timestamp,
          item.marketplace,
          item.market,
          item.baseAsset,
          item.fundingRate,
          item.fundingRatePrevious,
          item.fundingTimePrevious,
          item.openInterest,
          item.indexPrice,
          item.fundingRate7dAverage,
          item.fundingRate7dSum,
          item.fundingRate30dAverage,
          item.fundingRate30dSum
        ]);
        
        insertedRecords++;
      }

    } finally {
      client.release();
      await pool.end();
    }

    const processingTime = Date.now() - startTime;
    const success = insertedRecords > 0;

    console.log(`✅ Perp funding rates collection completed: ${insertedRecords}/${totalRecords} records inserted`);

    return res.status(200).json({
      success,
      job_run_id: jobRunId,
      date: new Date().toISOString(),
      
      // Metrics
      total_records: totalRecords,
      inserted_records: insertedRecords,
      skipped_records: skippedRecords,
      success_rate: totalRecords > 0 ? (insertedRecords / totalRecords) * 100 : 0,
      
      // Processing time
      processing_time_ms: processingTime,
      
      // Success message
      message: success ? 
        `Successfully inserted ${insertedRecords}/${totalRecords} perp funding rate records (${skippedRecords} skipped)` :
        `Failed to insert any perp funding rate records`
    });
    
  } catch (e) {
    console.error('Perp funding rates job failed:', e.message);
    
    return res.status(500).json({ 
      success: false,
      job_run_id: jobRunId,
      error: e.message,
      total_records: totalRecords,
      inserted_records: insertedRecords,
      skipped_records: skippedRecords,
      processing_time_ms: Date.now() - startTime
    });
  }
};
