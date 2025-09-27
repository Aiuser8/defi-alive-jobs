// api/job_tvl_defi_daily.js
// Daily DeFi TVL Collection Job
// Fetches the most recent DeFi TVL data point and inserts it into clean.tvl_defi_hist table

module.exports.config = { runtime: 'nodejs18.x' };

require('dotenv').config();
const { Pool } = require('pg');

function generateJobRunId() {
  return `tvl_defi_daily_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

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
  const {
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE
  } = process.env;

  if (!PGHOST || !PGPORT || !PGDATABASE || !PGUSER || !PGPASSWORD) {
    throw new Error('Missing DB env: need SUPABASE_DB_URL or PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD');
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

module.exports = async (req, res) => {
  const jobRunId = generateJobRunId();
  const startTime = Date.now();
  
  let totalRecords = 0;
  let insertedRecords = 0;
  let updatedRecords = 0;
  let skippedRecords = 0;

  try {
    const { DEFILLAMA_API_KEY } = process.env;
    if (!DEFILLAMA_API_KEY) {
      return res.status(500).json({ error: 'Missing DEFILLAMA_API_KEY' });
    }

    console.log(`📊 Starting DeFi TVL Daily Collection: ${jobRunId}`);

    // Fetch total DeFi TVL data from DeFiLlama Pro API
    const apiUrl = `https://pro-api.llama.fi/${DEFILLAMA_API_KEY}/api/v2/historicalChainTvl`;
    console.log(`📡 Fetching DeFi TVL data from: ${apiUrl}`);
    
    const response = await fetch(apiUrl);
    if (!response.ok) {
      throw new Error(`API request failed: ${response.status} ${response.statusText}`);
    }
    
    const data = await response.json();
    
    if (!Array.isArray(data)) {
      throw new Error('Invalid API response format - expected array of TVL data');
    }

    console.log(`📊 Received ${data.length} TVL data points`);
    
    // Get only the most recent data point
    if (data.length === 0) {
      throw new Error('No TVL data received from API');
    }
    
    const mostRecentData = data[data.length - 1]; // Last item is most recent
    console.log(`📊 Processing most recent TVL data point: date=${mostRecentData.date}, tvl=${mostRecentData.tvl}`);
    
    totalRecords = 1; // We only process 1 record (the most recent)

    const pool = makePoolFromEnv();
    const client = await pool.connect();
    
    try {
      await client.query('BEGIN');
      
      // Validate the most recent data point
      if (!mostRecentData.date || typeof mostRecentData.tvl === 'undefined') {
        skippedRecords = 1;
        throw new Error('Most recent TVL data point is invalid');
      }

      // Date is already a unix timestamp
      const dateUnix = mostRecentData.date;
      const tvlUsd = parseFloat(mostRecentData.tvl);
      
      if (isNaN(tvlUsd) || tvlUsd < 0) {
        skippedRecords = 1;
        throw new Error('Most recent TVL value is invalid');
      }

      // Insert or update the most recent TVL data into clean.tvl_defi_hist
      const result = await client.query(`
        INSERT INTO clean.tvl_defi_hist (date, tvl, inserted_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (date) 
        DO UPDATE SET 
          tvl = EXCLUDED.tvl,
          inserted_at = NOW()
        RETURNING (xmax = 0) AS inserted
      `, [dateUnix, tvlUsd]);

      if (result.rows[0].inserted) {
        insertedRecords = 1;
      } else {
        updatedRecords = 1;
      }
      
      await client.query('COMMIT');
      console.log(`✅ DeFi TVL collection completed: ${insertedRecords} inserted, ${updatedRecords} updated, ${skippedRecords} skipped`);
      
    } catch (dbError) {
      await client.query('ROLLBACK');
      throw dbError;
    } finally {
      client.release();
      await pool.end();
    }

    const processingTime = Date.now() - startTime;
    const successRate = totalRecords > 0 ? ((insertedRecords + updatedRecords) / totalRecords * 100) : 0;

    return res.status(200).json({
      success: true,
      job_run_id: jobRunId,
      date: new Date().toISOString(),
      
      // Metrics
      total_records: totalRecords,
      inserted_records: insertedRecords,
      updated_records: updatedRecords,
      skipped_records: skippedRecords,
      success_rate: parseFloat(successRate.toFixed(1)),
      processing_time_ms: processingTime,
      
      // Summary
      message: `Successfully processed ${totalRecords} TVL data points: ${insertedRecords} inserted, ${updatedRecords} updated, ${skippedRecords} skipped`
    });

  } catch (error) {
    console.error(`❌ DeFi TVL collection failed: ${error.message}`);
    
    const processingTime = Date.now() - startTime;
    
    return res.status(500).json({
      success: false,
      job_run_id: jobRunId,
      date: new Date().toISOString(),
      error: error.message,
      total_records: totalRecords,
      inserted_records: insertedRecords,
      updated_records: updatedRecords,
      skipped_records: skippedRecords,
      processing_time_ms: processingTime
    });
  }
};
