// api/job_dex_info.js
// Collect DEX volume and metrics data from DeFiLlama Pro API
// Populates update.dex_info table with comprehensive DEX trading data

module.exports.config = { runtime: 'nodejs18.x' };

require('dotenv').config();
const { Pool } = require('pg');

function generateJobRunId() {
  return `dex_info_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
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

    console.log(`📊 Starting DEX Info Collection: ${jobRunId}`);

    // Fetch DEX overview data from DeFiLlama Pro API
    const apiUrl = `https://pro-api.llama.fi/${DEFILLAMA_API_KEY}/api/overview/dexs`;
    console.log(`📡 Fetching DEX data from: ${apiUrl}`);
    
    const response = await fetch(apiUrl);
    if (!response.ok) {
      throw new Error(`API request failed: ${response.status} ${response.statusText}`);
    }
    
    const data = await response.json();
    
    if (!data.protocols || !Array.isArray(data.protocols)) {
      throw new Error('Invalid API response format - expected protocols array');
    }

    console.log(`📊 Received ${data.protocols.length} DEX protocols`);
    totalRecords = data.protocols.length;

    const pool = makePoolFromEnv();
    const client = await pool.connect();
    
    try {
      await client.query('BEGIN');
      
      // Process each DEX protocol
      for (const protocol of data.protocols) {
        // Validate required fields
        if (!protocol.defillamaId || !protocol.name) {
          skippedRecords++;
          continue;
        }

        // Insert or update the DEX data
        const result = await client.query(`
          INSERT INTO update.dex_info (
            defillama_id, name, display_name, module, category, logo, chains,
            protocol_type, methodology_url, parent_protocol, slug,
            total_24h, total_48h_to_24h, total_7d, total_14d_to_7d, total_30d,
            total_60d_to_30d, total_1y, total_all_time, average_1y, monthly_average_1y,
            change_1d, change_7d, change_1m, change_7d_over_7d, change_30d_over_30d,
            total_7_days_ago, total_30_days_ago,
            breakdown_24h, breakdown_30d, methodology, linked_protocols,
            data_timestamp, inserted_at
          )
          VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
            $12, $13, $14, $15, $16, $17, $18, $19, $20, $21,
            $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32,
            NOW(), NOW()
          )
          ON CONFLICT (defillama_id, data_timestamp) 
          DO UPDATE SET 
            name = EXCLUDED.name,
            display_name = EXCLUDED.display_name,
            total_24h = EXCLUDED.total_24h,
            total_7d = EXCLUDED.total_7d,
            total_30d = EXCLUDED.total_30d,
            change_1d = EXCLUDED.change_1d,
            change_7d = EXCLUDED.change_7d,
            change_1m = EXCLUDED.change_1m,
            breakdown_24h = EXCLUDED.breakdown_24h,
            breakdown_30d = EXCLUDED.breakdown_30d,
            inserted_at = NOW()
          RETURNING (xmax = 0) AS inserted
        `, [
          protocol.defillamaId,
          protocol.name,
          protocol.displayName || protocol.name,
          protocol.module,
          protocol.category || 'Dexs',
          protocol.logo,
          protocol.chains || [],
          protocol.protocolType,
          protocol.methodologyURL,
          protocol.parentProtocol,
          protocol.slug,
          protocol.total24h,
          protocol.total48hto24h,
          protocol.total7d,
          protocol.total14dto7d,
          protocol.total30d,
          protocol.total60dto30d,
          protocol.total1y,
          protocol.totalAllTime,
          protocol.average1y,
          protocol.monthlyAverage1y,
          protocol.change_1d,
          protocol.change_7d,
          protocol.change_1m,
          protocol.change_7dover7d,
          protocol.change_30dover30d,
          protocol.total7DaysAgo,
          protocol.total30DaysAgo,
          JSON.stringify(protocol.breakdown24h),
          JSON.stringify(protocol.breakdown30d),
          JSON.stringify(protocol.methodology),
          protocol.linkedProtocols || []
        ]);

        if (result.rows[0].inserted) {
          insertedRecords++;
        } else {
          updatedRecords++;
        }
      }
      
      await client.query('COMMIT');
      console.log(`✅ DEX info collection completed: ${insertedRecords} inserted, ${updatedRecords} updated, ${skippedRecords} skipped`);
      
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
      message: `Successfully processed ${totalRecords} DEX protocols: ${insertedRecords} inserted, ${updatedRecords} updated, ${skippedRecords} skipped`
    });

  } catch (error) {
    console.error(`❌ DEX info collection failed: ${error.message}`);
    
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
