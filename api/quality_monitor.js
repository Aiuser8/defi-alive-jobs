// api/quality_monitor.js
// Data quality monitoring and reporting endpoint

const { Client } = require('pg');

function makePoolFromEnv() {
  const { SUPABASE_DB_URL } = process.env;
  if (SUPABASE_DB_URL) {
    return new Client({
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

module.exports = async (req, res) => {
  const client = makePoolFromEnv();
  
  try {
    await client.connect();
    
    // Parse query parameters for filtering
    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const days = parseInt(url.searchParams.get('days') || '7', 10);
    const jobName = url.searchParams.get('job');
    const reportType = url.searchParams.get('type') || 'summary';

    let results = {};

    switch (reportType) {
      case 'summary':
        // Overall quality summary
        const summaryQuery = `
          SELECT 
            job_name,
            COUNT(*) as total_runs,
            AVG(overall_quality_score) as avg_quality_score,
            AVG(total_records) as avg_total_records,
            AVG(clean_records) as avg_clean_records,
            AVG(scrubbed_records) as avg_scrubbed_records,
            AVG(error_records) as avg_error_records,
            AVG(outlier_records) as avg_outlier_records,
            AVG(processing_time_ms) as avg_processing_time_ms,
            MIN(run_timestamp) as first_run,
            MAX(run_timestamp) as last_run
          FROM scrub.data_quality_summary
          WHERE run_timestamp >= NOW() - INTERVAL '${days} days'
          ${jobName ? `AND job_name = '${jobName}'` : ''}
          GROUP BY job_name
          ORDER BY avg_quality_score DESC;
        `;
        const summaryResult = await client.query(summaryQuery);
        results.summary = summaryResult.rows;
        break;

      case 'daily':
        // Daily quality trends
        const dailyQuery = `
          SELECT 
            DATE(run_timestamp) as date,
            job_name,
            COUNT(*) as runs_per_day,
            AVG(overall_quality_score) as avg_quality_score,
            SUM(total_records) as total_records,
            SUM(clean_records) as clean_records,
            SUM(scrubbed_records) as scrubbed_records,
            SUM(error_records) as error_records
          FROM scrub.data_quality_summary
          WHERE run_timestamp >= NOW() - INTERVAL '${days} days'
          ${jobName ? `AND job_name = '${jobName}'` : ''}
          GROUP BY DATE(run_timestamp), job_name
          ORDER BY date DESC, job_name;
        `;
        const dailyResult = await client.query(dailyQuery);
        results.daily = dailyResult.rows;
        break;

      case 'errors':
        // Error analysis
        const errorsQuery = `
          SELECT 
            job_name,
            key as error_type,
            SUM(value::integer) as error_count
          FROM scrub.data_quality_summary,
               jsonb_each_text(error_summary) as kv(key, value)
          WHERE run_timestamp >= NOW() - INTERVAL '${days} days'
          ${jobName ? `AND job_name = '${jobName}'` : ''}
          GROUP BY job_name, key
          ORDER BY error_count DESC;
        `;
        const errorsResult = await client.query(errorsQuery);
        results.errors = errorsResult.rows;
        break;

      case 'outliers':
        // Outlier analysis
        const outliersQuery = `
          SELECT 
            'token_prices' as table_name,
            COUNT(*) as total_outliers,
            COUNT(DISTINCT coin_id) as unique_coins,
            AVG(quality_score) as avg_quality_score
          FROM scrub.token_price_scrub
          WHERE processed_at >= NOW() - INTERVAL '${days} days'
          AND is_outlier = true
          
          UNION ALL
          
          SELECT 
            'lending_markets' as table_name,
            COUNT(*) as total_outliers,
            COUNT(DISTINCT market_id) as unique_markets,
            AVG(quality_score) as avg_quality_score
          FROM scrub.lending_market_scrub
          WHERE processed_at >= NOW() - INTERVAL '${days} days'
          AND is_outlier = true
          
          UNION ALL
          
          SELECT 
            'etf_flows' as table_name,
            COUNT(*) as total_outliers,
            COUNT(DISTINCT gecko_id) as unique_etfs,
            AVG(quality_score) as avg_quality_score
          FROM scrub.etf_flow_scrub
          WHERE processed_at >= NOW() - INTERVAL '${days} days'
          AND is_outlier = true
          
          UNION ALL
          
          SELECT 
            'stablecoin_mcaps' as table_name,
            COUNT(*) as total_outliers,
            COUNT(DISTINCT peg) as unique_pegs,
            AVG(quality_score) as avg_quality_score
          FROM scrub.stablecoin_mcap_scrub
          WHERE processed_at >= NOW() - INTERVAL '${days} days'
          AND is_outlier = true;
        `;
        const outliersResult = await client.query(outliersQuery);
        results.outliers = outliersResult.rows;
        break;

      case 'recent':
        // Recent scrubbed records
        const recentQuery = `
          SELECT 
            'token_prices' as table_name,
            coin_id as identifier,
            price_usd,
            quality_score,
            validation_errors,
            outlier_reason,
            processed_at
          FROM scrub.token_price_scrub
          WHERE processed_at >= NOW() - INTERVAL '1 day'
          ORDER BY processed_at DESC
          LIMIT 50
          
          UNION ALL
          
          SELECT 
            'lending_markets' as table_name,
            market_id as identifier,
            NULL as price_usd,
            quality_score,
            validation_errors,
            outlier_reason,
            processed_at
          FROM scrub.lending_market_scrub
          WHERE processed_at >= NOW() - INTERVAL '1 day'
          ORDER BY processed_at DESC
          LIMIT 50;
        `;
        const recentResult = await client.query(recentQuery);
        results.recent = recentResult.rows;
        break;

      case 'health':
        // System health check
        const healthQuery = `
          WITH latest_runs AS (
            SELECT 
              job_name,
              MAX(run_timestamp) as last_run,
              AVG(overall_quality_score) as avg_quality_score,
              AVG(processing_time_ms) as avg_processing_time
            FROM scrub.data_quality_summary
            WHERE run_timestamp >= NOW() - INTERVAL '1 day'
            GROUP BY job_name
          )
          SELECT 
            job_name,
            last_run,
            EXTRACT(EPOCH FROM (NOW() - last_run))/3600 as hours_since_last_run,
            avg_quality_score,
            avg_processing_time,
            CASE 
              WHEN EXTRACT(EPOCH FROM (NOW() - last_run))/3600 > 25 THEN 'STALE'
              WHEN avg_quality_score < 80 THEN 'LOW_QUALITY'
              WHEN avg_processing_time > 300000 THEN 'SLOW'
              ELSE 'HEALTHY'
            END as health_status
          FROM latest_runs
          ORDER BY hours_since_last_run DESC;
        `;
        const healthResult = await client.query(healthQuery);
        results.health = healthResult.rows;
        break;

      default:
        return res.status(400).json({ 
          error: 'Invalid report type. Use: summary, daily, errors, outliers, recent, health' 
        });
    }

    await client.end();
    
    return res.status(200).json({
      report_type: reportType,
      days: days,
      job_filter: jobName || 'all',
      generated_at: new Date().toISOString(),
      results
    });

  } catch (error) {
    try { await client.end(); } catch {}
    return res.status(500).json({ 
      error: error.message,
      report_type: reportType 
    });
  }
};
