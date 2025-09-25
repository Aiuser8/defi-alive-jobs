// api/setup_scrub_tables.js
// Database migration script to create scrub tables and quality monitoring system

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

async function setupScrubTables() {
  const client = makePoolFromEnv();
  
  try {
    await client.connect();
    console.log('Connected to database');

    // Create scrub schema
    await client.query('CREATE SCHEMA IF NOT EXISTS scrub;');
    console.log('✓ Created scrub schema');

    // 1. Token Price Scrub Table
    await client.query(`
      CREATE TABLE IF NOT EXISTS scrub.token_price_scrub (
        id SERIAL PRIMARY KEY,
        coin_id TEXT NOT NULL,
        symbol TEXT,
        price_usd NUMERIC,
        confidence DECIMAL(3,2),
        decimals INTEGER,
        price_timestamp TIMESTAMPTZ,
        
        -- Quality assessment
        validation_errors TEXT[],
        quality_score INTEGER, -- 0-100
        is_outlier BOOLEAN DEFAULT FALSE,
        outlier_reason TEXT,
        
        -- Original data for reference
        original_data JSONB,
        
        -- Processing metadata
        processed_at TIMESTAMPTZ DEFAULT NOW(),
        job_run_id TEXT,
        retry_count INTEGER DEFAULT 0
      );
    `);
    console.log('✓ Created token_price_scrub table');

    // 2. Lending Market Scrub Table
    await client.query(`
      CREATE TABLE IF NOT EXISTS scrub.lending_market_scrub (
        id SERIAL PRIMARY KEY,
        market_id TEXT NOT NULL,
        ts TIMESTAMPTZ NOT NULL,
        
        -- Lending data
        total_supply_usd NUMERIC,
        total_borrow_usd NUMERIC,
        debt_ceiling_usd NUMERIC,
        apy_base_supply NUMERIC,
        apy_reward_supply NUMERIC,
        apy_base_borrow NUMERIC,
        apy_reward_borrow NUMERIC,
        
        -- Quality assessment
        validation_errors TEXT[],
        quality_score INTEGER,
        is_outlier BOOLEAN DEFAULT FALSE,
        outlier_reason TEXT,
        
        -- Original data for reference
        original_data JSONB,
        
        -- Processing metadata
        processed_at TIMESTAMPTZ DEFAULT NOW(),
        job_run_id TEXT,
        retry_count INTEGER DEFAULT 0
      );
    `);
    console.log('✓ Created lending_market_scrub table');

    // 3. ETF Flow Scrub Table
    await client.query(`
      CREATE TABLE IF NOT EXISTS scrub.etf_flow_scrub (
        id SERIAL PRIMARY KEY,
        gecko_id TEXT NOT NULL,
        day DATE NOT NULL,
        total_flow_usd NUMERIC,
        
        -- Quality assessment
        validation_errors TEXT[],
        quality_score INTEGER,
        is_outlier BOOLEAN DEFAULT FALSE,
        outlier_reason TEXT,
        
        -- Original data for reference
        original_data JSONB,
        
        -- Processing metadata
        processed_at TIMESTAMPTZ DEFAULT NOW(),
        job_run_id TEXT,
        retry_count INTEGER DEFAULT 0
      );
    `);
    console.log('✓ Created etf_flow_scrub table');

    // 4. Stablecoin Market Cap Scrub Table
    await client.query(`
      CREATE TABLE IF NOT EXISTS scrub.stablecoin_mcap_scrub (
        id SERIAL PRIMARY KEY,
        day DATE NOT NULL,
        peg TEXT NOT NULL,
        amount_usd NUMERIC,
        
        -- Quality assessment
        validation_errors TEXT[],
        quality_score INTEGER,
        is_outlier BOOLEAN DEFAULT FALSE,
        outlier_reason TEXT,
        
        -- Original data for reference
        original_data JSONB,
        
        -- Processing metadata
        processed_at TIMESTAMPTZ DEFAULT NOW(),
        job_run_id TEXT,
        retry_count INTEGER DEFAULT 0
      );
    `);
    console.log('✓ Created stablecoin_mcap_scrub table');

    // 5. Data Quality Summary Table
    await client.query(`
      CREATE TABLE IF NOT EXISTS scrub.data_quality_summary (
        id SERIAL PRIMARY KEY,
        job_name TEXT NOT NULL,
        job_run_id TEXT NOT NULL,
        run_timestamp TIMESTAMPTZ DEFAULT NOW(),
        
        -- Quality metrics
        total_records INTEGER,
        clean_records INTEGER,
        scrubbed_records INTEGER,
        error_records INTEGER,
        outlier_records INTEGER,
        
        -- Quality score
        overall_quality_score DECIMAL(5,2),
        
        -- Processing time
        processing_time_ms INTEGER,
        
        -- Error summary
        error_summary JSONB,
        
        UNIQUE(job_name, job_run_id)
      );
    `);
    console.log('✓ Created data_quality_summary table');

    // Create indexes for performance
    const indexes = [
      'CREATE INDEX IF NOT EXISTS idx_token_price_scrub_processed_at ON scrub.token_price_scrub(processed_at);',
      'CREATE INDEX IF NOT EXISTS idx_token_price_scrub_quality_score ON scrub.token_price_scrub(quality_score);',
      'CREATE INDEX IF NOT EXISTS idx_token_price_scrub_is_outlier ON scrub.token_price_scrub(is_outlier);',
      'CREATE INDEX IF NOT EXISTS idx_token_price_scrub_coin_id ON scrub.token_price_scrub(coin_id);',
      
      'CREATE INDEX IF NOT EXISTS idx_lending_market_scrub_processed_at ON scrub.lending_market_scrub(processed_at);',
      'CREATE INDEX IF NOT EXISTS idx_lending_market_scrub_quality_score ON scrub.lending_market_scrub(quality_score);',
      'CREATE INDEX IF NOT EXISTS idx_lending_market_scrub_is_outlier ON scrub.lending_market_scrub(is_outlier);',
      'CREATE INDEX IF NOT EXISTS idx_lending_market_scrub_market_id ON scrub.lending_market_scrub(market_id);',
      
      'CREATE INDEX IF NOT EXISTS idx_etf_flow_scrub_processed_at ON scrub.etf_flow_scrub(processed_at);',
      'CREATE INDEX IF NOT EXISTS idx_etf_flow_scrub_quality_score ON scrub.etf_flow_scrub(quality_score);',
      'CREATE INDEX IF NOT EXISTS idx_etf_flow_scrub_gecko_id ON scrub.etf_flow_scrub(gecko_id);',
      
      'CREATE INDEX IF NOT EXISTS idx_stablecoin_mcap_scrub_processed_at ON scrub.stablecoin_mcap_scrub(processed_at);',
      'CREATE INDEX IF NOT EXISTS idx_stablecoin_mcap_scrub_quality_score ON scrub.stablecoin_mcap_scrub(quality_score);',
      'CREATE INDEX IF NOT EXISTS idx_stablecoin_mcap_scrub_day ON scrub.stablecoin_mcap_scrub(day);',
      
      'CREATE INDEX IF NOT EXISTS idx_data_quality_summary_run_timestamp ON scrub.data_quality_summary(run_timestamp);',
      'CREATE INDEX IF NOT EXISTS idx_data_quality_summary_job_name ON scrub.data_quality_summary(job_name);',
      'CREATE INDEX IF NOT EXISTS idx_data_quality_summary_quality_score ON scrub.data_quality_summary(overall_quality_score);'
    ];

    for (const indexQuery of indexes) {
      await client.query(indexQuery);
    }
    console.log('✓ Created performance indexes');

    // Create a view for easy monitoring of data quality
    await client.query(`
      CREATE OR REPLACE VIEW scrub.quality_dashboard AS
      SELECT 
        job_name,
        DATE(run_timestamp) as run_date,
        COUNT(*) as total_runs,
        AVG(overall_quality_score) as avg_quality_score,
        AVG(total_records) as avg_total_records,
        AVG(clean_records) as avg_clean_records,
        AVG(scrubbed_records) as avg_scrubbed_records,
        AVG(error_records) as avg_error_records,
        AVG(processing_time_ms) as avg_processing_time_ms,
        SUM(total_records) as total_records_today,
        SUM(clean_records) as total_clean_records_today,
        SUM(scrubbed_records) as total_scrubbed_records_today,
        SUM(error_records) as total_error_records_today
      FROM scrub.data_quality_summary
      WHERE run_timestamp >= CURRENT_DATE - INTERVAL '7 days'
      GROUP BY job_name, DATE(run_timestamp)
      ORDER BY run_date DESC, job_name;
    `);
    console.log('✓ Created quality_dashboard view');

    console.log('\n🎉 Scrub tables setup completed successfully!');
    console.log('\nNext steps:');
    console.log('1. Update your Vercel cron jobs to use the new quality-enabled endpoints');
    console.log('2. Monitor data quality using: SELECT * FROM scrub.quality_dashboard;');
    console.log('3. Review scrubbed data using: SELECT * FROM scrub.token_price_scrub WHERE processed_at >= CURRENT_DATE;');

  } catch (error) {
    console.error('❌ Error setting up scrub tables:', error.message);
    throw error;
  } finally {
    await client.end();
  }
}

// Run if called directly
if (require.main === module) {
  setupScrubTables()
    .then(() => {
      console.log('Setup completed');
      process.exit(0);
    })
    .catch((error) => {
      console.error('Setup failed:', error.message);
      process.exit(1);
    });
}

module.exports = setupScrubTables;
