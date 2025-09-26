const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  connectionString: process.env.SUPABASE_DB_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

async function createProtocolFeesTable() {
  const client = await pool.connect();
  try {
    console.log('💰 Creating update.protocol_fees_daily table...');

    await client.query(`
      -- Create protocol fees and revenue table for live collection
      CREATE TABLE IF NOT EXISTS update.protocol_fees_daily (
        id SERIAL PRIMARY KEY,
        protocol_id TEXT NOT NULL,
        defillama_id TEXT,
        name TEXT,
        display_name TEXT,
        slug TEXT,
        category TEXT,
        chains TEXT[],
        module TEXT,
        protocol_type TEXT,
        logo TEXT,
        
        -- Core metrics
        total_24h NUMERIC,
        total_48h_to_24h NUMERIC,
        total_7d NUMERIC,
        total_14d_to_7d NUMERIC,
        total_30d NUMERIC,
        total_60d_to_30d NUMERIC,
        total_1y NUMERIC,
        total_all_time NUMERIC,
        
        -- Averages
        average_1y NUMERIC,
        monthly_average_1y NUMERIC,
        
        -- Changes (%)
        change_1d NUMERIC,
        change_7d NUMERIC,
        change_1m NUMERIC,
        change_7d_over_7d NUMERIC,
        change_30d_over_30d NUMERIC,
        
        -- Reference data
        total_7_days_ago NUMERIC,
        total_30_days_ago NUMERIC,
        
        -- Breakdowns (JSONB for flexibility)
        breakdown_24h JSONB,
        breakdown_30d JSONB,
        
        -- Methodology
        methodology JSONB,
        methodology_url TEXT,
        
        -- Metadata
        collection_date DATE NOT NULL,
        inserted_at TIMESTAMPTZ DEFAULT NOW(),
        
        UNIQUE(protocol_id, collection_date)
      );
    `);

    await client.query(`
      -- Add comment
      COMMENT ON TABLE update.protocol_fees_daily IS 'Live protocol fees and revenue data collected from DeFiLlama /overview/fees endpoint with detailed breakdowns by chain and comprehensive metrics.';
    `);

    await client.query(`
      -- Create indexes for performance
      CREATE INDEX IF NOT EXISTS idx_protocol_fees_daily_date ON update.protocol_fees_daily(collection_date DESC);
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_protocol_fees_daily_category ON update.protocol_fees_daily(category, total_24h DESC);
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_protocol_fees_daily_revenue ON update.protocol_fees_daily(total_24h DESC) WHERE total_24h > 0;
    `);

    console.log('✅ Successfully created update.protocol_fees_daily table with indexes');
    
  } catch (error) {
    console.error('❌ Error creating table:', error.message);
  } finally {
    client.release();
  }
}

createProtocolFeesTable().catch(console.error);
