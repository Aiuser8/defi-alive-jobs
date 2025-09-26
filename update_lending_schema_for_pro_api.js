const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  connectionString: process.env.SUPABASE_DB_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

async function updateLendingMarketSchema() {
  const client = await pool.connect();
  try {
    console.log('🔄 Updating update.lending_market_history schema for Pro API data...');
    
    // Add new columns to accommodate Pro API data structure
    await client.query(`
      -- Add new columns for Pro API data (using IF NOT EXISTS pattern)
      DO $$ 
      BEGIN
        -- Core identification fields
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'pool_id') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN pool_id TEXT;
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'project') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN project TEXT;
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'chain') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN chain TEXT;
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'symbol') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN symbol TEXT;
        END IF;
        
        -- TVL and supply/borrow data (enhanced)
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'tvl_usd') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN tvl_usd NUMERIC;
        END IF;
        
        -- APY fields (enhanced with reward borrow)
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'apy') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN apy NUMERIC;
        END IF;
        
        -- Percentage changes
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'apy_pct_1d') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN apy_pct_1d NUMERIC;
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'apy_pct_7d') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN apy_pct_7d NUMERIC;
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'apy_pct_30d') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN apy_pct_30d NUMERIC;
        END IF;
        
        -- Risk and classification fields
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'stablecoin') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN stablecoin BOOLEAN;
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'il_risk') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN il_risk TEXT;
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'exposure') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN exposure TEXT;
        END IF;
        
        -- Lending-specific fields
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'ltv') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN ltv NUMERIC;
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'borrowable') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN borrowable BOOLEAN;
        END IF;
        
        -- Statistical fields
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'mu') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN mu NUMERIC;
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'sigma') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN sigma NUMERIC;
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'count') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN count INTEGER;
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'outlier') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN outlier BOOLEAN;
        END IF;
        
        -- Average APY fields
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'apy_mean_30d') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN apy_mean_30d NUMERIC;
        END IF;
        
        -- JSONB fields for complex data
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'predictions') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN predictions JSONB;
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'reward_tokens') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN reward_tokens JSONB;
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'underlying_tokens') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN underlying_tokens JSONB;
        END IF;
        
        -- Additional metadata
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'pool_meta') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN pool_meta TEXT;
        END IF;
        
        -- Data freshness tracking
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'update' AND table_name = 'lending_market_history' AND column_name = 'data_timestamp') THEN
          ALTER TABLE update.lending_market_history ADD COLUMN data_timestamp TIMESTAMPTZ DEFAULT NOW();
        END IF;
        
      END $$;
    `);
    
    console.log('✅ Schema update completed successfully!');
    
    // Show the updated schema
    const { rows } = await client.query(`
      SELECT column_name, data_type, is_nullable, column_default
      FROM information_schema.columns 
      WHERE table_schema = 'update' AND table_name = 'lending_market_history'
      ORDER BY ordinal_position;
    `);
    
    console.log('\n📊 Updated schema for update.lending_market_history:');
    console.table(rows);
    
  } catch (error) {
    console.error('❌ Schema update failed:', error.message);
    throw error;
  } finally {
    client.release();
  }
}

if (require.main === module) {
  updateLendingMarketSchema()
    .then(() => {
      console.log('\n🎉 Lending market schema update complete!');
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Update failed:', error.message);
      process.exit(1);
    });
}

module.exports = { updateLendingMarketSchema };
