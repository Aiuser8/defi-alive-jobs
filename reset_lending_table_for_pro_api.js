const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  connectionString: process.env.SUPABASE_DB_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

async function resetLendingTableForProApi() {
  const client = await pool.connect();
  try {
    console.log('🗑️ Clearing update.lending_market_history table...');
    
    // Clear all existing data
    await client.query('DELETE FROM update.lending_market_history;');
    console.log('✅ Table cleared successfully');
    
    console.log('🔧 Optimizing schema for Pro API data...');
    
    // Drop old constraints that might conflict
    await client.query(`
      DO $$ 
      BEGIN
        -- Drop old constraint if it exists
        IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_market_ts') THEN
          ALTER TABLE update.lending_market_history DROP CONSTRAINT uniq_market_ts;
        END IF;
        
        -- Drop old constraint if it exists  
        IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_pool_data_timestamp') THEN
          ALTER TABLE update.lending_market_history DROP CONSTRAINT uniq_pool_data_timestamp;
        END IF;
      END $$;
    `);
    
    // Add simple constraint for Pro API data
    await client.query(`
      -- Add simple constraint for Pro API data structure
      ALTER TABLE update.lending_market_history
      ADD CONSTRAINT uniq_pool_timestamp UNIQUE (pool_id, data_timestamp);
    `);
    
    console.log('✅ Schema optimized for Pro API');
    
    // Show current schema structure
    const { rows } = await client.query(`
      SELECT 
        column_name, 
        data_type, 
        is_nullable,
        CASE 
          WHEN column_name IN ('pool_id', 'project', 'chain', 'symbol', 'tvl_usd', 'total_supply_usd', 'total_borrow_usd', 'apy', 'apy_base_borrow', 'ltv') 
          THEN '🔑 Pro API Key Field'
          WHEN column_name IN ('data_timestamp', 'created_at')
          THEN '⏰ Timestamp Field'
          WHEN column_name LIKE 'apy_%'
          THEN '📊 APY Metric'
          WHEN data_type = 'jsonb'
          THEN '📋 JSON Data'
          ELSE '📝 Other Field'
        END as field_type
      FROM information_schema.columns 
      WHERE table_schema = 'update' AND table_name = 'lending_market_history'
      ORDER BY 
        CASE 
          WHEN column_name IN ('pool_id', 'project', 'chain', 'symbol') THEN 1
          WHEN column_name IN ('tvl_usd', 'total_supply_usd', 'total_borrow_usd') THEN 2
          WHEN column_name LIKE 'apy%' THEN 3
          WHEN column_name IN ('ltv', 'borrowable', 'stablecoin') THEN 4
          WHEN data_type = 'jsonb' THEN 5
          WHEN column_name IN ('data_timestamp', 'created_at') THEN 6
          ELSE 7
        END,
        ordinal_position;
    `);
    
    console.log('\n📊 Optimized schema for Pro API lending data:');
    console.table(rows);
    
    console.log('\n🎯 Key Pro API fields ready:');
    console.log('  🔑 pool_id (primary identifier)');
    console.log('  🏢 project, chain, symbol (classification)');
    console.log('  💰 tvl_usd, total_supply_usd, total_borrow_usd (financial)');
    console.log('  📈 apy, apy_base_borrow, apy_reward_borrow (yields)');
    console.log('  🎯 ltv, borrowable, stablecoin (lending specifics)');
    console.log('  📋 predictions, reward_tokens, underlying_tokens (JSONB)');
    
  } catch (error) {
    console.error('❌ Reset failed:', error.message);
    throw error;
  } finally {
    client.release();
  }
}

if (require.main === module) {
  resetLendingTableForProApi()
    .then(() => {
      console.log('\n🎉 Lending table reset and optimized for Pro API!');
      console.log('✅ Ready for fresh Pro API lending data collection');
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Reset failed:', error.message);
      process.exit(1);
    });
}

module.exports = { resetLendingTableForProApi };
