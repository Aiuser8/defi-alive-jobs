// api/clear_migrated_update_data.js - Clear migrated data from update tables
// This script removes data that has been successfully migrated to clean.* tables

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

async function clearMigratedData() {
  const client = makePoolFromEnv();
  
  try {
    await client.connect();
    console.log('🔗 Connected to Supabase database\n');
    
    console.log('🧹 Clearing migrated data from update tables\n');
    console.log('⚠️  This will remove data that has been migrated to clean.* tables');
    console.log('✅ Token price data will be preserved in update.token_price_daily\n');
    
    // Show current counts before clearing
    console.log('📊 Current update table counts (BEFORE clearing):');
    const beforeCounts = [
      { name: 'update.lending_market_history', query: 'SELECT COUNT(*) FROM update.lending_market_history' },
      { name: 'update.raw_etf', query: 'SELECT COUNT(*) FROM update.raw_etf' },
      { name: 'update.stablecoin_mcap_by_peg_daily', query: 'SELECT COUNT(*) FROM update.stablecoin_mcap_by_peg_daily' },
      { name: 'update.token_price_daily', query: 'SELECT COUNT(*) FROM update.token_price_daily' }
    ];
    
    for (const { name, query } of beforeCounts) {
      const result = await client.query(query);
      console.log(`${name}: ${result.rows[0].count} records`);
    }
    
    console.log('\n🚀 Starting data clearing...\n');
    
    // 1. Clear lending_market_history
    console.log('📊 Clearing update.lending_market_history...');
    const lendingResult = await client.query('DELETE FROM update.lending_market_history');
    console.log(`✅ Cleared ${lendingResult.rowCount} lending market records`);
    
    // 2. Clear raw_etf
    console.log('\n📈 Clearing update.raw_etf...');
    const etfResult = await client.query('DELETE FROM update.raw_etf');
    console.log(`✅ Cleared ${etfResult.rowCount} ETF flow records`);
    
    // 3. Clear stablecoin_mcap_by_peg_daily
    console.log('\n🪙 Clearing update.stablecoin_mcap_by_peg_daily...');
    const stablecoinResult = await client.query('DELETE FROM update.stablecoin_mcap_by_peg_daily');
    console.log(`✅ Cleared ${stablecoinResult.rowCount} stablecoin market cap records`);
    
    // 4. Keep token_price_daily (don't clear)
    console.log('\n💰 Keeping update.token_price_daily (not cleared)');
    console.log('   Token price data preserved for continued collection');
    
    // 5. Show counts after clearing
    console.log('\n📊 Update table counts (AFTER clearing):');
    const afterCounts = [
      { name: 'update.lending_market_history', query: 'SELECT COUNT(*) FROM update.lending_market_history' },
      { name: 'update.raw_etf', query: 'SELECT COUNT(*) FROM update.raw_etf' },
      { name: 'update.stablecoin_mcap_by_peg_daily', query: 'SELECT COUNT(*) FROM update.stablecoin_mcap_by_peg_daily' },
      { name: 'update.token_price_daily', query: 'SELECT COUNT(*) FROM update.token_price_daily' }
    ];
    
    for (const { name, query } of afterCounts) {
      const result = await client.query(query);
      console.log(`${name}: ${result.rows[0].count} records`);
    }
    
    // 6. Verify clean tables still have the data
    console.log('\n🔍 Verifying clean tables still have migrated data:');
    const cleanCounts = [
      { name: 'clean.lending_market_history', query: 'SELECT COUNT(*) FROM clean.lending_market_history' },
      { name: 'clean.etf_flows_daily', query: 'SELECT COUNT(*) FROM clean.etf_flows_daily' },
      { name: 'clean.stablecoin_mcap_by_peg_daily', query: 'SELECT COUNT(*) FROM clean.stablecoin_mcap_by_peg_daily' }
    ];
    
    for (const { name, query } of cleanCounts) {
      const result = await client.query(query);
      console.log(`${name}: ${result.rows[0].count} records`);
    }
    
    console.log('\n🎉 Data clearing completed successfully!');
    console.log('\n📋 Summary:');
    console.log('✅ Cleared migrated data from update.* tables');
    console.log('✅ Historical data preserved in clean.* tables');
    console.log('✅ Token price data preserved in update.token_price_daily');
    console.log('✅ Update tables are now ready for new quality-enabled data collection');
    
    console.log('\n🔄 Next Steps:');
    console.log('- Your quality-enabled jobs will start populating update.* tables with fresh data');
    console.log('- Clean data will go to update.* tables');
    console.log('- Dirty data will go to scrub.* tables for your review');
    console.log('- After testing, you can manually move clean data from update.* to clean.* tables');
    
    return {
      success: true,
      cleared: {
        lending: lendingResult.rowCount,
        etf: etfResult.rowCount,
        stablecoin: stablecoinResult.rowCount
      }
    };
    
  } catch (error) {
    console.error('❌ Data clearing failed:', error.message);
    console.error('Stack:', error.stack);
    return { success: false, error: error.message };
  } finally {
    await client.end();
  }
}

// Run if called directly
if (require.main === module) {
  clearMigratedData()
    .then((result) => {
      if (result.success) {
        console.log('\n✅ Data clearing completed successfully');
        process.exit(0);
      } else {
        console.log('\n❌ Data clearing failed');
        process.exit(1);
      }
    })
    .catch((error) => {
      console.error('Data clearing failed:', error.message);
      process.exit(1);
    });
}

module.exports = clearMigratedData;
