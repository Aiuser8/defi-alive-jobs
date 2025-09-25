// api/migrate_update_to_clean.js - Migrate data from update.* tables to clean.* tables
// This script moves historical data from update tables to clean tables for archival

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

async function migrateUpdateToClean() {
  const client = makePoolFromEnv();
  
  try {
    await client.connect();
    console.log('🔗 Connected to Supabase database\n');
    
    console.log('🚀 Starting data migration from update.* to clean.* tables\n');
    
    // 1. Migrate lending_market_history
    console.log('📊 Migrating lending_market_history...');
    console.log('   Mapping: ts (timestamp) -> ts (bigint), created_at -> inserted_at');
    const lendingResult = await client.query(`
      INSERT INTO clean.lending_market_history 
      (market_id, ts, project, chain, symbol, total_supply_usd, total_borrow_usd, 
       debt_ceiling_usd, apy_base_supply, apy_reward_supply, apy_base_borrow, apy_reward_borrow, inserted_at)
      SELECT 
        market_id, 
        EXTRACT(EPOCH FROM ts)::bigint as ts,
        project, chain, symbol, 
        total_supply_usd, total_borrow_usd,
        debt_ceiling_usd, apy_base_supply, apy_reward_supply, 
        apy_base_borrow, apy_reward_borrow, 
        created_at as inserted_at
      FROM update.lending_market_history
      ON CONFLICT DO NOTHING;
    `);
    console.log(`✅ Migrated ${lendingResult.rowCount} lending market records`);
    
    // 2. Migrate raw_etf to etf_flows_daily
    console.log('\n📈 Migrating raw_etf to etf_flows_daily...');
    console.log('   Mapping: created_at -> inserted_at');
    const etfResult = await client.query(`
      INSERT INTO clean.etf_flows_daily 
      (gecko_id, day, total_flow_usd, inserted_at)
      SELECT 
        gecko_id, day, total_flow_usd, 
        created_at::timestamptz as inserted_at
      FROM update.raw_etf
      ON CONFLICT DO NOTHING;
    `);
    console.log(`✅ Migrated ${etfResult.rowCount} ETF flow records`);
    
    // 3. Migrate stablecoin_mcap_by_peg_daily
    console.log('\n🪙 Migrating stablecoin_mcap_by_peg_daily...');
    console.log('   Columns match exactly');
    const stablecoinResult = await client.query(`
      INSERT INTO clean.stablecoin_mcap_by_peg_daily 
      (day, peg, amount_usd, ingest_time)
      SELECT 
        day, peg, amount_usd, ingest_time
      FROM update.stablecoin_mcap_by_peg_daily
      ON CONFLICT DO NOTHING;
    `);
    console.log(`✅ Migrated ${stablecoinResult.rowCount} stablecoin market cap records`);
    
    // 4. Token price data
    console.log('\n💰 Token Price Data:');
    console.log('   ⚠️  Token prices are kept in update.token_price_daily');
    console.log('   Reason: clean.token_price_daily has different structure and foreign key constraints');
    console.log('   New live token price data will continue to be collected in update.token_price_daily');
    
    // 5. Verify migration results
    console.log('\n🔍 Verifying migration results...');
    
    const verificationQueries = [
      { name: 'clean.lending_market_history', query: 'SELECT COUNT(*) FROM clean.lending_market_history' },
      { name: 'clean.etf_flows_daily', query: 'SELECT COUNT(*) FROM clean.etf_flows_daily' },
      { name: 'clean.stablecoin_mcap_by_peg_daily', query: 'SELECT COUNT(*) FROM clean.stablecoin_mcap_by_peg_daily' }
    ];
    
    for (const { name, query } of verificationQueries) {
      const result = await client.query(query);
      console.log(`${name}: ${result.rows[0].count} records`);
    }
    
    // 6. Show current update table counts
    console.log('\n📊 Current update table counts:');
    const updateCounts = [
      { name: 'update.lending_market_history', query: 'SELECT COUNT(*) FROM update.lending_market_history' },
      { name: 'update.raw_etf', query: 'SELECT COUNT(*) FROM update.raw_etf' },
      { name: 'update.stablecoin_mcap_by_peg_daily', query: 'SELECT COUNT(*) FROM update.stablecoin_mcap_by_peg_daily' },
      { name: 'update.token_price_daily', query: 'SELECT COUNT(*) FROM update.token_price_daily' }
    ];
    
    for (const { name, query } of updateCounts) {
      const result = await client.query(query);
      console.log(`${name}: ${result.rows[0].count} records`);
    }
    
    console.log('\n🎉 Data migration completed successfully!');
    console.log('\n📋 Summary:');
    console.log('✅ Historical data preserved in clean.* tables');
    console.log('✅ Update tables ready for new live data collection');
    console.log('✅ Quality-enabled jobs will populate update.* tables with clean data');
    console.log('✅ Dirty data will go to scrub.* tables for review');
    
    return {
      success: true,
      migrated: {
        lending: lendingResult.rowCount,
        etf: etfResult.rowCount,
        stablecoin: stablecoinResult.rowCount
      }
    };
    
  } catch (error) {
    console.error('❌ Migration failed:', error.message);
    console.error('Stack:', error.stack);
    return { success: false, error: error.message };
  } finally {
    await client.end();
  }
}

// Run if called directly
if (require.main === module) {
  migrateUpdateToClean()
    .then((result) => {
      if (result.success) {
        console.log('\n✅ Migration completed successfully');
        process.exit(0);
      } else {
        console.log('\n❌ Migration failed');
        process.exit(1);
      }
    })
    .catch((error) => {
      console.error('Migration failed:', error.message);
      process.exit(1);
    });
}

module.exports = migrateUpdateToClean;
