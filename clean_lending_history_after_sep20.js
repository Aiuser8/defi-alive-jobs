const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  connectionString: process.env.SUPABASE_DB_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

async function cleanLendingHistoryAfterSep20() {
  const client = await pool.connect();
  try {
    console.log('🔍 Checking data quality in clean.lending_market_history...');
    
    // First, let's see what we have
    const beforeQuery = `
      SELECT 
        DATE(to_timestamp(ts)) as data_date,
        COUNT(*) as total_records,
        COUNT(DISTINCT symbol) as unique_symbols,
        COUNT(*) FILTER (WHERE symbol IS NOT NULL AND symbol != '') as records_with_symbol,
        ROUND(COUNT(*) FILTER (WHERE symbol IS NOT NULL AND symbol != '') * 100.0 / COUNT(*), 1) as symbol_coverage_pct
      FROM clean.lending_market_history
      WHERE to_timestamp(ts) >= '2025-09-15'
      GROUP BY DATE(to_timestamp(ts))
      ORDER BY data_date DESC;
    `;
    
    const beforeResult = await client.query(beforeQuery);
    console.log('\n📊 Current data quality by date:');
    console.table(beforeResult.rows);
    
    // Count records to be removed
    const countQuery = `
      SELECT COUNT(*) as records_to_remove
      FROM clean.lending_market_history
      WHERE to_timestamp(ts) > '2025-09-20 23:59:59';
    `;
    
    const countResult = await client.query(countQuery);
    const recordsToRemove = parseInt(countResult.rows[0].records_to_remove, 10);
    
    console.log(`\n🗑️ Records to remove (after Sep 20, 2025): ${recordsToRemove.toLocaleString()}`);
    
    if (recordsToRemove === 0) {
      console.log('✅ No records need to be removed!');
      return;
    }
    
    console.log('🔄 Removing degraded quality records after Sep 20, 2025...');
    
    // Remove records after Sep 20, 2025
    const deleteQuery = `
      DELETE FROM clean.lending_market_history
      WHERE to_timestamp(ts) > '2025-09-20 23:59:59';
    `;
    
    const deleteResult = await client.query(deleteQuery);
    console.log(`✅ Successfully removed ${deleteResult.rowCount.toLocaleString()} records`);
    
    // Verify the cleanup
    const afterQuery = `
      SELECT 
        DATE(to_timestamp(ts)) as data_date,
        COUNT(*) as total_records,
        COUNT(DISTINCT symbol) as unique_symbols,
        COUNT(*) FILTER (WHERE symbol IS NOT NULL AND symbol != '') as records_with_symbol,
        ROUND(COUNT(*) FILTER (WHERE symbol IS NOT NULL AND symbol != '') * 100.0 / COUNT(*), 1) as symbol_coverage_pct,
        MIN(to_timestamp(ts)) as earliest_time,
        MAX(to_timestamp(ts)) as latest_time
      FROM clean.lending_market_history
      WHERE to_timestamp(ts) >= '2025-09-15'
      GROUP BY DATE(to_timestamp(ts))
      ORDER BY data_date DESC;
    `;
    
    const afterResult = await client.query(afterQuery);
    console.log('\n📊 Data quality after cleanup:');
    console.table(afterResult.rows);
    
    // Final summary
    const summaryQuery = `
      SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT market_id) as unique_markets,
        COUNT(DISTINCT symbol) as unique_symbols,
        MIN(to_timestamp(ts)) as earliest_date,
        MAX(to_timestamp(ts)) as latest_date,
        COUNT(*) FILTER (WHERE symbol IS NOT NULL AND symbol != '') as records_with_symbol,
        ROUND(COUNT(*) FILTER (WHERE symbol IS NOT NULL AND symbol != '') * 100.0 / COUNT(*), 1) as symbol_coverage_pct
      FROM clean.lending_market_history;
    `;
    
    const summaryResult = await client.query(summaryQuery);
    const summary = summaryResult.rows[0];
    
    console.log('\n🎯 Final clean.lending_market_history summary:');
    console.log(`📊 Total records: ${parseInt(summary.total_records).toLocaleString()}`);
    console.log(`🏛️ Unique markets: ${parseInt(summary.unique_markets).toLocaleString()}`);
    console.log(`🏷️ Unique symbols: ${parseInt(summary.unique_symbols).toLocaleString()}`);
    console.log(`📅 Date range: ${summary.earliest_date.toISOString().split('T')[0]} to ${summary.latest_date.toISOString().split('T')[0]}`);
    console.log(`✅ Symbol coverage: ${summary.symbol_coverage_pct}%`);
    console.log(`📈 Records with symbols: ${parseInt(summary.records_with_symbol).toLocaleString()}`);
    
  } catch (error) {
    console.error('❌ Cleanup failed:', error.message);
    throw error;
  } finally {
    client.release();
  }
}

if (require.main === module) {
  cleanLendingHistoryAfterSep20()
    .then(() => {
      console.log('\n🎉 Clean lending history cleanup complete!');
      console.log('✅ Only high-quality data with symbol coverage remains');
      console.log('📊 Perfect historical dataset for analysis');
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Cleanup failed:', error.message);
      process.exit(1);
    });
}

module.exports = { cleanLendingHistoryAfterSep20 };
