require('dotenv').config();
const { Pool } = require('pg');

// Create database connection pool
function makePoolFromEnv() {
  return new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
  });
}

/**
 * Maps API field names to database column names
 */
function getColumnName(apiFieldName) {
  const fieldMapping = {
    'Analytics': 'analytics',
    'Artificial Intelligence (AI)': 'artificial_intelligence',
    'Bitcoin': 'bitcoin',
    'Bridge Governance Tokens': 'bridge_governance_tokens',
    'Centralized Exchange (CEX) Token': 'centralized_exchange_token',
    'Data Availability': 'data_availability',
    'Decentralized Finance (DeFi)': 'decentralized_finance',
    'Decentralized Identifier (DID)': 'decentralized_identifier',
    'DePIN': 'depin',
    'Ethereum': 'ethereum',
    'Gaming (GameFi)': 'gaming_gamefi',
    'Liquid Staking Governance Tokens': 'liquid_staking_governance_tokens',
    'Meme': 'meme',
    'NFT Marketplace': 'nft_marketplace',
    'Oracle': 'oracle',
    'PolitiFi': 'politifi',
    'Prediction Markets': 'prediction_markets',
    'Real World Assets (RWA)': 'real_world_assets',
    'Rollup': 'rollup',
    'Smart Contract Platform': 'smart_contract_platform',
    'SocialFi': 'socialfi',
    'Solana': 'solana',
    'null': 'null_category'
  };
  
  return fieldMapping[apiFieldName] || null;
}

/**
 * Fetches latest FDV performance data from DeFiLlama Pro API
 */
async function fetchLatestFDVPerformanceData() {
  const { DEFILLAMA_API_KEY } = process.env;
  
  if (!DEFILLAMA_API_KEY) {
    throw new Error('DEFILLAMA_API_KEY environment variable is required');
  }

  // Use 7-day period to get the most recent data points
  const apiUrl = `https://pro-api.llama.fi/${DEFILLAMA_API_KEY}/fdv/performance/7`;
  
  console.log(`📡 Fetching latest FDV performance data from: ${apiUrl}`);
  
  const response = await fetch(apiUrl);
  
  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`API request failed: ${response.status} ${response.statusText} - ${errorText}`);
  }
  
  const data = await response.json();
  
  if (!Array.isArray(data)) {
    throw new Error('Invalid API response format - expected array of FDV performance data');
  }
  
  if (data.length === 0) {
    throw new Error('No FDV performance data received from API');
  }
  
  // Get the latest record (last in the array)
  const latestRecord = data[data.length - 1];
  console.log(`✅ Fetched latest FDV performance data for date: ${new Date(latestRecord.date * 1000).toISOString().split('T')[0]}`);
  
  return latestRecord;
}

/**
 * Inserts or updates the latest FDV performance data in database
 */
async function upsertLatestFDVPerformanceData(client, performanceRecord) {
  console.log(`📝 Processing latest FDV performance record for date: ${performanceRecord.date}...`);
  
  try {
    // Extract date and normalize all other fields
    const { date, ...performanceFields } = performanceRecord;
    
    // Build the column names and values dynamically
    const columns = ['date'];
    const values = [date];
    const placeholders = ['$1'];
    const updateSets = [];
    
    let paramIndex = 2;
    
    for (const [fieldName, value] of Object.entries(performanceFields)) {
      const columnName = getColumnName(fieldName);
      
      // Skip unmapped fields
      if (!columnName) {
        console.log(`⚠️  Skipping unmapped field: ${fieldName}`);
        continue;
      }
      
      columns.push(columnName);
      values.push(value);
      placeholders.push(`$${paramIndex}`);
      updateSets.push(`${columnName} = EXCLUDED.${columnName}`);
      paramIndex++;
    }
    
    const insertQuery = `
      INSERT INTO update.narratives (${columns.join(', ')})
      VALUES (${placeholders.join(', ')})
      ON CONFLICT (date)
      DO UPDATE SET
        ${updateSets.join(', ')},
        inserted_at = NOW()
    `;
    
    const result = await client.query(insertQuery, values);
    
    if (result.rowCount > 0) {
      console.log(`✅ Successfully upserted FDV performance data for date: ${date}`);
      return { success: true, upserted: true };
    } else {
      console.log(`⚠️  No rows affected for date: ${date}`);
      return { success: true, upserted: false };
    }
    
  } catch (error) {
    console.error(`❌ Error processing record for date ${performanceRecord.date}:`, error.message);
    throw error;
  }
}

/**
 * Main function to collect and store latest FDV performance data
 */
module.exports = async function(req, res) {
  const startTime = Date.now();
  let client;
  
  try {
    console.log('🚀 Starting Daily FDV Narratives Update Job...');
    
    // Create database connection
    const pool = makePoolFromEnv();
    client = await pool.connect();
    
    // Fetch latest FDV performance data
    const latestPerformanceData = await fetchLatestFDVPerformanceData();
    
    // Insert data into database
    const upsertResult = await upsertLatestFDVPerformanceData(client, latestPerformanceData);
    
    const duration = Date.now() - startTime;
    
    console.log('🎉 Daily FDV Narratives Update Complete!');
    console.log(`✅ Record upserted: ${upsertResult.upserted}`);
    console.log(`⏱️  Processing time: ${duration}ms`);
    
    // Get current performance statistics
    const statsResult = await client.query(`
      SELECT 
        to_timestamp(date) as record_date,
        decentralized_finance,
        artificial_intelligence,
        meme,
        real_world_assets,
        gaming_gamefi,
        politifi,
        inserted_at
      FROM update.narratives
      WHERE date = $1
    `, [latestPerformanceData.date]);
    
    const stats = statsResult.rows[0];
    
    if (stats) {
      console.log(`📊 Updated performance for ${stats.record_date.toISOString().split('T')[0]}:`);
      console.log(`🔮 DeFi: ${parseFloat(stats.decentralized_finance || 0).toFixed(2)}%`);
      console.log(`🤖 AI: ${parseFloat(stats.artificial_intelligence || 0).toFixed(2)}%`);
      console.log(`🐸 Meme: ${parseFloat(stats.meme || 0).toFixed(2)}%`);
      console.log(`🏢 RWA: ${parseFloat(stats.real_world_assets || 0).toFixed(2)}%`);
      console.log(`🎮 Gaming: ${parseFloat(stats.gaming_gamefi || 0).toFixed(2)}%`);
      console.log(`🏛️  PolitiFi: ${parseFloat(stats.politifi || 0).toFixed(2)}%`);
    }
    
    res.status(200).json({
      success: true,
      message: 'Daily FDV narratives update completed successfully',
      recordUpserted: upsertResult.upserted,
      recordDate: new Date(latestPerformanceData.date * 1000).toISOString().split('T')[0],
      currentPerformance: stats ? {
        defi: parseFloat(stats.decentralized_finance || 0).toFixed(2),
        ai: parseFloat(stats.artificial_intelligence || 0).toFixed(2),
        meme: parseFloat(stats.meme || 0).toFixed(2),
        rwa: parseFloat(stats.real_world_assets || 0).toFixed(2),
        gaming: parseFloat(stats.gaming_gamefi || 0).toFixed(2),
        politifi: parseFloat(stats.politifi || 0).toFixed(2)
      } : null,
      processingTimeMs: duration
    });
    
  } catch (error) {
    console.error('❌ Daily FDV Narratives Update failed:', error);
    
    res.status(500).json({
      success: false,
      error: error.message,
      processingTimeMs: Date.now() - startTime
    });
    
  } finally {
    if (client) {
      client.release();
    }
  }
};
