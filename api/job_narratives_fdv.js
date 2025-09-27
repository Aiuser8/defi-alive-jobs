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
 * Fetches FDV performance data from DeFiLlama Pro API
 */
async function fetchFDVPerformanceData() {
  const { DEFILLAMA_API_KEY } = process.env;
  
  if (!DEFILLAMA_API_KEY) {
    throw new Error('DEFILLAMA_API_KEY environment variable is required');
  }

  // Use 365-day period for maximum historical data
  const apiUrl = `https://pro-api.llama.fi/${DEFILLAMA_API_KEY}/fdv/performance/365`;
  
  console.log(`📡 Fetching FDV performance data from: ${apiUrl}`);
  
  const response = await fetch(apiUrl);
  
  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`API request failed: ${response.status} ${response.statusText} - ${errorText}`);
  }
  
  const data = await response.json();
  
  if (!Array.isArray(data)) {
    throw new Error('Invalid API response format - expected array of FDV performance data');
  }
  
  console.log(`✅ Fetched ${data.length} days of FDV performance data`);
  return data;
}

/**
 * Processes and inserts FDV performance data into database
 */
async function insertFDVPerformanceData(client, performanceData) {
  let insertedCount = 0;
  let updatedCount = 0;
  
  console.log(`📝 Processing ${performanceData.length} FDV performance records...`);
  
  for (const record of performanceData) {
    try {
      // Extract date and normalize all other fields
      const { date, ...performanceFields } = record;
      
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
        INSERT INTO clean.narratives (${columns.join(', ')})
        VALUES (${placeholders.join(', ')})
        ON CONFLICT (date)
        DO UPDATE SET
          ${updateSets.join(', ')},
          inserted_at = NOW()
      `;
      
      const result = await client.query(insertQuery, values);
      
      if (result.rowCount > 0) {
        // Check if this was an insert or update by querying if the record existed
        const existsResult = await client.query(
          'SELECT COUNT(*) FROM clean.narratives WHERE date = $1 AND inserted_at < NOW() - INTERVAL \'1 second\'',
          [date]
        );
        
        if (existsResult.rows[0].count > 0) {
          updatedCount++;
        } else {
          insertedCount++;
        }
      }
      
    } catch (error) {
      console.error(`❌ Error processing record for date ${record.date}:`, error.message);
      throw error;
    }
  }
  
  return { insertedCount, updatedCount };
}

/**
 * Main function to collect and store FDV performance data
 */
module.exports = async function(req, res) {
  const startTime = Date.now();
  let client;
  
  try {
    console.log('🚀 Starting FDV Narratives Collection Job...');
    
    // Create database connection
    const pool = makePoolFromEnv();
    client = await pool.connect();
    
    // Fetch FDV performance data
    const performanceData = await fetchFDVPerformanceData();
    
    if (performanceData.length === 0) {
      console.log('⚠️  No FDV performance data received from API');
      return res.status(200).json({
        success: true,
        message: 'No data to process',
        insertedRecords: 0,
        updatedRecords: 0,
        processingTimeMs: Date.now() - startTime
      });
    }
    
    // Insert data into database
    const { insertedCount, updatedCount } = await insertFDVPerformanceData(client, performanceData);
    
    const duration = Date.now() - startTime;
    
    console.log('🎉 FDV Narratives Collection Complete!');
    console.log(`✅ Inserted: ${insertedCount} records`);
    console.log(`🔄 Updated: ${updatedCount} records`);
    console.log(`⏱️  Processing time: ${duration}ms`);
    
    // Get some statistics about the data range
    const statsResult = await client.query(`
      SELECT 
        MIN(date) as earliest_date,
        MAX(date) as latest_date,
        COUNT(*) as total_records,
        AVG(decentralized_finance) as avg_defi_performance,
        AVG(artificial_intelligence) as avg_ai_performance,
        AVG(meme) as avg_meme_performance
      FROM clean.narratives
    `);
    
    const stats = statsResult.rows[0];
    console.log(`📊 Data range: ${new Date(stats.earliest_date * 1000).toISOString().split('T')[0]} to ${new Date(stats.latest_date * 1000).toISOString().split('T')[0]}`);
    console.log(`📈 Total records in DB: ${stats.total_records}`);
    console.log(`🔮 Avg DeFi performance: ${parseFloat(stats.avg_defi_performance || 0).toFixed(2)}%`);
    console.log(`🤖 Avg AI performance: ${parseFloat(stats.avg_ai_performance || 0).toFixed(2)}%`);
    console.log(`🐸 Avg Meme performance: ${parseFloat(stats.avg_meme_performance || 0).toFixed(2)}%`);
    
    res.status(200).json({
      success: true,
      message: 'FDV narratives data collection completed successfully',
      insertedRecords: insertedCount,
      updatedRecords: updatedCount,
      totalRecordsInDB: parseInt(stats.total_records),
      dataRange: {
        earliest: new Date(stats.earliest_date * 1000).toISOString().split('T')[0],
        latest: new Date(stats.latest_date * 1000).toISOString().split('T')[0]
      },
      averagePerformance: {
        defi: parseFloat(stats.avg_defi_performance || 0).toFixed(2),
        ai: parseFloat(stats.avg_ai_performance || 0).toFixed(2),
        meme: parseFloat(stats.avg_meme_performance || 0).toFixed(2)
      },
      processingTimeMs: duration
    });
    
  } catch (error) {
    console.error('❌ FDV Narratives Collection failed:', error);
    
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
