// api/job_protocol_tvl_direct.js
// Direct protocol TVL data collection - no validation, direct data landing
// Fast data collection for later cleaning/normalization

module.exports.config = { runtime: 'nodejs18.x' };

require('dotenv').config();
const { Pool } = require('pg');

function makePoolFromEnv() {
  return new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
  });
}

/**
 * Fetch protocol data from DeFiLlama API
 */
async function fetchProtocolData() {
  const url = 'https://api.llama.fi/protocols';
  
  console.log(`📡 Fetching protocol data from: ${url}`);
  
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`API request failed: ${response.status} ${response.statusText}`);
  }
  
  const data = await response.json();
  if (!Array.isArray(data)) {
    throw new Error('Invalid API response format - expected array');
  }
  
  console.log(`✅ Fetched ${data.length} protocols`);
  return data;
}

/**
 * Insert protocol TVL data directly into update table
 */
async function insertProtocolData(client, protocolData) {
  let insertedCount = 0;
  let errorCount = 0;
  
  console.log(`📝 Inserting ${protocolData.length} protocol records...`);
  
  for (const protocol of protocolData) {
    try {
      // Insert main protocol record
      const insertQuery = `
        INSERT INTO update.protocol_chain_tvl_daily (
          protocol_id, protocol_name, chain, series_type, ts, 
          total_liquidity_usd, category, symbol, url
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      `;
      
      const values = [
        protocol.id || protocol.slug || null,     // protocol_id
        protocol.name || null,                    // protocol_name
        'multi-chain',                            // chain (default for main record)
        'total',                                  // series_type
        new Date().toISOString().split('T')[0],   // ts (date)
        protocol.tvl || null,                     // total_liquidity_usd
        protocol.category || null,                // category
        protocol.symbol || null,                  // symbol
        protocol.url || null                      // url
      ];
      
      await client.query(insertQuery, values);
      insertedCount++;
      
      // Insert chain-specific records if available
      if (protocol.chainTvls && typeof protocol.chainTvls === 'object') {
        for (const [chain, tvl] of Object.entries(protocol.chainTvls)) {
          if (chain !== 'tvl' && typeof tvl === 'number' && tvl > 0) {
            try {
              const chainValues = [
                protocol.id || protocol.slug || null,
                protocol.name || null,
                chain,
                'chain-specific',
                new Date().toISOString().split('T')[0],
                tvl,
                protocol.category || null,
                protocol.symbol || null,
                protocol.url || null
              ];
              
              await client.query(insertQuery, chainValues);
              insertedCount++;
            } catch (chainError) {
              console.error(`❌ Error inserting chain record ${protocol.id}-${chain}:`, chainError.message);
              errorCount++;
            }
          }
        }
      }
      
    } catch (error) {
      console.error(`❌ Error inserting protocol record ${protocol.id}:`, error.message);
      errorCount++;
    }
  }
  
  return { insertedCount, errorCount };
}

/**
 * Main function
 */
module.exports = async function(req, res) {
  const startTime = Date.now();
  let client;
  
  try {
    console.log('🚀 Starting Direct Protocol TVL Collection...');
    
    // Create database connection
    const pool = makePoolFromEnv();
    client = await pool.connect();
    
    // Fetch protocol data
    const protocolData = await fetchProtocolData();
    
    // Insert data directly
    const { insertedCount, errorCount } = await insertProtocolData(client, protocolData);
    
    const duration = Date.now() - startTime;
    
    console.log('🎉 Direct Protocol TVL Collection Complete!');
    console.log(`✅ Inserted: ${insertedCount} records`);
    console.log(`❌ Errors: ${errorCount} records`);
    console.log(`⏱️  Processing time: ${duration}ms`);
    
    res.status(200).json({
      success: true,
      message: 'Direct protocol TVL collection completed',
      insertedRecords: insertedCount,
      errorRecords: errorCount,
      totalProcessed: protocolData.length,
      processingTimeMs: duration
    });
    
  } catch (error) {
    console.error('❌ Direct Protocol TVL Collection failed:', error);
    
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
