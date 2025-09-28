// api/job_protocol_fees_direct.js
// Direct protocol fees data collection - no validation, direct data landing
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
 * Fetch protocol fees data from DeFiLlama Pro API
 */
async function fetchProtocolFeesData() {
  const { DEFILLAMA_API_KEY } = process.env;
  
  console.log(`🔑 API Key loaded: ${DEFILLAMA_API_KEY ? 'YES' : 'NO'} (length: ${DEFILLAMA_API_KEY?.length || 0})`);
  
  if (!DEFILLAMA_API_KEY) {
    throw new Error('DEFILLAMA_API_KEY environment variable is required');
  }

  const url = `https://pro-api.llama.fi/${DEFILLAMA_API_KEY}/overview/fees`;
  
  console.log(`📡 Fetching protocol fees data from DeFiLlama Pro API`);
  
  const response = await fetch(url);
  if (!response.ok) {
    const errorText = await response.text().catch(() => '');
    throw new Error(`API request failed: ${response.status} ${response.statusText} - ${errorText}`);
  }
  
  const data = await response.json();
  if (!data.protocols || !Array.isArray(data.protocols)) {
    throw new Error('Invalid API response format - expected protocols array');
  }
  
  console.log(`✅ Fetched ${data.protocols.length} protocol fee records`);
  return data.protocols;
}

/**
 * Insert protocol fees data directly into update table
 */
async function insertProtocolFeesData(client, protocolsData) {
  let insertedCount = 0;
  let errorCount = 0;
  
  console.log(`📝 Inserting ${protocolsData.length} protocol fee records...`);
  
  for (const protocol of protocolsData) {
    try {
      const insertQuery = `
        INSERT INTO update.protocol_fees_daily (
          protocol_id, defillama_id, name, display_name, slug, category, chains,
          module, protocol_type, logo, total_24h, total_48h_to_24h, total_7d,
          total_14d_to_7d, total_30d, total_60d_to_30d, total_1y, total_all_time,
          average_1y, monthly_average_1y, change_1d, change_7d, change_1m,
          change_7d_over_7d, change_30d_over_30d, total_7_days_ago, total_30_days_ago,
          breakdown_24h, breakdown_30d, methodology, methodology_url, collection_date
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
          $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32
        )
      `;
      
      const values = [
        protocol.defillamaId || protocol.name || null,           // protocol_id
        protocol.defillamaId || null,                           // defillama_id
        protocol.name || null,                                  // name
        protocol.displayName || protocol.name || null,         // display_name
        protocol.slug || null,                                  // slug
        protocol.category || null,                              // category
        protocol.chains || null,                                // chains
        protocol.module || null,                                // module
        protocol.protocolType || null,                          // protocol_type
        protocol.logo || null,                                  // logo
        protocol.total24h || null,                              // total_24h
        protocol.total48hto24h || null,                         // total_48h_to_24h
        protocol.total7d || null,                               // total_7d
        protocol.total14dto7d || null,                          // total_14d_to_7d
        protocol.total30d || null,                              // total_30d
        protocol.total60dto30d || null,                         // total_60d_to_30d
        protocol.total1y || null,                               // total_1y
        protocol.totalAllTime || null,                          // total_all_time
        protocol.average1y || null,                             // average_1y
        protocol.monthlyAverage1y || null,                      // monthly_average_1y
        protocol.change_1d || null,                             // change_1d
        protocol.change_7d || null,                             // change_7d
        protocol.change_1m || null,                             // change_1m
        protocol.change_7dover7d || null,                       // change_7d_over_7d
        protocol.change_30dover30d || null,                     // change_30d_over_30d
        protocol.total7DaysAgo || null,                         // total_7_days_ago
        protocol.total30DaysAgo || null,                        // total_30_days_ago
        protocol.breakdown24h ? JSON.stringify(protocol.breakdown24h) : null,  // breakdown_24h
        protocol.breakdown30d ? JSON.stringify(protocol.breakdown30d) : null,  // breakdown_30d
        protocol.methodology ? JSON.stringify(protocol.methodology) : null,    // methodology
        protocol.methodologyURL || null,                        // methodology_url
        new Date().toISOString().split('T')[0]                  // collection_date
      ];
      
      await client.query(insertQuery, values);
      insertedCount++;
      
    } catch (error) {
      console.error(`❌ Error inserting protocol fee record ${protocol.name}:`, error.message);
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
    console.log('🚀 Starting Direct Protocol Fees Collection...');
    
    // Create database connection
    const pool = makePoolFromEnv();
    client = await pool.connect();
    
    // Fetch protocol fees data
    const protocolsData = await fetchProtocolFeesData();
    
    // Insert data directly
    const { insertedCount, errorCount } = await insertProtocolFeesData(client, protocolsData);
    
    const duration = Date.now() - startTime;
    
    console.log('🎉 Direct Protocol Fees Collection Complete!');
    console.log(`✅ Inserted: ${insertedCount} records`);
    console.log(`❌ Errors: ${errorCount} records`);
    console.log(`⏱️  Processing time: ${duration}ms`);
    
    res.status(200).json({
      success: true,
      message: 'Direct protocol fees collection completed',
      insertedRecords: insertedCount,
      errorRecords: errorCount,
      totalProcessed: protocolsData.length,
      processingTimeMs: duration
    });
    
  } catch (error) {
    console.error('❌ Direct Protocol Fees Collection failed:', error);
    
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
