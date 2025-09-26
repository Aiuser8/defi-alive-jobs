// api/job_protocol_tvl_with_quality.js (CommonJS)
// Live protocol TVL collection with data quality gates and scrub table routing
// Force Node runtime (pg not supported on Edge)
module.exports.config = { runtime: 'nodejs18.x' };

const { Pool } = require('pg');
const { 
  validateProtocolTvlData, 
  insertIntoScrubTable, 
  updateQualitySummary, 
  generateJobRunId 
} = require('./data_validation');

const pool = new Pool({
  connectionString: process.env.SUPABASE_DB_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

function makePoolFromEnv() {
  return new Pool({
    connectionString: process.env.SUPABASE_DB_URL,
    ssl: {
      rejectUnauthorized: false,
    },
  });
}

module.exports = async (req, res) => {
  const jobRunId = generateJobRunId();
  const startTime = Date.now();
  
  // Parse query parameters for batching
  const { offset = 0, limit = 500 } = req.query || {};
  const offsetNum = parseInt(offset, 10);
  const limitNum = parseInt(limit, 10);
  
  // Metrics tracking
  let totalRecords = 0;
  let cleanRecords = 0;
  let scrubbedRecords = 0;
  let errors = [];
  let processingTime = 0;
  let successRate = 0;

  try {
    console.log(`📊 Starting protocol TVL collection job: ${jobRunId}`);
    console.log(`📦 Batch: offset=${offsetNum}, limit=${limitNum}`);

    // First, fetch the list of all protocols
    const protocolsListUrl = 'https://api.llama.fi/protocols';
    console.log(`📡 Fetching protocol list from: ${protocolsListUrl}`);
    
    const listResponse = await fetch(protocolsListUrl);
    if (!listResponse.ok) {
      throw new Error(`Protocols list request failed with status ${listResponse.status}: ${listResponse.statusText}`);
    }

    const protocolsList = await listResponse.json();
    if (!Array.isArray(protocolsList)) {
      throw new Error('Protocols list response is not an array');
    }

    // Apply batching to the protocol list
    const allProtocols = protocolsList;
    const protocolsBatch = allProtocols.slice(offsetNum, offsetNum + limitNum);
    
    console.log(`📊 Processing batch: ${protocolsBatch.length} protocols (${offsetNum}-${offsetNum + limitNum - 1} of ${allProtocols.length} total)`);

    const pool = makePoolFromEnv();
    const client = await pool.connect();

    try {
      const today = new Date().toISOString().split('T')[0]; // YYYY-MM-DD format

      for (const protocolInfo of protocolsBatch) {
        // Fetch detailed protocol data with TVL breakdown
        try {
          const protocolUrl = `https://api.llama.fi/protocol/${protocolInfo.slug || protocolInfo.id}`;
          console.log(`🔍 Fetching details for: ${protocolInfo.name} (${protocolUrl})`);
          
          const protocolResponse = await fetch(protocolUrl);
          if (!protocolResponse.ok) {
            console.warn(`⚠️ Failed to fetch ${protocolInfo.name}: ${protocolResponse.status}`);
            continue;
          }

          const protocol = await protocolResponse.json();
          
          // Process each chain's TVL for this protocol
          if (protocol.currentChainTvls && typeof protocol.currentChainTvls === 'object') {
          for (const [chain, tvl] of Object.entries(protocol.currentChainTvls)) {
            // Skip borrowed, staking, pool2 etc - focus on main TVL
            if (chain.includes('-borrowed') || chain.includes('-staking') || 
                chain.includes('-pool2') || chain === 'borrowed' || 
                chain === 'staking' || chain === 'pool2') {
              continue;
            }

            const protocolTvlData = {
              protocol_id: protocol.id || protocol.slug,
              protocol_name: protocol.name,
              chain: chain,
              series_type: 'total',
              ts: today,
              total_liquidity_usd: tvl,
              category: protocol.category,
              symbol: protocol.symbol,
              url: protocol.url
            };

            totalRecords++;

            // Validate data quality
            const validation = validateProtocolTvlData(protocolTvlData);
            
            if (validation.isValid) {
              // Insert clean data into update table
              try {
                await insertCleanProtocolTvl(client, protocolTvlData);
                cleanRecords++;
              } catch (error) {
                console.error(`❌ Error inserting clean protocol TVL data:`, error.message);
                errors.push(`clean_insert_error: ${error.message}`);
                
                // If clean insert fails, send to scrub with additional error context
                validation.errors.push(`clean_insert_failed: ${error.message}`);
                validation.isValid = false;
              }
            }
            
            if (!validation.isValid) {
              // Route to scrub table with quality metadata
              try {
                await insertIntoScrubTable(
                  client,
                  'protocol_tvl_scrub',
                  protocolTvlData,
                  validation,
                  jobRunId,
                  protocolTvlData
                );
                scrubbedRecords++;
              } catch (scrubError) {
                console.error(`❌ Failed to insert into protocol_tvl_scrub:`, {
                  error: scrubError.message,
                  data: protocolTvlData,
                  validation: validation
                });
                errors.push(`scrub_insert_error: ${scrubError.message}`);
              }
            }
          }
        } catch (protocolError) {
          console.error(`❌ Error processing protocol ${protocolInfo.name}:`, protocolError.message);
          errors.push(`protocol_fetch_error: ${protocolInfo.name} - ${protocolError.message}`);
        }
      }

      // Calculate final metrics
      successRate = totalRecords > 0 ? (cleanRecords / totalRecords) * 100 : 0;
      processingTime = Date.now() - startTime;
      
      // Skip quality summary for now to avoid timeout issues
      console.log('📊 Skipping quality summary to avoid timeouts');

    } finally {
      client.release();
    }
    
    console.log(`✅ Protocol TVL collection completed successfully`);
    console.log(`📊 Results: ${totalRecords} total, ${cleanRecords} clean, ${scrubbedRecords} scrubbed`);
    console.log(`⏱️  Processing time: ${processingTime}ms`);
    console.log(`🎯 Success rate: ${successRate.toFixed(1)}%`);

    res.status(200).json({
      success: true,
      jobRunId,
      metrics: {
        totalRecords,
        cleanRecords,
        scrubbedRecords,
        successRate: parseFloat(successRate.toFixed(1)),
        avgQualityScore,
        processingTime,
        protocolsProcessed: protocolsBatch.length,
        batchInfo: {
          offset: offsetNum,
          limit: limitNum,
          totalProtocols: allProtocols.length
        }
      },
      message: `Processed ${protocolsBatch.length} protocols with ${successRate.toFixed(1)}% success rate`
    });

  } catch (error) {
    processingTime = Date.now() - startTime;
    console.error(`❌ Protocol TVL collection job failed:`, error.message);
    
    // Skip quality summary in error case to avoid further issues
    console.log('📊 Skipping error quality summary to avoid additional timeouts');

    res.status(500).json({
      success: false,
      jobRunId,
      error: error.message,
      metrics: {
        totalRecords,
        cleanRecords,
        scrubbedRecords,
        processingTime
      }
    });
  }
};

async function insertCleanProtocolTvl(client, protocolTvlData) {
  await client.query(`
    INSERT INTO update.protocol_chain_tvl_daily (
      protocol_id, protocol_name, chain, series_type, ts, total_liquidity_usd, 
      category, symbol, url, inserted_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    ON CONFLICT (protocol_id, chain, series_type, ts) DO UPDATE SET
      protocol_name = EXCLUDED.protocol_name,
      total_liquidity_usd = EXCLUDED.total_liquidity_usd,
      category = EXCLUDED.category,
      symbol = EXCLUDED.symbol,
      url = EXCLUDED.url,
      inserted_at = EXCLUDED.inserted_at
  `, [
    protocolTvlData.protocol_id,
    protocolTvlData.protocol_name,
    protocolTvlData.chain,
    protocolTvlData.series_type,
    protocolTvlData.ts,
    protocolTvlData.total_liquidity_usd,
    protocolTvlData.category,
    protocolTvlData.symbol,
    protocolTvlData.url,
    new Date().toISOString()
  ]);
}
