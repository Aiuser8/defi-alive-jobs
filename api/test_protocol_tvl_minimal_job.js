// api/test_protocol_tvl_minimal_job.js
// Minimal version of the main job to test core functionality
module.exports.config = { runtime: 'nodejs18.x' };

const { Pool } = require('pg');
const { validateProtocolTvlData } = require('./data_validation');

function makePoolFromEnv() {
  return new Pool({
    connectionString: process.env.SUPABASE_DB_URL,
    ssl: {
      rejectUnauthorized: false,
    },
  });
}

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

module.exports = async (req, res) => {
  const startTime = Date.now();
  let totalRecords = 0;
  let cleanRecords = 0;
  let scrubbedRecords = 0;

  try {
    console.log('🧪 Testing minimal protocol TVL job');

    // Fetch protocol list
    const listResponse = await fetch('https://api.llama.fi/protocols');
    const protocolsList = await listResponse.json();
    
    // Just process the first protocol
    const protocolInfo = protocolsList[1]; // Use Aave (index 1)
    console.log(`🔍 Processing: ${protocolInfo.name}`);

    // Fetch detailed protocol data
    const protocolUrl = `https://api.llama.fi/protocol/${protocolInfo.slug}`;
    const protocolResponse = await fetch(protocolUrl);
    const protocol = await protocolResponse.json();

    const pool = makePoolFromEnv();
    const client = await pool.connect();

    try {
      const today = new Date().toISOString().split('T')[0];

      // Process chains for this protocol
      if (protocol.currentChainTvls) {
        for (const [chain, tvl] of Object.entries(protocol.currentChainTvls)) {
          // Skip borrowed/staking chains
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

          // Validate data
          const validation = validateProtocolTvlData(protocolTvlData);
          
          if (validation.isValid) {
            await insertCleanProtocolTvl(client, protocolTvlData);
            cleanRecords++;
            console.log(`✅ Inserted: ${chain} - $${tvl.toLocaleString()}`);
          } else {
            scrubbedRecords++;
            console.log(`⚠️ Scrubbed: ${chain} - ${validation.errors.join(', ')}`);
          }

          // Only process first 3 chains to keep it fast
          if (totalRecords >= 3) break;
        }
      }

    } finally {
      client.release();
    }

    const processingTime = Date.now() - startTime;
    const successRate = totalRecords > 0 ? (cleanRecords / totalRecords) * 100 : 0;

    console.log(`✅ Minimal job completed: ${cleanRecords}/${totalRecords} records processed`);

    res.status(200).json({
      success: true,
      message: `Processed ${protocolInfo.name} successfully`,
      metrics: {
        totalRecords,
        cleanRecords,
        scrubbedRecords,
        successRate: parseFloat(successRate.toFixed(1)),
        processingTime
      }
    });

  } catch (error) {
    console.error('❌ Minimal job failed:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      metrics: {
        totalRecords,
        cleanRecords,
        scrubbedRecords,
        processingTime: Date.now() - startTime
      }
    });
  }
};
