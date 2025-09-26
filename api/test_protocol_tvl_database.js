// api/test_protocol_tvl_database.js
// Test database operations for protocol TVL
module.exports.config = { runtime: 'nodejs18.x' };

const { Pool } = require('pg');

function makePoolFromEnv() {
  return new Pool({
    connectionString: process.env.SUPABASE_DB_URL,
    ssl: {
      rejectUnauthorized: false,
    },
  });
}

module.exports = async (req, res) => {
  try {
    console.log('🧪 Testing protocol TVL database operations');

    // Get test data
    const listResponse = await fetch('https://api.llama.fi/protocols');
    const protocolsList = await listResponse.json();
    const testProtocol = protocolsList[1]; // Try Aave instead of Binance
    
    const protocolUrl = `https://api.llama.fi/protocol/${testProtocol.slug}`;
    const protocolResponse = await fetch(protocolUrl);
    const protocol = await protocolResponse.json();
    
    const today = new Date().toISOString().split('T')[0];
    
    // Test database connection
    const pool = makePoolFromEnv();
    const client = await pool.connect();
    
    console.log('✅ Database connected');
    
    try {
      // Find the first valid chain with TVL
      let testChain = null;
      let testTvl = null;
      
      if (protocol.currentChainTvls) {
        for (const [chain, tvl] of Object.entries(protocol.currentChainTvls)) {
          if (!chain.includes('-borrowed') && !chain.includes('-staking') && 
              !chain.includes('-pool2') && chain !== 'borrowed' && 
              chain !== 'staking' && chain !== 'pool2' && tvl > 0) {
            testChain = chain;
            testTvl = tvl;
            break;
          }
        }
      }
      
      if (!testChain) {
        throw new Error('No valid chain found for testing');
      }
      
      console.log(`🔍 Testing with: ${protocol.name} on ${testChain} with TVL: $${testTvl.toLocaleString()}`);
      
      // Prepare the data exactly as our job would
      const protocolTvlData = {
        protocol_id: protocol.id || protocol.slug,
        protocol_name: protocol.name,
        chain: testChain,
        series_type: 'total', 
        ts: today,
        total_liquidity_usd: testTvl,
        category: protocol.category || null,
        symbol: protocol.symbol || null,
        url: protocol.url || null
      };
      
      console.log('📊 Data to insert:', {
        protocol_id: protocolTvlData.protocol_id,
        protocol_name: protocolTvlData.protocol_name,
        chain: protocolTvlData.chain,
        ts: protocolTvlData.ts,
        total_liquidity_usd: protocolTvlData.total_liquidity_usd
      });
      
      // Try the insert
      const result = await client.query(`
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
        RETURNING id
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
      
      console.log('✅ Insert successful, ID:', result.rows[0].id);
      
      // Verify the data was inserted
      const verify = await client.query(`
        SELECT * FROM update.protocol_chain_tvl_daily WHERE id = $1
      `, [result.rows[0].id]);
      
      res.status(200).json({
        success: true,
        message: 'Database test completed successfully',
        testData: protocolTvlData,
        insertedId: result.rows[0].id,
        verifiedData: verify.rows[0]
      });
      
    } finally {
      client.release();
    }

  } catch (error) {
    console.error('❌ Database test failed:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      stack: error.stack
    });
  }
};
