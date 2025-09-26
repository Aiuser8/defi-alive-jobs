// api/test_protocol_tvl_validation.js
// Test the validation function without database operations
module.exports.config = { runtime: 'nodejs18.x' };

const { validateProtocolTvlData } = require('./data_validation');

module.exports = async (req, res) => {
  try {
    console.log('🧪 Testing protocol TVL validation');

    // Fetch one protocol to test validation
    const listResponse = await fetch('https://api.llama.fi/protocols');
    const protocolsList = await listResponse.json();
    const testProtocol = protocolsList[0];
    
    const protocolUrl = `https://api.llama.fi/protocol/${testProtocol.slug}`;
    const protocolResponse = await fetch(protocolUrl);
    const protocol = await protocolResponse.json();
    
    const today = new Date().toISOString().split('T')[0];
    const results = [];
    
    // Test validation for each chain
    if (protocol.currentChainTvls) {
      for (const [chain, tvl] of Object.entries(protocol.currentChainTvls)) {
        // Skip borrowed/staking chains
        if (chain.includes('-borrowed') || chain.includes('-staking') || 
            chain.includes('-pool2') || chain === 'borrowed' || 
            chain === 'staking' || chain === 'pool2') {
          continue;
        }

        const protocolTvlData = {
          protocol_id: protocol.id,
          protocol_name: protocol.name,
          chain: chain,
          series_type: 'total',
          ts: today,
          total_liquidity_usd: tvl,
          category: protocol.category,
          symbol: protocol.symbol,
          url: protocol.url
        };

        // Test validation
        const validation = validateProtocolTvlData(protocolTvlData);
        
        results.push({
          chain,
          tvl,
          isValid: validation.isValid,
          qualityScore: validation.qualityScore,
          errors: validation.errors
        });
      }
    }

    res.status(200).json({
      success: true,
      message: 'Protocol TVL validation test completed',
      protocol: testProtocol.name,
      validationResults: results.slice(0, 5), // Show first 5 chains
      summary: {
        totalChains: results.length,
        validChains: results.filter(r => r.isValid).length,
        invalidChains: results.filter(r => !r.isValid).length
      }
    });

  } catch (error) {
    console.error('❌ Validation test failed:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      stack: error.stack
    });
  }
};
