// api/test_protocol_tvl_simple.js
// Minimal test version to debug the protocol TVL job
module.exports.config = { runtime: 'nodejs18.x' };

module.exports = async (req, res) => {
  try {
    console.log('🧪 Testing protocol TVL job - minimal version');

    // Test 1: Can we fetch the protocols list?
    const listResponse = await fetch('https://api.llama.fi/protocols');
    if (!listResponse.ok) {
      throw new Error(`Protocols list failed: ${listResponse.status}`);
    }
    const protocolsList = await listResponse.json();
    console.log(`✅ Fetched ${protocolsList.length} protocols`);

    // Test 2: Can we fetch one specific protocol?
    const testProtocol = protocolsList[0];
    console.log(`🔍 Testing with: ${testProtocol.name} (${testProtocol.slug})`);
    
    const protocolUrl = `https://api.llama.fi/protocol/${testProtocol.slug}`;
    const protocolResponse = await fetch(protocolUrl);
    if (!protocolResponse.ok) {
      throw new Error(`Protocol fetch failed: ${protocolResponse.status}`);
    }
    const protocol = await protocolResponse.json();
    console.log(`✅ Fetched protocol details`);

    // Test 3: Check data structure
    const hasChainTvls = protocol.currentChainTvls && typeof protocol.currentChainTvls === 'object';
    console.log(`📊 Has currentChainTvls: ${hasChainTvls}`);
    
    if (hasChainTvls) {
      const chains = Object.keys(protocol.currentChainTvls);
      console.log(`🔗 Chains: ${chains.slice(0, 3).join(', ')}...`);
    }

    res.status(200).json({
      success: true,
      message: 'Protocol TVL test completed successfully',
      data: {
        protocolsCount: protocolsList.length,
        testProtocol: testProtocol.name,
        hasChainTvls,
        sampleChains: hasChainTvls ? Object.keys(protocol.currentChainTvls).slice(0, 3) : []
      }
    });

  } catch (error) {
    console.error('❌ Test failed:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      stack: error.stack
    });
  }
};
