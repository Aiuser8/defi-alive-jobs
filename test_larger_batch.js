// test_larger_batch.js - Test with a larger batch to see validation in action
const jobFunction = require('./api/backfill_token_prices_with_quality');

async function testLargerBatch() {
  console.log('🧪 Testing Quality-Enabled Job with Larger Batch\n');
  
  const mockReq = {
    url: 'http://localhost:3000/api/backfill_token_prices_with_quality?offset=0&limit=50',
    headers: {
      host: 'localhost:3000'
    }
  };
  
  const mockRes = {
    status: (code) => ({
      json: (data) => {
        console.log(`📊 Response Status: ${code}`);
        console.log('📋 Response Data:');
        console.log(JSON.stringify(data, null, 2));
        return mockRes;
      }
    })
  };
  
  try {
    console.log('🚀 Running quality-enabled token price job with 50 tokens...\n');
    
    await jobFunction(mockReq, mockRes);
    
    console.log('\n✅ Large batch test completed!');
    console.log('\n🔍 This should show:');
    console.log('- Some clean records (valid token prices)');
    console.log('- Some error records (tokens with no API data)');
    console.log('- Quality metrics showing the split between clean/dirty data');
    
  } catch (error) {
    console.error('❌ Job failed:', error.message);
  }
}

testLargerBatch();
