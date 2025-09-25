// test_job_local.js - Test the quality-enabled job locally
const { Pool } = require('pg');
require('dotenv').config();

// Import the job function
const jobFunction = require('./api/backfill_token_prices_with_quality');

async function testJobLocally() {
  console.log('🧪 Testing Quality-Enabled Job Locally\n');
  
  // Create a mock request object
  const mockReq = {
    url: 'http://localhost:3000/api/backfill_token_prices_with_quality?offset=0&limit=5',
    headers: {
      host: 'localhost:3000'
    }
  };
  
  // Create a mock response object
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
    console.log('🚀 Running quality-enabled token price job...');
    console.log('📝 Testing with offset=0, limit=5 (first 5 tokens)\n');
    
    await jobFunction(mockReq, mockRes);
    
    console.log('\n✅ Job completed successfully!');
    console.log('\n🔍 Check your Supabase database:');
    console.log('- Clean data should be in: update.token_price_daily');
    console.log('- Dirty data should be in: scrub.token_price_scrub');
    console.log('- Quality metrics in: scrub.data_quality_summary');
    
  } catch (error) {
    console.error('❌ Job failed:', error.message);
    console.error('Stack:', error.stack);
  }
}

// Run the test
testJobLocally();
