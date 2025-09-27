// api/dispatcher_lending_direct.js
// Direct lending market dispatcher - no validation, fast data collection

module.exports.config = { runtime: 'nodejs18.x' };

// Import the direct lending job
const lendingJob = require('./job_lending_direct.js');

module.exports = async function(req, res) {
  const startTime = Date.now();
  
  try {
    console.log('🚀 Starting Direct Lending Market Dispatcher...');
    
    // Create mock request/response for the job
    const jobReq = {};
    let jobResult = null;
    
    const jobRes = {
      status: (code) => ({
        json: (data) => {
          jobResult = { statusCode: code, data };
        }
      })
    };
    
    // Execute the lending job directly
    await lendingJob(jobReq, jobRes);
    
    const duration = Date.now() - startTime;
    
    if (jobResult && jobResult.statusCode === 200) {
      console.log('🎉 Direct Lending Dispatcher Complete!');
      console.log(`✅ Job Status: SUCCESS`);
      console.log(`📊 Records Inserted: ${jobResult.data.insertedRecords}`);
      console.log(`❌ Records with Errors: ${jobResult.data.errorRecords}`);
      console.log(`⏱️  Total Time: ${duration}ms`);
      
      res.status(200).json({
        success: true,
        message: 'Direct lending market collection completed successfully',
        jobResults: jobResult.data,
        totalTimeMs: duration
      });
    } else {
      throw new Error(`Lending job failed with status: ${jobResult?.statusCode || 'unknown'}`);
    }
    
  } catch (error) {
    console.error('❌ Direct Lending Dispatcher failed:', error);
    
    res.status(500).json({
      success: false,
      error: error.message,
      totalTimeMs: Date.now() - startTime
    });
  }
};
