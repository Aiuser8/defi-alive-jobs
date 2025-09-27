// api/dispatcher_daily_jobs_direct.js
// Direct daily jobs dispatcher - no validation, fast data collection

module.exports.config = { runtime: 'nodejs18.x' };

// Import direct jobs
const protocolTvlJob = require('./job_protocol_tvl_direct.js');
const protocolFeesJob = require('./job_protocol_fees_direct.js');

module.exports = async function(req, res) {
  const startTime = Date.now();
  
  try {
    console.log('🚀 Starting Direct Daily Jobs Dispatcher...');
    
    const jobResults = {};
    
    // Execute Protocol TVL Job
    try {
      console.log('📊 Starting Protocol TVL Collection...');
      
      let tvlResult = null;
      const tvlRes = {
        status: (code) => ({
          json: (data) => {
            tvlResult = { statusCode: code, data };
          }
        })
      };
      
      await protocolTvlJob({}, tvlRes);
      
      if (tvlResult && tvlResult.statusCode === 200) {
        console.log('✅ Protocol TVL completed: SUCCESS');
        jobResults.protocolTvl = { success: true, ...tvlResult.data };
      } else {
        throw new Error(`TVL job failed with status: ${tvlResult?.statusCode || 'unknown'}`);
      }
      
    } catch (error) {
      console.error('❌ Protocol TVL failed:', error.message);
      jobResults.protocolTvl = { success: false, error: error.message };
    }
    
    // Execute Protocol Fees Job
    try {
      console.log('💸 Starting Protocol Fees Collection...');
      
      let feesResult = null;
      const feesRes = {
        status: (code) => ({
          json: (data) => {
            feesResult = { statusCode: code, data };
          }
        })
      };
      
      await protocolFeesJob({}, feesRes);
      
      if (feesResult && feesResult.statusCode === 200) {
        console.log('✅ Protocol Fees completed: SUCCESS');
        jobResults.protocolFees = { success: true, ...feesResult.data };
      } else {
        throw new Error(`Fees job failed with status: ${feesResult?.statusCode || 'unknown'}`);
      }
      
    } catch (error) {
      console.error('❌ Protocol Fees failed:', error.message);
      jobResults.protocolFees = { success: false, error: error.message };
    }
    
    const duration = Date.now() - startTime;
    
    // Calculate summary
    const successfulJobs = Object.values(jobResults).filter(job => job.success).length;
    const totalJobs = Object.keys(jobResults).length;
    
    console.log('🎉 Direct Daily Jobs Dispatcher Complete!');
    console.log(`✅ Successful jobs: ${successfulJobs}/${totalJobs}`);
    console.log(`⏱️  Total time: ${(duration / 1000).toFixed(1)}s`);
    
    // Log individual job results
    Object.entries(jobResults).forEach(([jobName, result]) => {
      if (result.success) {
        console.log(`📊 ${jobName}: ${result.insertedRecords || 0} records inserted`);
      } else {
        console.log(`❌ ${jobName}: ${result.error}`);
      }
    });
    
    res.status(200).json({
      success: true,
      message: 'Direct daily jobs collection completed',
      totalJobs,
      successfulJobs,
      jobResults,
      processingTimeMs: duration
    });
    
  } catch (error) {
    console.error('❌ Direct Daily Jobs Dispatcher failed:', error);
    
    res.status(500).json({
      success: false,
      error: error.message,
      processingTimeMs: Date.now() - startTime
    });
  }
};
