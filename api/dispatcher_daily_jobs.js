// api/dispatcher_daily_jobs.js
// Daily Jobs Dispatcher
// Runs ETF and Stablecoin jobs in parallel once per day

module.exports.config = { runtime: 'nodejs18.x' };

// Import the actual job functions
const etfJob = require('./job_etf.js');
const stablecoinJob = require('./job_stablecoins.js');
const protocolTvlJob = require('./job_protocol_tvl_with_quality.js');

function generateJobRunId() {
  return `dispatcher_daily_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

module.exports = async (req, res) => {
  const startTime = Date.now();
  const jobRunId = generateJobRunId();
  
  console.log(`📅 Starting Daily Jobs Dispatcher: ${jobRunId}`);
  console.log(`🎯 Jobs: ETF flows + Stablecoin market cap + Protocol TVL data (13 batches covering 6,467+ protocols)`);

  try {
    // Define the jobs to run in parallel
    const dailyJobs = [
      {
        name: 'ETF Flows',
        job: etfJob,
        schedule: 'Daily at 10:05 AM UTC'
      },
      {
        name: 'Stablecoin Market Cap',
        job: stablecoinJob,
        schedule: 'Daily at 10:55 AM UTC'
      },
      // Protocol TVL jobs - multiple batches to cover all 6,467+ protocols
      {
        name: 'Protocol TVL Batch 1',
        job: protocolTvlJob,
        schedule: 'Daily at 10:05 AM UTC',
        params: { offset: '0', limit: '500' }
      },
      {
        name: 'Protocol TVL Batch 2', 
        job: protocolTvlJob,
        schedule: 'Daily at 10:05 AM UTC',
        params: { offset: '500', limit: '500' }
      },
      {
        name: 'Protocol TVL Batch 3',
        job: protocolTvlJob,
        schedule: 'Daily at 10:05 AM UTC', 
        params: { offset: '1000', limit: '500' }
      },
      {
        name: 'Protocol TVL Batch 4',
        job: protocolTvlJob,
        schedule: 'Daily at 10:05 AM UTC',
        params: { offset: '1500', limit: '500' }
      },
      {
        name: 'Protocol TVL Batch 5',
        job: protocolTvlJob,
        schedule: 'Daily at 10:05 AM UTC',
        params: { offset: '2000', limit: '500' }
      },
      {
        name: 'Protocol TVL Batch 6',
        job: protocolTvlJob,
        schedule: 'Daily at 10:05 AM UTC',
        params: { offset: '2500', limit: '500' }
      },
      {
        name: 'Protocol TVL Batch 7',
        job: protocolTvlJob,
        schedule: 'Daily at 10:05 AM UTC',
        params: { offset: '3000', limit: '500' }
      },
      {
        name: 'Protocol TVL Batch 8',
        job: protocolTvlJob,
        schedule: 'Daily at 10:05 AM UTC',
        params: { offset: '3500', limit: '500' }
      },
      {
        name: 'Protocol TVL Batch 9',
        job: protocolTvlJob,
        schedule: 'Daily at 10:05 AM UTC',
        params: { offset: '4000', limit: '500' }
      },
      {
        name: 'Protocol TVL Batch 10',
        job: protocolTvlJob,
        schedule: 'Daily at 10:05 AM UTC',
        params: { offset: '4500', limit: '500' }
      },
      {
        name: 'Protocol TVL Batch 11',
        job: protocolTvlJob,
        schedule: 'Daily at 10:05 AM UTC',
        params: { offset: '5000', limit: '500' }
      },
      {
        name: 'Protocol TVL Batch 12',
        job: protocolTvlJob,
        schedule: 'Daily at 10:05 AM UTC',
        params: { offset: '5500', limit: '500' }
      },
      {
        name: 'Protocol TVL Batch 13',
        job: protocolTvlJob,
        schedule: 'Daily at 10:05 AM UTC',
        params: { offset: '6000', limit: '467' } // Final batch covers remaining protocols
      }
    ];

    console.log(`📦 Running ${dailyJobs.length} daily jobs in parallel`);

    // Execute all daily jobs in parallel
    const jobPromises = dailyJobs.map(async (jobConfig, index) => {
      console.log(`🔄 Starting ${jobConfig.name} job...`);
      
      // Create mock request/response for the job
      const mockReq = {
        query: jobConfig.params || {}
      };
      
      const mockRes = {
        status: (code) => ({
          json: (data) => {
            console.log(`✅ ${jobConfig.name} completed: ${data.success ? 'SUCCESS' : 'FAILED'}`);
            if (data.metrics) {
              console.log(`   📊 ${data.metrics.totalRecords || 'N/A'} records processed`);
            }
            return { code, data };
          }
        })
      };

      try {
        await jobConfig.job(mockReq, mockRes);
        return { 
          job: jobConfig.name, 
          success: true,
          schedule: jobConfig.schedule
        };
      } catch (error) {
        console.error(`❌ ${jobConfig.name} failed:`, error.message);
        return { 
          job: jobConfig.name, 
          success: false, 
          error: error.message,
          schedule: jobConfig.schedule
        };
      }
    });

    // Wait for all jobs to complete
    const results = await Promise.all(jobPromises);
    
    // Calculate summary
    const successful = results.filter(r => r.success).length;
    const failed = results.filter(r => !r.success).length;
    const processingTime = Date.now() - startTime;

    console.log(`🎉 Daily Jobs Dispatcher Complete!`);
    console.log(`   ✅ Successful jobs: ${successful}/${dailyJobs.length}`);
    console.log(`   ❌ Failed jobs: ${failed}/${dailyJobs.length}`);
    console.log(`   ⏱️  Total time: ${(processingTime / 1000).toFixed(1)}s`);

    // Return success if all jobs succeeded
    const success = successful === dailyJobs.length;

    res.status(200).json({
      success,
      jobRunId,
      dispatcher: 'daily_jobs',
      summary: {
        totalJobs: dailyJobs.length,
        successfulJobs: successful,
        failedJobs: failed,
        processingTimeMs: processingTime,
        successRate: (successful / dailyJobs.length * 100).toFixed(1) + '%',
        executionTime: new Date().toISOString()
      },
      results
    });

  } catch (error) {
    console.error('❌ Daily Jobs Dispatcher failed:', error);
    res.status(500).json({
      success: false,
      jobRunId,
      dispatcher: 'daily_jobs',
      error: error.message,
      processingTimeMs: Date.now() - startTime
    });
  }
};
