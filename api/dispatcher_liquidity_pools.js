// api/dispatcher_liquidity_pools.js
// Parallel Liquidity Pool Collection Dispatcher
// Runs all liquidity pool batches in parallel every hour

module.exports.config = { runtime: 'nodejs18.x' };

// Import the actual job function
const poolJob = require('./job_liquidity_pools_with_quality.js');

// Pool configuration
const POOL_CONFIG = {
  totalPools: 20000, // Approximate total pools
  batchSize: 2000,
  maxConcurrency: 10 // Run 10 batches in parallel (same as current)
};

function generateJobRunId() {
  return `dispatcher_pools_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

module.exports = async (req, res) => {
  const startTime = Date.now();
  const jobRunId = generateJobRunId();
  
  console.log(`🏊 Starting Liquidity Pool Dispatcher: ${jobRunId}`);
  console.log(`📊 Config: ${POOL_CONFIG.totalPools} pools, ${POOL_CONFIG.batchSize} per batch, ${POOL_CONFIG.maxConcurrency} parallel batches`);

  try {
    // Calculate all batches
    const batches = [];
    for (let offset = 0; offset < POOL_CONFIG.totalPools; offset += POOL_CONFIG.batchSize) {
      batches.push({
        offset,
        limit: Math.min(POOL_CONFIG.batchSize, POOL_CONFIG.totalPools - offset)
      });
    }

    console.log(`📦 Created ${batches.length} batches to process in parallel`);

    // Execute all batches in parallel
    const batchPromises = batches.map(async (batch, index) => {
      console.log(`🔄 Starting pool batch ${index + 1}/${batches.length}: offset=${batch.offset}, limit=${batch.limit}`);
      
      // Create mock request/response for the job
      const mockReq = {
        query: {
          offset: batch.offset.toString(),
          limit: batch.limit.toString()
        }
      };
      
      const mockRes = {
        status: (code) => ({
          json: (data) => {
            console.log(`✅ Pool batch ${index + 1} completed: ${data.success ? 'SUCCESS' : 'FAILED'}`);
            if (data.metrics) {
              console.log(`   📊 ${data.metrics.totalRecords} records, ${data.metrics.cleanRecords} clean, ${data.metrics.scrubbedRecords} scrubbed`);
            }
            return { code, data };
          }
        })
      };

      try {
        await poolJob(mockReq, mockRes);
        return { batch: index + 1, success: true };
      } catch (error) {
        console.error(`❌ Pool batch ${index + 1} failed:`, error.message);
        return { batch: index + 1, success: false, error: error.message };
      }
    });

    // Wait for all batches to complete
    const results = await Promise.all(batchPromises);
    
    // Calculate summary
    const successful = results.filter(r => r.success).length;
    const failed = results.filter(r => !r.success).length;
    const processingTime = Date.now() - startTime;

    console.log(`🎉 Liquidity Pool Dispatcher Complete!`);
    console.log(`   ✅ Successful batches: ${successful}/${batches.length}`);
    console.log(`   ❌ Failed batches: ${failed}/${batches.length}`);
    console.log(`   ⏱️  Total time: ${(processingTime / 1000).toFixed(1)}s`);

    // Return success if most batches succeeded
    const success = successful >= Math.ceil(batches.length * 0.7); // 70% success threshold

    res.status(200).json({
      success,
      jobRunId,
      dispatcher: 'liquidity_pools',
      summary: {
        totalBatches: batches.length,
        successfulBatches: successful,
        failedBatches: failed,
        processingTimeMs: processingTime,
        successRate: (successful / batches.length * 100).toFixed(1) + '%'
      },
      results
    });

  } catch (error) {
    console.error('❌ Liquidity Pool Dispatcher failed:', error);
    res.status(500).json({
      success: false,
      jobRunId,
      dispatcher: 'liquidity_pools',
      error: error.message,
      processingTimeMs: Date.now() - startTime
    });
  }
};
