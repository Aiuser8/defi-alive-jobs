// api/dispatcher_lending_markets.js
// Parallel Lending Market Collection Dispatcher
// Runs all lending market batches in parallel every 6 hours

module.exports.config = { runtime: 'nodejs18.x' };

// Import the actual job function
const lendingJob = require('./job_lending_with_quality.js');

// Lending market configuration
const LENDING_CONFIG = {
  totalMarkets: 2000, // Approximate total markets
  batchSize: 500,
  maxConcurrency: 4 // Run 4 batches in parallel
};

function generateJobRunId() {
  return `dispatcher_lending_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

module.exports = async (req, res) => {
  const startTime = Date.now();
  const jobRunId = generateJobRunId();
  
  console.log(`🏦 Starting Lending Market Dispatcher: ${jobRunId}`);
  console.log(`📊 Config: ${LENDING_CONFIG.totalMarkets} markets, ${LENDING_CONFIG.batchSize} per batch, ${LENDING_CONFIG.maxConcurrency} parallel batches`);

  try {
    // Calculate all batches
    const batches = [];
    for (let offset = 0; offset < LENDING_CONFIG.totalMarkets; offset += LENDING_CONFIG.batchSize) {
      batches.push({
        offset,
        limit: Math.min(LENDING_CONFIG.batchSize, LENDING_CONFIG.totalMarkets - offset)
      });
    }

    console.log(`📦 Created ${batches.length} batches to process in parallel`);

    // Execute all batches in parallel
    const batchPromises = batches.map(async (batch, index) => {
      console.log(`🔄 Starting lending batch ${index + 1}/${batches.length}: offset=${batch.offset}, limit=${batch.limit}`);
      
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
            console.log(`✅ Lending batch ${index + 1} completed: ${data.success ? 'SUCCESS' : 'FAILED'}`);
            if (data.metrics) {
              console.log(`   📊 ${data.metrics.totalRecords} records, ${data.metrics.cleanRecords} clean, ${data.metrics.scrubbedRecords} scrubbed`);
            }
            return { code, data };
          }
        })
      };

      try {
        await lendingJob(mockReq, mockRes);
        return { batch: index + 1, success: true };
      } catch (error) {
        console.error(`❌ Lending batch ${index + 1} failed:`, error.message);
        return { batch: index + 1, success: false, error: error.message };
      }
    });

    // Wait for all batches to complete
    const results = await Promise.all(batchPromises);
    
    // Calculate summary
    const successful = results.filter(r => r.success).length;
    const failed = results.filter(r => !r.success).length;
    const processingTime = Date.now() - startTime;

    console.log(`🎉 Lending Market Dispatcher Complete!`);
    console.log(`   ✅ Successful batches: ${successful}/${batches.length}`);
    console.log(`   ❌ Failed batches: ${failed}/${batches.length}`);
    console.log(`   ⏱️  Total time: ${(processingTime / 1000).toFixed(1)}s`);

    // Return success if most batches succeeded
    const success = successful >= Math.ceil(batches.length * 0.7); // 70% success threshold

    res.status(200).json({
      success,
      jobRunId,
      dispatcher: 'lending_markets',
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
    console.error('❌ Lending Market Dispatcher failed:', error);
    res.status(500).json({
      success: false,
      jobRunId,
      dispatcher: 'lending_markets',
      error: error.message,
      processingTimeMs: Date.now() - startTime
    });
  }
};
