// api/dispatcher_token_prices.js
// Parallel Token Price Collection Dispatcher
// Runs all token price batches in parallel every 5 minutes

module.exports.config = { runtime: 'nodejs18.x' };

const { Pool } = require('pg');

// Import the actual job function
const tokenPriceJob = require('./backfill_token_prices_with_quality.js');

// Token price configuration - OPTIMIZED: Active tokens only
const TOKEN_PRICE_CONFIG = {
  totalTokens: 2454,   // Filtered to only tokens with fresh price data (67.1% coverage)
  batchSize: 818,      // Adjusted to evenly divide active tokens across 3 batches
  maxConcurrency: 3    // Keep 3 parallel batches for optimal performance
};

function generateJobRunId() {
  return `dispatcher_token_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

module.exports = async (req, res) => {
  const startTime = Date.now();
  const jobRunId = generateJobRunId();
  
  console.log(`🚀 Starting Token Price Dispatcher: ${jobRunId}`);
  console.log(`📊 Config: ${TOKEN_PRICE_CONFIG.totalTokens} tokens, ${TOKEN_PRICE_CONFIG.batchSize} per batch, ${TOKEN_PRICE_CONFIG.maxConcurrency} parallel batches`);
  console.log(`🎯 OPTIMIZED: Processing ${TOKEN_PRICE_CONFIG.totalTokens} active tokens (filtered for fresh price data)`);

  try {
    // Calculate all batches
    const batches = [];
    for (let offset = 0; offset < TOKEN_PRICE_CONFIG.totalTokens; offset += TOKEN_PRICE_CONFIG.batchSize) {
      batches.push({
        offset,
        limit: Math.min(TOKEN_PRICE_CONFIG.batchSize, TOKEN_PRICE_CONFIG.totalTokens - offset)
      });
    }

    console.log(`📦 Created ${batches.length} batches to process in parallel`);

    // Execute all batches in parallel
    const batchPromises = batches.map(async (batch, index) => {
      console.log(`🔄 Starting batch ${index + 1}/${batches.length}: offset=${batch.offset}, limit=${batch.limit}`);
      
      // Create mock request/response for the job
      const mockReq = {
        query: {
          offset: batch.offset.toString(),
          limit: batch.limit.toString()
        }
      };
      
      let jobResult = null;
      
      const mockRes = {
        status: (code) => ({
          json: (data) => {
            console.log(`✅ Batch ${index + 1} completed: ${data.success ? 'SUCCESS' : 'FAILED'}`);
            if (data.metrics) {
              console.log(`   📊 ${data.metrics.totalRecords} records, ${data.metrics.cleanRecords} clean, ${data.metrics.scrubbedRecords} scrubbed`);
            }
            jobResult = { code, data }; // Capture the actual result
            return { code, data };
          }
        })
      };

      try {
        await tokenPriceJob(mockReq, mockRes);
        // Check the actual job success, not just if it completed without throwing
        const actualSuccess = jobResult?.data?.success || false;
        return { 
          batch: index + 1, 
          success: actualSuccess,
          error: actualSuccess ? null : (jobResult?.data?.error || 'Job completed but reported failure')
        };
      } catch (error) {
        console.error(`❌ Batch ${index + 1} failed:`, error.message);
        return { batch: index + 1, success: false, error: error.message };
      }
    });

    // Wait for all batches to complete
    const results = await Promise.all(batchPromises);
    
    // Calculate summary
    const successful = results.filter(r => r.success).length;
    const failed = results.filter(r => !r.success).length;
    const processingTime = Date.now() - startTime;

    console.log(`🎉 Token Price Dispatcher Complete!`);
    console.log(`   ✅ Successful batches: ${successful}/${batches.length}`);
    console.log(`   ❌ Failed batches: ${failed}/${batches.length}`);
    console.log(`   ⏱️  Total time: ${(processingTime / 1000).toFixed(1)}s`);

    // Return success if most batches succeeded
    const success = successful >= Math.ceil(batches.length * 0.7); // 70% success threshold

    res.status(200).json({
      success,
      jobRunId,
      dispatcher: 'token_prices',
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
    console.error('❌ Token Price Dispatcher failed:', error);
    res.status(500).json({
      success: false,
      jobRunId,
      dispatcher: 'token_prices',
      error: error.message,
      processingTimeMs: Date.now() - startTime
    });
  }
};
