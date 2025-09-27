// api/dispatcher_liquidity_pools_direct.js
// Direct liquidity pools dispatcher - no validation, fast parallel data collection

module.exports.config = { runtime: 'nodejs18.x' };

// Import the direct pools job
const poolsJob = require('./job_liquidity_pools_direct.js');

// Pool configuration - optimized for direct collection
const POOL_CONFIG = {
  totalPools: 19200,    // Total pools to process
  batchSize: 1600,      // Pools per batch
  maxConcurrency: 6     // Parallel batches
};

module.exports = async function(req, res) {
  const startTime = Date.now();
  
  try {
    console.log('🚀 Starting Direct Liquidity Pools Dispatcher...');
    console.log(`📊 Config: ${POOL_CONFIG.totalPools} pools, ${POOL_CONFIG.batchSize} per batch, ${POOL_CONFIG.maxConcurrency} parallel batches`);
    
    // Calculate batches
    const totalBatches = Math.ceil(POOL_CONFIG.totalPools / POOL_CONFIG.batchSize);
    const batches = [];
    
    for (let i = 0; i < totalBatches; i++) {
      const offset = i * POOL_CONFIG.batchSize;
      const limit = Math.min(POOL_CONFIG.batchSize, POOL_CONFIG.totalPools - offset);
      
      batches.push({ offset, limit, batchNumber: i + 1 });
    }
    
    console.log(`📦 Created ${batches.length} batches to process in parallel`);
    
    // Process batches in parallel with concurrency limit
    const results = [];
    let successfulBatches = 0;
    let failedBatches = 0;
    
    for (let i = 0; i < batches.length; i += POOL_CONFIG.maxConcurrency) {
      const batchGroup = batches.slice(i, i + POOL_CONFIG.maxConcurrency);
      
      const batchPromises = batchGroup.map(async (batch) => {
        try {
          console.log(`🔄 Starting batch ${batch.batchNumber}/${batches.length}: offset=${batch.offset}, limit=${batch.limit}`);
          
          // Create mock request/response for the job
          const jobReq = {
            query: {
              offset: batch.offset.toString(),
              limit: batch.limit.toString()
            }
          };
          
          let jobResult = null;
          const jobRes = {
            status: (code) => ({
              json: (data) => {
                jobResult = { statusCode: code, data };
              }
            })
          };
          
          // Execute the pools job
          await poolsJob(jobReq, jobRes);
          
          if (jobResult && jobResult.statusCode === 200) {
            console.log(`✅ Batch ${batch.batchNumber} completed: SUCCESS`);
            successfulBatches++;
            return { success: true, batch: batch.batchNumber, result: jobResult.data };
          } else {
            throw new Error(`Job failed with status: ${jobResult?.statusCode || 'unknown'}`);
          }
          
        } catch (error) {
          console.error(`❌ Batch ${batch.batchNumber} failed:`, error.message);
          failedBatches++;
          return { success: false, batch: batch.batchNumber, error: error.message };
        }
      });
      
      const batchResults = await Promise.all(batchPromises);
      results.push(...batchResults);
    }
    
    const duration = Date.now() - startTime;
    
    // Calculate totals
    const totalInserted = results
      .filter(r => r.success)
      .reduce((sum, r) => sum + (r.result?.insertedRecords || 0), 0);
    
    const totalErrors = results
      .filter(r => r.success)
      .reduce((sum, r) => sum + (r.result?.errorRecords || 0), 0);
    
    console.log('🎉 Direct Liquidity Pools Dispatcher Complete!');
    console.log(`✅ Successful batches: ${successfulBatches}/${batches.length}`);
    console.log(`❌ Failed batches: ${failedBatches}/${batches.length}`);
    console.log(`📊 Total records inserted: ${totalInserted}`);
    console.log(`❌ Total records with errors: ${totalErrors}`);
    console.log(`⏱️  Total time: ${(duration / 1000).toFixed(1)}s`);
    
    res.status(200).json({
      success: true,
      message: 'Direct liquidity pools collection completed',
      totalBatches: batches.length,
      successfulBatches,
      failedBatches,
      totalInserted,
      totalErrors,
      processingTimeMs: duration,
      batchResults: results
    });
    
  } catch (error) {
    console.error('❌ Direct Liquidity Pools Dispatcher failed:', error);
    
    res.status(500).json({
      success: false,
      error: error.message,
      processingTimeMs: Date.now() - startTime
    });
  }
};
