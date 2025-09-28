// api/dispatcher_token_prices_direct.js
// Direct token price dispatcher - no validation, fast parallel data collection

module.exports.config = { runtime: 'nodejs18.x' };

// Import the direct token price job
const tokenPriceJob = require('./backfill_token_prices_direct.js');

module.exports = async function(req, res) {
  const startTime = Date.now();
  
  try {
    console.log('🚀 Starting Direct Token Price Dispatcher...');
    
    // Load token list to determine batch configuration
    const fs = require('fs');
    const path = require('path');
    const tokenListPath = path.join(process.cwd(), 'token_list_active.json');
    const tokenList = JSON.parse(fs.readFileSync(tokenListPath, 'utf8'));
    
    // Convert token list to coin IDs for accurate count
    const coinIds = [];
    for (const item of tokenList) {
      const chain = String(item.chain || '').trim().toLowerCase();
      const address = String(item.address || '').trim();
      if (!chain || !address) continue;
      
      const addrForSlug = ['ethereum', 'polygon', 'arbitrum', 'optimism', 'base', 'avalanche', 'bsc'].includes(chain) 
        ? address.toLowerCase() 
        : address;
      
      coinIds.push(`${chain}:${addrForSlug}`);
    }
    
    const totalTokens = coinIds.length;
    const tokensPerBatch = 818;
    const maxParallelBatches = 3;
    
    console.log(`📊 Config: ${totalTokens} tokens, ${tokensPerBatch} per batch, ${maxParallelBatches} parallel batches`);
    console.log(`🎯 Processing ${totalTokens} active tokens (filtered for fresh price data)`);
    
    // Calculate batches
    const totalBatches = Math.ceil(totalTokens / tokensPerBatch);
    const batches = [];
    
    for (let i = 0; i < totalBatches; i++) {
      const offset = i * tokensPerBatch;
      const limit = Math.min(tokensPerBatch, totalTokens - offset);
      
      batches.push({ offset, limit, batchNumber: i + 1 });
    }
    
    console.log(`📦 Created ${batches.length} batches to process in parallel`);
    
    // Process batches in parallel
    const results = [];
    let successfulBatches = 0;
    let failedBatches = 0;
    
    for (let i = 0; i < batches.length; i += maxParallelBatches) {
      const batchGroup = batches.slice(i, i + maxParallelBatches);
      
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
          
          // Execute the token price job
          await tokenPriceJob(jobReq, jobRes);
          
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
    
    const totalSkipped = results
      .filter(r => r.success)
      .reduce((sum, r) => sum + (r.result?.skippedRecords || 0), 0);
    
    console.log('🎉 Direct Token Price Dispatcher Complete!');
    console.log(`✅ Successful batches: ${successfulBatches}/${batches.length}`);
    console.log(`❌ Failed batches: ${failedBatches}/${batches.length}`);
    console.log(`📊 Total records inserted: ${totalInserted}`);
    console.log(`⚠️ Total records skipped: ${totalSkipped}`);
    console.log(`⏱️  Total time: ${(duration / 1000).toFixed(1)}s`);
    
    res.status(200).json({
      success: true,
      message: 'Direct token price collection completed',
      totalBatches: batches.length,
      successfulBatches,
      failedBatches,
      totalInserted,
      totalSkipped,
      processingTimeMs: duration,
      batchResults: results
    });
    
  } catch (error) {
    console.error('❌ Direct Token Price Dispatcher failed:', error);
    
    res.status(500).json({
      success: false,
      error: error.message,
      processingTimeMs: Date.now() - startTime
    });
  }
};
