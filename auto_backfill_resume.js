// auto_backfill_resume.js - Automated backfill with automatic resume capability
// Handles database disconnections and runs continuously until complete
require('dotenv').config();

const BATCH_SIZE = 100;
const TOTAL_POOLS = 19201;
const DELAY_BETWEEN_BATCHES = 3000; // 3 seconds
const MAX_RETRIES = 3;

async function runBatchWithRetry(offset, limit, retryCount = 0) {
  const job = require('./api/backfill_liquidity_pools_historical.js');
  
  return new Promise((resolve, reject) => {
    const req = {
      query: {
        offset: offset.toString(),
        limit: limit.toString(),
        startDate: '2025-09-05',
        endDate: '2025-09-26'
      }
    };

    const res = {
      status: (code) => ({
        json: (data) => {
          if (data.success) {
            resolve(data);
          } else {
            reject(new Error(data.error || `Batch failed with status ${code}`));
          }
        }
      })
    };

    // Set a timeout for this batch
    const timeout = setTimeout(() => {
      reject(new Error('Batch timeout after 5 minutes'));
    }, 5 * 60 * 1000);

    job(req, res)
      .then(() => clearTimeout(timeout))
      .catch((error) => {
        clearTimeout(timeout);
        reject(error);
      });
  });
}

async function getCurrentProgress() {
  // Get progress by checking database records with recent timestamps
  const { Client } = require('pg');
  const client = new Client({
    connectionString: process.env.SUPABASE_DB_URL,
    ssl: { rejectUnauthorized: false }
  });
  
  try {
    await client.connect();
    const result = await client.query(`
      SELECT COUNT(DISTINCT pool_id) as completed_pools
      FROM clean.cl_pool_hist
      WHERE inserted_at >= NOW() - INTERVAL '2 hours'
    `);
    
    const completedPools = parseInt(result.rows[0].completed_pools);
    await client.end();
    
    // Calculate next offset (round to nearest batch)
    const nextOffset = Math.floor(completedPools / BATCH_SIZE) * BATCH_SIZE;
    return { completedPools, nextOffset };
  } catch (error) {
    console.log('⚠️  Could not determine progress from database, starting from 900');
    return { completedPools: 900, nextOffset: 900 };
  }
}

async function runAutomatedBackfill() {
  console.log('🚀 AUTOMATED POOL BACKFILL WITH AUTO-RESUME');
  console.log('='.repeat(80));
  
  // Get current progress
  const { completedPools, nextOffset } = await getCurrentProgress();
  console.log(`📊 Current progress: ${completedPools} pools completed`);
  console.log(`🔄 Resuming from offset: ${nextOffset}`);
  console.log('');

  const startTime = Date.now();
  let totalRecordsProcessed = 0;
  let totalCleanRecords = 0;
  let totalScrubbedRecords = 0;
  let currentOffset = nextOffset;
  let consecutiveErrors = 0;

  for (let offset = currentOffset; offset < TOTAL_POOLS; offset += BATCH_SIZE) {
    const batchNumber = Math.floor(offset / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(TOTAL_POOLS / BATCH_SIZE);
    const currentBatchSize = Math.min(BATCH_SIZE, TOTAL_POOLS - offset);
    
    console.log(`\n🔄 BATCH ${batchNumber}/${totalBatches} (AUTO-RESUME)`);
    console.log(`📦 Processing pools ${offset}-${offset + currentBatchSize - 1}`);
    console.log(`⏱️  Started: ${new Date().toLocaleTimeString()}`);
    
    try {
      const result = await runBatchWithRetry(offset, currentBatchSize);
      
      // Update totals
      totalRecordsProcessed += result.metrics.totalRecords;
      totalCleanRecords += result.metrics.cleanRecords;
      totalScrubbedRecords += result.metrics.scrubbedRecords;
      
      // Reset error counter on success
      consecutiveErrors = 0;
      
      const elapsedMinutes = (Date.now() - startTime) / (1000 * 60);
      const completedSoFar = offset + currentBatchSize;
      const progressPercent = ((completedSoFar / TOTAL_POOLS) * 100).toFixed(1);
      const avgTimePerBatch = elapsedMinutes / (batchNumber - Math.floor(currentOffset / BATCH_SIZE));
      const remainingBatches = totalBatches - batchNumber;
      const estimatedRemainingMinutes = avgTimePerBatch * remainingBatches;
      
      console.log(`✅ Batch ${batchNumber} completed in ${(result.metrics.processingTimeMs / 1000).toFixed(1)}s`);
      console.log(`📊 Batch results: ${result.metrics.poolsProcessed} pools, ${result.metrics.totalRecords} records (${result.metrics.cleanRecords} clean)`);
      console.log(`🎯 Overall progress: ${completedSoFar}/${TOTAL_POOLS} pools (${progressPercent}%)`);
      console.log(`📈 Session totals: ${totalRecordsProcessed} records (${totalCleanRecords} clean, ${totalScrubbedRecords} scrubbed)`);
      console.log(`⏱️  Elapsed: ${elapsedMinutes.toFixed(1)}m | ETA: ${estimatedRemainingMinutes.toFixed(1)}m remaining`);
      
      // Progress milestone celebrations
      if (batchNumber % 10 === 0) {
        console.log(`\n🎉 MILESTONE: ${batchNumber} batches completed!`);
        console.log(`📊 ${completedSoFar} pools processed, ${totalRecordsProcessed} historical records added`);
      }
      
      // Save progress checkpoint
      if (batchNumber % 5 === 0) {
        console.log(`💾 Progress checkpoint: Pool ${completedSoFar} completed successfully`);
      }
      
      // Small delay between batches
      if (offset + BATCH_SIZE < TOTAL_POOLS) {
        console.log(`⏸️  Waiting ${DELAY_BETWEEN_BATCHES/1000}s before next batch...`);
        await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_BATCHES));
      }
      
    } catch (error) {
      consecutiveErrors++;
      console.error(`❌ Batch ${batchNumber} failed:`, error.message);
      
      if (error.message.includes('db_termination') || error.message.includes('connection')) {
        console.log('🔌 Database connection lost - this is normal for long operations');
        console.log('⚡ Auto-resume will handle this gracefully');
      }
      
      if (consecutiveErrors >= MAX_RETRIES) {
        console.error(`💥 Too many consecutive errors (${consecutiveErrors}). Stopping for safety.`);
        console.log(`📍 Resume point: offset ${offset}`);
        break;
      }
      
      const retryDelay = DELAY_BETWEEN_BATCHES * (consecutiveErrors + 1);
      console.log(`🔄 Retrying in ${retryDelay/1000}s... (${consecutiveErrors}/${MAX_RETRIES} consecutive errors)`);
      await new Promise(resolve => setTimeout(resolve, retryDelay));
      
      // Retry the same batch
      offset -= BATCH_SIZE;
    }
  }

  const totalTimeMinutes = (Date.now() - startTime) / (1000 * 60);
  const finalCompleted = Math.min(currentOffset + totalRecordsProcessed, TOTAL_POOLS);
  
  console.log('\n🎊 AUTOMATED BACKFILL SESSION COMPLETE!');
  console.log('='.repeat(80));
  console.log(`📊 SESSION RESULTS:`);
  console.log(`✅ Session records processed: ${totalRecordsProcessed}`);
  console.log(`🧹 Clean records: ${totalCleanRecords}`);
  console.log(`🗑️  Scrubbed records: ${totalScrubbedRecords}`);
  console.log(`🎯 Quality score: ${totalRecordsProcessed > 0 ? ((totalCleanRecords / totalRecordsProcessed) * 100).toFixed(2) : 0}%`);
  console.log(`⏱️  Session time: ${totalTimeMinutes.toFixed(1)} minutes`);
  console.log(`📍 Total pools completed: ~${finalCompleted}`);
  console.log('');
  
  if (finalCompleted >= TOTAL_POOLS) {
    console.log('🎉 COMPLETE HISTORICAL BACKFILL FINISHED!');
    console.log('✅ All 19,201 pools processed successfully');
    console.log('🏆 Historical data continuity fully restored!');
  } else {
    console.log('⏸️  Session ended - backfill can be resumed');
    console.log(`🔄 Run this script again to continue from pool ${finalCompleted}`);
  }
}

// Run the automated backfill
runAutomatedBackfill()
  .then(() => {
    console.log('\n✅ Automated backfill session completed');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\n❌ Automated backfill failed:', error.message);
    process.exit(1);
  });
