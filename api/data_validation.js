// api/data_validation.js
// Minimal data validation utilities for remaining jobs

function generateJobRunId() {
  return `job_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

/**
 * Insert data into scrub table (minimal implementation)
 */
async function insertIntoScrubTable(client, tableName, data, validation, jobRunId, originalData) {
  // Minimal implementation - just log for now
  console.log(`📝 Would scrub ${tableName}: ${JSON.stringify(data).substring(0, 100)}...`);
}

/**
 * Update quality summary (minimal implementation)
 */
async function updateQualitySummary(client, jobName, jobRunId, metrics) {
  // Minimal implementation - just log for now
  console.log(`📊 Quality summary for ${jobName}: ${JSON.stringify(metrics)}`);
}

/**
 * Validate protocol TVL data (minimal implementation)
 */
function validateProtocolTvlData(tvlData) {
  // Accept all valid data
  if (!tvlData || typeof tvlData !== 'object') {
    return {
      isValid: false,
      errors: ['invalid_data'],
      qualityScore: 0,
      isOutlier: false,
      outlierReason: null
    };
  }

  // Accept all valid protocol data
  return {
    isValid: true,
    errors: [],
    qualityScore: 100,
    isOutlier: false,
    outlierReason: null
  };
}

/**
 * Validate liquidity pool data (minimal implementation)
 */
function validatePoolData(poolData) {
  // Accept all valid data
  if (!poolData || typeof poolData !== 'object') {
    return {
      isValid: false,
      errors: ['invalid_data'],
      qualityScore: 0,
      isOutlier: false,
      outlierReason: null
    };
  }

  return {
    isValid: true,
    errors: [],
    qualityScore: 100,
    isOutlier: false,
    outlierReason: null
  };
}

/**
 * Validate lending market data (minimal implementation)
 */
function validateLendingMarket(lendingData) {
  // Accept all valid data
  if (!lendingData || typeof lendingData !== 'object') {
    return {
      isValid: false,
      errors: ['invalid_data'],
      qualityScore: 0,
      isOutlier: false,
      outlierReason: null
    };
  }

  return {
    isValid: true,
    errors: [],
    qualityScore: 100,
    isOutlier: false,
    outlierReason: null
  };
}

module.exports = {
  generateJobRunId,
  insertIntoScrubTable,
  updateQualitySummary,
  validateProtocolTvlData,
  validatePoolData,
  validateLendingMarket
};
