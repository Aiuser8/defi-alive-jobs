// data_validation.js - Data Quality Validation Functions
// This module provides validation functions for all data types in the pipeline

/**
 * Generate a unique job run ID for tracking
 */
function generateJobRunId() {
  return `job_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

/**
 * Validate token price data
 */
function validateTokenPrice(priceData, previousPrice = null) {
  const errors = [];
  let qualityScore = 100;
  let isOutlier = false;
  let outlierReason = null;

  // Basic validation
  if (!priceData.price || typeof priceData.price !== 'number') {
    errors.push('invalid_price');
    qualityScore -= 30;
  } else {
    const price = priceData.price;
    
    // Price range validation
    if (price <= 0) {
      errors.push('negative_price');
      qualityScore -= 40;
    } else if (price > 1e12) {
      errors.push('unrealistic_price_high');
      qualityScore -= 30;
    }
    
    // Outlier detection based on previous price
    if (previousPrice && previousPrice > 0) {
      const priceChange = Math.abs(price - previousPrice) / previousPrice;
      
      // Detect sudden price changes (>50% in 15 minutes)
      if (priceChange > 0.5) {
        isOutlier = true;
        outlierReason = `sudden_price_change_${(priceChange * 100).toFixed(1)}%`;
        qualityScore -= 20;
      }
      
      // Detect extreme price changes (>90%)
      if (priceChange > 0.9) {
        errors.push('extreme_price_change');
        qualityScore -= 50;
      }
    }
  }

  // Timestamp validation
  if (!priceData.timestamp || !Number.isFinite(priceData.timestamp)) {
    errors.push('invalid_timestamp');
    qualityScore -= 25;
  } else {
    const ageMinutes = (Date.now() - priceData.timestamp * 1000) / (1000 * 60);
    if (ageMinutes > 60) {
      errors.push('stale_data');
      qualityScore -= Math.min(30, ageMinutes / 2);
    }
  }

  // Confidence validation
  if (priceData.confidence && (priceData.confidence < 0 || priceData.confidence > 1)) {
    errors.push('invalid_confidence');
    qualityScore -= 10;
  }

  return {
    isValid: errors.length === 0 && qualityScore >= 70,
    errors,
    qualityScore: Math.max(0, qualityScore),
    isOutlier,
    outlierReason
  };
}

/**
 * Validate lending market data
 */
function validateLendingMarket(lendingData) {
  const errors = [];
  let qualityScore = 100;
  let isOutlier = false;
  let outlierReason = null;

  // Basic field validation
  if (!lendingData.market_id) {
    errors.push('missing_market_id');
    qualityScore -= 40;
  }

  // APY validation
  const apyFields = ['apyBase', 'apyReward', 'apyBaseBorrow', 'apyRewardBorrow'];
  for (const field of apyFields) {
    if (lendingData[field] !== null && lendingData[field] !== undefined) {
      const apy = lendingData[field];
      if (typeof apy !== 'number') {
        errors.push(`invalid_${field}`);
        qualityScore -= 10;
      } else if (apy < -100 || apy > 10000) { // -100% to 10,000%
        isOutlier = true;
        outlierReason = `extreme_${field}_${apy.toFixed(2)}%`;
        qualityScore -= 20;
      }
    }
  }

  // USD amount validation
  const usdFields = ['totalSupplyUsd', 'totalBorrowUsd', 'debtCeilingUsd'];
  for (const field of usdFields) {
    if (lendingData[field] !== null && lendingData[field] !== undefined) {
      const amount = lendingData[field];
      if (typeof amount !== 'number' || amount < 0) {
        errors.push(`invalid_${field}`);
        qualityScore -= 15;
      } else if (amount > 1e15) { // > $1 quadrillion
        isOutlier = true;
        outlierReason = `unrealistic_${field}_${amount.toExponential()}`;
        qualityScore -= 25;
      }
    }
  }

  // Timestamp validation
  if (!lendingData.timestamp || !Number.isFinite(lendingData.timestamp)) {
    errors.push('invalid_timestamp');
    qualityScore -= 25;
  }

  return {
    isValid: errors.length === 0 && qualityScore >= 70,
    errors,
    qualityScore: Math.max(0, qualityScore),
    isOutlier,
    outlierReason
  };
}

/**
 * Validate ETF flow data
 */
function validateEtfFlow(etfData) {
  const errors = [];
  let qualityScore = 100;
  let isOutlier = false;
  let outlierReason = null;

  // Basic validation
  if (!etfData.gecko_id) {
    errors.push('missing_gecko_id');
    qualityScore -= 40;
  }

  if (!etfData.day) {
    errors.push('missing_day');
    qualityScore -= 30;
  }

  // Flow amount validation
  if (etfData.total_flow_usd !== null && etfData.total_flow_usd !== undefined) {
    const flow = etfData.total_flow_usd;
    if (typeof flow !== 'number') {
      errors.push('invalid_flow_amount');
      qualityScore -= 20;
    } else if (Math.abs(flow) > 1e12) { // > $1 trillion daily flow
      isOutlier = true;
      outlierReason = `extreme_daily_flow_${flow.toExponential()}`;
      qualityScore -= 15;
    }
  }

  return {
    isValid: errors.length === 0 && qualityScore >= 70,
    errors,
    qualityScore: Math.max(0, qualityScore),
    isOutlier,
    outlierReason
  };
}

/**
 * Validate stablecoin market cap data
 */
function validateStablecoinMcap(stablecoinData) {
  const errors = [];
  let qualityScore = 100;
  let isOutlier = false;
  let outlierReason = null;

  // Basic validation
  if (!stablecoinData.peg) {
    errors.push('missing_peg');
    qualityScore -= 30;
  }

  if (!stablecoinData.day) {
    errors.push('missing_day');
    qualityScore -= 30;
  }

  // Amount validation
  if (stablecoinData.amount_usd !== null && stablecoinData.amount_usd !== undefined) {
    const amount = stablecoinData.amount_usd;
    if (typeof amount !== 'number' || amount < 0) {
      errors.push('invalid_amount');
      qualityScore -= 25;
    } else if (amount > 1e15) { // > $1 quadrillion
      isOutlier = true;
      outlierReason = `unrealistic_mcap_${amount.toExponential()}`;
      qualityScore -= 20;
    }
  }

  return {
    isValid: errors.length === 0 && qualityScore >= 70,
    errors,
    qualityScore: Math.max(0, qualityScore),
    isOutlier,
    outlierReason
  };
}

/**
 * Get previous price for outlier detection
 */
async function getPreviousPrice(client, coinId, currentTimestamp) {
  try {
    const result = await client.query(`
      SELECT price_usd, price_timestamp 
      FROM update.token_price_daily 
      WHERE coin_id = $1 AND price_timestamp < $2
      ORDER BY price_timestamp DESC 
      LIMIT 1
    `, [coinId, new Date(currentTimestamp * 1000).toISOString()]);
    
    return result.rows[0]?.price_usd || null;
  } catch (error) {
    console.warn(`Failed to get previous price for ${coinId}:`, error.message);
    return null;
  }
}

/**
 * Insert data into scrub table
 */
async function insertIntoScrubTable(client, tableName, data, validation, jobRunId, originalData) {
  const scrubData = {
    ...data,
    validation_errors: validation.errors,
    quality_score: validation.qualityScore,
    is_outlier: validation.isOutlier,
    outlier_reason: validation.outlierReason,
    original_data: originalData,
    job_run_id: jobRunId
  };
  
  // Fix column name mapping for different tables
  if (tableName === 'token_price_scrub') {
    if (data.coinId) {
      scrubData.coin_id = data.coinId;
      delete scrubData.coinId;
    }
    if (data.tsSec) {
      scrubData.price_timestamp = new Date(data.tsSec * 1000).toISOString();
      delete scrubData.tsSec;
    }
    if (data.price) {
      scrubData.price_usd = data.price;
      delete scrubData.price;
    }
    if (data.symbol) {
      scrubData.symbol = data.symbol;
    }
    if (data.confidence) {
      scrubData.confidence = data.confidence;
    }
    if (data.decimals) {
      scrubData.decimals = data.decimals;
    }
    // Remove timestamp field that doesn't exist in the table
    delete scrubData.timestamp;
  }
  if (tableName === 'lending_market_scrub' && data.market_id) {
    scrubData.market_id = data.market_id;
  }

  // Remove undefined values and convert to proper format
  const cleanData = Object.fromEntries(
    Object.entries(scrubData).filter(([_, v]) => v !== undefined)
  );

  const columns = Object.keys(cleanData);
  const values = Object.values(cleanData);
  const placeholders = values.map((_, i) => `$${i + 1}`).join(', ');

  await client.query(`
    INSERT INTO scrub.${tableName} (${columns.join(', ')})
    VALUES (${placeholders})
  `, values);
}

/**
 * Update data quality summary
 */
async function updateQualitySummary(client, jobName, jobRunId, metrics) {
  const {
    totalRecords,
    cleanRecords,
    scrubbedRecords,
    errorRecords,
    outlierRecords,
    processingTimeMs,
    errorSummary
  } = metrics;

  const overallQualityScore = totalRecords > 0 ? (cleanRecords / totalRecords) * 100 : 0;

  await client.query(`
    INSERT INTO scrub.data_quality_summary (
      job_name, job_run_id, total_records, clean_records, scrubbed_records,
      error_records, outlier_records, overall_quality_score,
      processing_time_ms, error_summary
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    ON CONFLICT (job_name, job_run_id) DO UPDATE SET
      total_records = EXCLUDED.total_records,
      clean_records = EXCLUDED.clean_records,
      scrubbed_records = EXCLUDED.scrubbed_records,
      error_records = EXCLUDED.error_records,
      outlier_records = EXCLUDED.outlier_records,
      overall_quality_score = EXCLUDED.overall_quality_score,
      processing_time_ms = EXCLUDED.processing_time_ms,
      error_summary = EXCLUDED.error_summary
  `, [
    jobName, jobRunId, totalRecords, cleanRecords, scrubbedRecords,
    errorRecords, outlierRecords, overallQualityScore,
    processingTimeMs, JSON.stringify(errorSummary)
  ]);
}

module.exports = {
  generateJobRunId,
  validateTokenPrice,
  validateLendingMarket,
  validateEtfFlow,
  validateStablecoinMcap,
  getPreviousPrice,
  insertIntoScrubTable,
  updateQualitySummary
};
