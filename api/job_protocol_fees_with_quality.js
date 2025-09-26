// api/job_protocol_fees_with_quality.js (CommonJS)
// Live protocol fees and revenue collection with data quality gates
// Force Node runtime (pg not supported on Edge)
module.exports.config = { runtime: 'nodejs18.x' };

const { Pool } = require('pg');
const { generateJobRunId } = require('./data_validation');

function makePoolFromEnv() {
  return new Pool({
    connectionString: process.env.SUPABASE_DB_URL,
    ssl: {
      rejectUnauthorized: false,
    },
  });
}

function validateProtocolFeesData(feesData) {
  const errors = [];
  let qualityScore = 100;
  let isOutlier = false;
  let outlierReason = null;

  // Basic validation
  if (!feesData.protocol_id || typeof feesData.protocol_id !== 'string') {
    errors.push('invalid_protocol_id');
    qualityScore -= 30;
  }

  if (!feesData.name || typeof feesData.name !== 'string') {
    errors.push('missing_protocol_name');
    qualityScore -= 20;
  }

  // Revenue validation
  if (feesData.total_24h !== null && feesData.total_24h !== undefined) {
    const revenue24h = feesData.total_24h;
    
    if (revenue24h < 0) {
      errors.push('negative_revenue');
      qualityScore -= 40;
    } else if (revenue24h > 50000000) {
      // Revenue > $50M per day is extremely high
      isOutlier = true;
      outlierReason = `extremely_high_revenue_${(revenue24h / 1000000).toFixed(1)}M`;
      qualityScore -= 10;
    }
  }

  // Date validation
  if (!feesData.collection_date) {
    errors.push('missing_collection_date');
    qualityScore -= 20;
  }

  // Ensure quality score doesn't go below 0
  qualityScore = Math.max(0, qualityScore);

  return {
    isValid: errors.length === 0 && qualityScore >= 60,
    errors,
    qualityScore: Math.round(qualityScore),
    isOutlier,
    outlierReason
  };
}

async function insertCleanProtocolFees(client, feesData) {
  await client.query(`
    INSERT INTO update.protocol_fees_daily (
      protocol_id, defillama_id, name, display_name, slug, category, 
      chains, module, protocol_type, logo,
      total_24h, total_48h_to_24h, total_7d, total_14d_to_7d, 
      total_30d, total_60d_to_30d, total_1y, total_all_time,
      average_1y, monthly_average_1y,
      change_1d, change_7d, change_1m, change_7d_over_7d, change_30d_over_30d,
      total_7_days_ago, total_30_days_ago,
      breakdown_24h, breakdown_30d, methodology, methodology_url,
      collection_date, inserted_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33)
    ON CONFLICT (protocol_id, collection_date) DO UPDATE SET
      defillama_id = EXCLUDED.defillama_id,
      name = EXCLUDED.name,
      display_name = EXCLUDED.display_name,
      total_24h = EXCLUDED.total_24h,
      total_48h_to_24h = EXCLUDED.total_48h_to_24h,
      total_7d = EXCLUDED.total_7d,
      total_30d = EXCLUDED.total_30d,
      change_1d = EXCLUDED.change_1d,
      change_7d = EXCLUDED.change_7d,
      breakdown_24h = EXCLUDED.breakdown_24h,
      breakdown_30d = EXCLUDED.breakdown_30d,
      inserted_at = EXCLUDED.inserted_at
  `, [
    feesData.protocol_id,
    feesData.defillama_id,
    feesData.name,
    feesData.display_name,
    feesData.slug,
    feesData.category,
    feesData.chains,
    feesData.module,
    feesData.protocol_type,
    feesData.logo,
    feesData.total_24h,
    feesData.total_48h_to_24h,
    feesData.total_7d,
    feesData.total_14d_to_7d,
    feesData.total_30d,
    feesData.total_60d_to_30d,
    feesData.total_1y,
    feesData.total_all_time,
    feesData.average_1y,
    feesData.monthly_average_1y,
    feesData.change_1d,
    feesData.change_7d,
    feesData.change_1m,
    feesData.change_7d_over_7d,
    feesData.change_30d_over_30d,
    feesData.total_7_days_ago,
    feesData.total_30_days_ago,
    feesData.breakdown_24h,
    feesData.breakdown_30d,
    feesData.methodology,
    feesData.methodology_url,
    feesData.collection_date,
    new Date().toISOString()
  ]);
}

module.exports = async (req, res) => {
  const jobRunId = generateJobRunId();
  const startTime = Date.now();
  
  // Metrics tracking
  let totalRecords = 0;
  let cleanRecords = 0;
  let scrubbedRecords = 0;
  let errors = [];
  let processingTime = 0;
  let successRate = 0;

  try {
    console.log(`💰 Starting protocol fees collection job: ${jobRunId}`);

    // Fetch protocol fees data from DeFiLlama API
    const apiUrl = 'https://api.llama.fi/overview/fees';
    console.log(`📡 Fetching protocol fees from: ${apiUrl}`);
    
    const response = await fetch(apiUrl);
    if (!response.ok) {
      throw new Error(`API request failed with status ${response.status}: ${response.statusText}`);
    }

    const data = await response.json();
    if (!data.protocols || !Array.isArray(data.protocols)) {
      throw new Error('API response does not contain protocols array');
    }

    const protocols = data.protocols;
    totalRecords = protocols.length;
    
    console.log(`📊 Processing ${protocols.length} protocols with fee data`);

    const pool = makePoolFromEnv();
    const client = await pool.connect();

    try {
      const today = new Date().toISOString().split('T')[0]; // YYYY-MM-DD format

      for (const protocol of protocols) {
        const protocolFeesData = {
          protocol_id: protocol.id || protocol.defillamaId,
          defillama_id: protocol.defillamaId,
          name: protocol.name,
          display_name: protocol.displayName,
          slug: protocol.slug,
          category: protocol.category,
          chains: protocol.chains,
          module: protocol.module,
          protocol_type: protocol.protocolType,
          logo: protocol.logo,
          
          // Core metrics
          total_24h: protocol.total24h,
          total_48h_to_24h: protocol.total48hto24h,
          total_7d: protocol.total7d,
          total_14d_to_7d: protocol.total14dto7d,
          total_30d: protocol.total30d,
          total_60d_to_30d: protocol.total60dto30d,
          total_1y: protocol.total1y,
          total_all_time: protocol.totalAllTime,
          
          // Averages
          average_1y: protocol.average1y,
          monthly_average_1y: protocol.monthlyAverage1y,
          
          // Changes
          change_1d: protocol.change_1d,
          change_7d: protocol.change_7d,
          change_1m: protocol.change_1m,
          change_7d_over_7d: protocol.change_7dover7d,
          change_30d_over_30d: protocol.change_30dover30d,
          
          // Reference data
          total_7_days_ago: protocol.total7DaysAgo,
          total_30_days_ago: protocol.total30DaysAgo,
          
          // Breakdowns
          breakdown_24h: protocol.breakdown24h ? JSON.stringify(protocol.breakdown24h) : null,
          breakdown_30d: protocol.breakdown30d ? JSON.stringify(protocol.breakdown30d) : null,
          methodology: protocol.methodology ? JSON.stringify(protocol.methodology) : null,
          methodology_url: protocol.methodologyURL,
          
          collection_date: today
        };

        // Validate data quality
        const validation = validateProtocolFeesData(protocolFeesData);
        
        if (validation.isValid) {
          try {
            await insertCleanProtocolFees(client, protocolFeesData);
            cleanRecords++;
          } catch (error) {
            console.error(`❌ Error inserting clean protocol fees data:`, error.message);
            errors.push(`clean_insert_error: ${error.message}`);
            scrubbedRecords++;
          }
        } else {
          // For now, skip scrub table to avoid complexity
          scrubbedRecords++;
          console.log(`⚠️ Skipping invalid fees data for: ${protocolFeesData.name}`);
        }
      }

      // Calculate final metrics
      successRate = totalRecords > 0 ? (cleanRecords / totalRecords) * 100 : 0;
      processingTime = Date.now() - startTime;

    } finally {
      client.release();
    }
    
    console.log(`✅ Protocol fees collection completed successfully`);
    console.log(`📊 Results: ${totalRecords} total, ${cleanRecords} clean, ${scrubbedRecords} scrubbed`);
    console.log(`⏱️  Processing time: ${processingTime}ms`);
    console.log(`🎯 Success rate: ${successRate.toFixed(1)}%`);

    res.status(200).json({
      success: true,
      jobRunId,
      metrics: {
        totalRecords,
        cleanRecords,
        scrubbedRecords,
        successRate: parseFloat(successRate.toFixed(1)),
        avgQualityScore: successRate,
        processingTime
      },
      message: `Processed ${totalRecords} protocols with ${successRate.toFixed(1)}% success rate`
    });

  } catch (error) {
    processingTime = Date.now() - startTime;
    console.error(`❌ Protocol fees collection job failed:`, error.message);

    res.status(500).json({
      success: false,
      jobRunId,
      error: error.message,
      metrics: {
        totalRecords,
        cleanRecords,
        scrubbedRecords,
        processingTime
      }
    });
  }
};
