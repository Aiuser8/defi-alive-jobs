// api/job_lending_with_quality.js
// Enhanced lending job with data quality gates and scrub table routing

const fs = require("fs");
const path = require("path");
const { Client } = require("pg");
const {
  generateJobRunId,
  validateLendingMarket,
  insertIntoScrubTable,
  updateQualitySummary
} = require('./data_validation');

// ---------- helpers ----------
const toUnix = (d) => Math.floor(d.getTime() / 1000);
const isoDay = (d) => d.toISOString().slice(0, 10);

function getTargetDayFromReq(req) {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const day = url.searchParams.get("day");
    if (day && /^\d{4}-\d{2}-\d{2}$/.test(day)) {
      const [y, m, d] = day.split("-").map(Number);
      return new Date(Date.UTC(y, m - 1, d, 0, 0, 0));
    }
  } catch {}
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1, 0, 0, 0));
}

async function ensureTables(client) {
  // Main table
  await client.query(`
    CREATE SCHEMA IF NOT EXISTS update;
    CREATE TABLE IF NOT EXISTS update.lending_market_history (
      id                 SERIAL PRIMARY KEY,
      market_id          TEXT        NOT NULL,
      ts                 TIMESTAMPTZ NOT NULL,
      total_supply_usd   NUMERIC,
      total_borrow_usd   NUMERIC,
      debt_ceiling_usd   NUMERIC,
      apy_base_supply    NUMERIC,
      apy_reward_supply  NUMERIC,
      apy_base_borrow    NUMERIC,
      apy_reward_borrow  NUMERIC,
      created_at         TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  // Scrub table
  await client.query(`
    CREATE SCHEMA IF NOT EXISTS scrub;
    CREATE TABLE IF NOT EXISTS scrub.lending_market_scrub (
      id SERIAL PRIMARY KEY,
      market_id TEXT NOT NULL,
      ts TIMESTAMPTZ NOT NULL,
      total_supply_usd NUMERIC,
      total_borrow_usd NUMERIC,
      debt_ceiling_usd NUMERIC,
      apy_base_supply NUMERIC,
      apy_reward_supply NUMERIC,
      apy_base_borrow NUMERIC,
      apy_reward_borrow NUMERIC,
      validation_errors TEXT[],
      quality_score INTEGER,
      is_outlier BOOLEAN DEFAULT FALSE,
      outlier_reason TEXT,
      original_data JSONB,
      processed_at TIMESTAMPTZ DEFAULT NOW(),
      job_run_id TEXT,
      retry_count INTEGER DEFAULT 0
    );
  `);

  // Add UNIQUE constraint if missing
  await client.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uniq_market_ts'
          AND conrelid = 'update.lending_market_history'::regclass
      ) THEN
        ALTER TABLE update.lending_market_history
        ADD CONSTRAINT uniq_market_ts UNIQUE (market_id, ts);
      END IF;
    END$$;
  `);
}

function normalizeArray(json) {
  if (Array.isArray(json)) return json;
  if (json && Array.isArray(json.data)) return json.data;
  return [];
}

async function upsertCleanData(client, lendingData) {
  await client.query(`
    INSERT INTO update.lending_market_history (
      market_id, ts,
      total_supply_usd, total_borrow_usd, debt_ceiling_usd,
      apy_base_supply, apy_reward_supply, apy_base_borrow, apy_reward_borrow
    )
    VALUES ($1, to_timestamp($2), $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT (market_id, ts) DO UPDATE SET
      total_supply_usd   = EXCLUDED.total_supply_usd,
      total_borrow_usd   = EXCLUDED.total_borrow_usd,
      debt_ceiling_usd   = EXCLUDED.debt_ceiling_usd,
      apy_base_supply    = EXCLUDED.apy_base_supply,
      apy_reward_supply  = EXCLUDED.apy_reward_supply,
      apy_base_borrow    = EXCLUDED.apy_base_borrow,
      apy_reward_borrow  = EXCLUDED.apy_reward_borrow;
  `, [
    lendingData.market_id,
    lendingData.ts,
    lendingData.totalSupplyUsd ?? null,
    lendingData.totalBorrowUsd ?? null,
    lendingData.debtCeilingUsd ?? null,
    lendingData.apyBase ?? null,
    lendingData.apyReward ?? null,
    lendingData.apyBaseBorrow ?? null,
    lendingData.apyRewardBorrow ?? null,
  ]);
}

// ---------- handler ----------
module.exports = async (req, res) => {
  const jobRunId = generateJobRunId();
  const startTime = Date.now();
  
  // Quality metrics tracking
  let totalRecords = 0;
  let cleanRecords = 0;
  let scrubbedRecords = 0;
  let errorRecords = 0;
  let outlierRecords = 0;
  const errorSummary = {};

  const API_KEY = process.env.DEFILLAMA_API_KEY;
  if (!API_KEY) return res.status(500).json({ ok: false, error: "Missing DEFILLAMA_API_KEY" });

  // Load poollist.json from repo root
  let allIds = [];
  try {
    const poolPath = path.join(process.cwd(), "poollist.json");
    const raw = fs.readFileSync(poolPath, "utf8");
    const pools = JSON.parse(raw);
    allIds = pools.map((p) => p.market_id).filter(Boolean);
  } catch (e) {
    return res.status(500).json({ ok: false, error: `poollist.json not found or invalid: ${e.message}` });
  }

  // Chunking controls
  let limit = 400;
  let offset = parseInt(process.env.POOLS_OFFSET || "0", 10);
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    if (url.searchParams.get("limit"))  limit  = Math.max(1, Math.min(parseInt(url.searchParams.get("limit"), 10)  || 400, 2000));
    if (url.searchParams.get("offset")) offset = Math.max(0, parseInt(url.searchParams.get("offset"), 10) || 0);
  } catch {}
  if (process.env.MAX_POOLS_PER_RUN) {
    limit = Math.max(1, Math.min(parseInt(process.env.MAX_POOLS_PER_RUN, 10) || limit, 2000));
  }
  const ids = allIds.slice(offset, offset + limit);

  // Target day window (full UTC day)
  const target = getTargetDayFromReq(req);
  const start = new Date(target);
  const end = new Date(target); end.setUTCHours(23, 59, 59, 999);
  const startTs = toUnix(start);
  const endTs   = toUnix(end);
  const dayStr  = isoDay(target);

  const client = new Client({
    host: process.env.PGHOST,
    port: process.env.PGPORT,
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    ssl: process.env.PGSSLMODE ? { rejectUnauthorized: false } : undefined
  });

  let httpErrors = 0;

  try {
    await client.connect();
    await ensureTables(client);

    // Process each pool sequentially
    for (const market_id of ids) {
      const url = `https://pro-api.llama.fi/${API_KEY}/yields/chartLendBorrow/${market_id}`;
      const resp = await fetch(url);
      if (!resp.ok) { 
        httpErrors++; 
        errorRecords++;
        errorSummary['http_error'] = (errorSummary['http_error'] || 0) + 1;
        continue; 
      }
      
      const json = await resp.json();
      const arr = normalizeArray(json);

      // Filter to just the target day
      const rows = arr.filter((r) => {
        const t = Math.floor(new Date(r.timestamp).getTime() / 1000);
        return Number.isFinite(t) && t >= startTs && t <= endTs;
      });

      if (rows.length === 0) continue;

      await client.query("BEGIN");
      try {
        for (const r of rows) {
          totalRecords++;
          const ts = Math.floor(new Date(r.timestamp).getTime() / 1000);
          
          const lendingData = {
            market_id,
            ts,
            timestamp: ts,
            totalSupplyUsd: r.totalSupplyUsd ?? null,
            totalBorrowUsd: r.totalBorrowUsd ?? null,
            debtCeilingUsd: r.debtCeilingUsd ?? null,
            apyBase: r.apyBase ?? null,
            apyReward: r.apyReward ?? null,
            apyBaseBorrow: r.apyBaseBorrow ?? null,
            apyRewardBorrow: r.apyRewardBorrow ?? null,
          };

          // Validate the data
          const validation = validateLendingMarket(lendingData);
          
          if (validation.isValid) {
            // Clean data - goes to main table
            await upsertCleanData(client, lendingData);
            cleanRecords++;
          } else {
            // Invalid data - goes to scrub table
            await insertIntoScrubTable(
              client,
              'lending_market_scrub',
              lendingData,
              validation,
              jobRunId,
              r
            );
            
            scrubbedRecords++;
            
            // Track error types
            validation.errors.forEach(error => {
              errorSummary[error] = (errorSummary[error] || 0) + 1;
            });
            
            if (validation.isOutlier) {
              outlierRecords++;
            }
          }
        }
        await client.query("COMMIT");
      } catch (e) {
        await client.query("ROLLBACK");
        errorRecords += rows.length;
        errorSummary['database_error'] = (errorSummary['database_error'] || 0) + rows.length;
        console.error(`Database error for market ${market_id}:`, e.message);
      }
    }

    // Update quality summary
    await updateQualitySummary(client, 'job_lending', jobRunId, {
      totalRecords,
      cleanRecords,
      scrubbedRecords,
      errorRecords,
      outlierRecords,
      processingTimeMs: Date.now() - startTime,
      errorSummary
    });

    await client.end();
    
    return res.status(200).json({
      ok: true,
      job_run_id: jobRunId,
      day: dayStr,
      total_pools: allIds.length,
      offset,
      limit,
      pools_considered: ids.length,
      
      // Quality metrics
      total_records: totalRecords,
      clean_records: cleanRecords,
      scrubbed_records: scrubbedRecords,
      error_records: errorRecords,
      outlier_records: outlierRecords,
      overall_quality_score: totalRecords > 0 ? (cleanRecords / totalRecords) * 100 : 0,
      
      // Legacy metrics
      http_errors: httpErrors,
      ms: Date.now() - startTime,
      
      // Error summary
      error_summary: errorSummary
    });
  } catch (e) {
    try { 
      // Update quality summary with error
      await updateQualitySummary(client, 'job_lending', jobRunId, {
        totalRecords,
        cleanRecords,
        scrubbedRecords,
        errorRecords: errorRecords + 1,
        outlierRecords,
        processingTimeMs: Date.now() - startTime,
        errorSummary: { ...errorSummary, fatal_error: e.message }
      });
      await client.end(); 
    } catch {}
    return res.status(500).json({ 
      ok: false, 
      job_run_id: jobRunId,
      error: e.message 
    });
  }
};
