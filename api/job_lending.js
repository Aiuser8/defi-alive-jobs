// api/job_lending.js
// Fetch exactly one full UTC day of lending data (yesterday by default)
// from DeFiLlama Pro for a slice of pools, and upsert into
// Supabase/Postgres table: update.lending_market_history
//
// Supports chunking via:
//   - query params:  ?limit=400&offset=800
//   - env vars:      MAX_POOLS_PER_RUN=400, POOLS_OFFSET=0
//
// Optional test override of the day:
//   - query param:   ?day=YYYY-MM-DD
//
// Requires env: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE=require, DEFILLAMA_API_KEY

const fs = require("fs");
const path = require("path");
const { Client } = require("pg");

// ---------- helpers ----------
const toUnix = (d) => Math.floor(d.getTime() / 1000);
const isoDay = (d) => d.toISOString().slice(0, 10);

function getTargetDayFromReq(req) {
  // allow ?day=YYYY-MM-DD to override (handy for manual tests)
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const day = url.searchParams.get("day");
    if (day && /^\d{4}-\d{2}-\d{2}$/.test(day)) {
      const [y, m, d] = day.split("-").map(Number);
      return new Date(Date.UTC(y, m - 1, d, 0, 0, 0));
    }
  } catch {}
  // default: yesterday UTC (avoid partial "today")
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1, 0, 0, 0));
}

async function ensureTable(client) {
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

  // add UNIQUE(market_id, ts) if missing
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

// ---------- handler ----------
module.exports = async (req, res) => {
  const API_KEY = process.env.DEFILLAMA_API_KEY;
  if (!API_KEY) return res.status(500).json({ ok: false, error: "Missing DEFILLAMA_API_KEY" });

  // Load poollist.json from repo root (committed file)
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

  const upsert = `
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
  `;

  const t0 = Date.now();
  let rowsFetched = 0;
  let rowsUpserted = 0;
  let httpErrors = 0;

  try {
    await client.connect();
    await ensureTable(client);

    // Process each pool sequentially (keeps within serverless limits)
    for (const market_id of ids) {
      const url = `https://pro-api.llama.fi/${API_KEY}/yields/chartLendBorrow/${market_id}`;
      const resp = await fetch(url);
      if (!resp.ok) { httpErrors++; continue; }
      const json = await resp.json();
      const arr = normalizeArray(json);

      // filter to just the target day
      const rows = arr.filter((r) => {
        const t = Math.floor(new Date(r.timestamp).getTime() / 1000);
        return Number.isFinite(t) && t >= startTs && t <= endTs;
      });

      if (rows.length === 0) continue;

      await client.query("BEGIN");
      try {
        for (const r of rows) {
          const ts = Math.floor(new Date(r.timestamp).getTime() / 1000);
          await client.query(upsert, [
            market_id,
            ts,
            r.totalSupplyUsd ?? null,
            r.totalBorrowUsd ?? null,
            r.debtCeilingUsd ?? null,
            r.apyBase ?? null,
            r.apyReward ?? null,
            r.apyBaseBorrow ?? null,
            r.apyRewardBorrow ?? null,
          ]);
          rowsUpserted++;
        }
        await client.query("COMMIT");
        rowsFetched += rows.length;
      } catch (e) {
        await client.query("ROLLBACK");
        // continue to next pool on error
      }
    }

    await client.end();
    return res.status(200).json({
      ok: true,
      day: dayStr,
      total_pools: allIds.length,
      offset,
      limit,
      pools_considered: ids.length,
      rows_fetched: rowsFetched,
      rows_upserted: rowsUpserted,
      http_errors: httpErrors,
      ms: Date.now() - t0
    });
  } catch (e) {
    try { await client.end(); } catch {}
    return res.status(500).json({ ok: false, error: e.message });
  }
};