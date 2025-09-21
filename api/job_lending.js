// api/job_lending.js
const fs = require("fs");
const path = require("path");
const { Client } = require("pg");

function toUnix(d) { return Math.floor(d.getTime() / 1000); }
function isoDay(d) { return d.toISOString().slice(0, 10); }
function getTargetDay() {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1, 0, 0, 0));
}

async function ensureTable(client) {
  await client.query(`
    CREATE SCHEMA IF NOT EXISTS update;
    CREATE TABLE IF NOT EXISTS update.lending_market_history (
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
      created_at TIMESTAMP DEFAULT NOW(),
      UNIQUE (market_id, ts)
    );
  `);
}

module.exports = async (req, res) => {
  const API_KEY = process.env.DEFILLAMA_API_KEY;
  if (!API_KEY) return res.status(500).json({ ok:false, error:"Missing DEFILLAMA_API_KEY" });

  const client = new Client({
    host: process.env.PGHOST,
    port: process.env.PGPORT,
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    ssl: process.env.PGSSLMODE ? { rejectUnauthorized: false } : undefined,
  });

  // load poollist.json that you committed with the repo (root of project)
  const poolPath = path.join(process.cwd(), "poollist.json");
  let pools;
  try {
    pools = JSON.parse(fs.readFileSync(poolPath, "utf8"));
  } catch (e) {
    return res.status(500).json({ ok:false, error:`poollist.json not found or invalid: ${e.message}` });
  }
  const ids = pools.map(p => p.market_id).filter(Boolean);

  const target = getTargetDay();
  const start = new Date(target);
  const end = new Date(target); end.setUTCHours(23,59,59,999);
  const startTs = toUnix(start);
  const endTs = toUnix(end);
  const dayStr = isoDay(target);

  const upsert = `
    INSERT INTO update.lending_market_history (
      market_id, ts, total_supply_usd, total_borrow_usd, debt_ceiling_usd,
      apy_base_supply, apy_reward_supply, apy_base_borrow, apy_reward_borrow
    )
    VALUES ($1, to_timestamp($2), $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT (market_id, ts) DO UPDATE SET
      total_supply_usd = EXCLUDED.total_supply_usd,
      total_borrow_usd = EXCLUDED.total_borrow_usd,
      debt_ceiling_usd = EXCLUDED.debt_ceiling_usd,
      apy_base_supply = EXCLUDED.apy_base_supply,
      apy_reward_supply = EXCLUDED.apy_reward_supply,
      apy_base_borrow = EXCLUDED.apy_base_borrow,
      apy_reward_borrow = EXCLUDED.apy_reward_borrow;
  `;

  const t0 = Date.now();
  let inserted = 0;
  let fetched = 0;

  try {
    await client.connect();
    await ensureTable(client);

    // ⚠️ Serverless functions have time limits. If you have ~1.8k pools,
    // consider batching. Here we hard-cap per run to first 400 pools to be safe.
    const MAX_PER_RUN = parseInt(process.env.MAX_POOLS_PER_RUN || "400", 10);
    const slice = ids.slice(0, MAX_PER_RUN);

    for (const market_id of slice) {
      const url = `https://pro-api.llama.fi/${API_KEY}/yields/chartLendBorrow/${market_id}`;
      const resp = await fetch(url);
      if (!resp.ok) continue;
      const json = await resp.json();

      const arr = Array.isArray(json) ? json
        : Array.isArray(json.data) ? json.data
        : [];

      const rows = arr.filter(r => {
        const t = Math.floor(new Date(r.timestamp).getTime() / 1000);
        return t >= startTs && t <= endTs;
      });

      fetched += rows.length;
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
        inserted++;
      }
    }

    await client.end();
    return res.status(200).json({
      ok: true,
      day: dayStr,
      pools_considered: slice.length,
      rows_fetched: fetched,
      rows_upserted: inserted,
      ms: Date.now() - t0,
    });
  } catch (e) {
    try { await client.end(); } catch {}
    return res.status(500).json({ ok:false, error: e.message });
  }
};