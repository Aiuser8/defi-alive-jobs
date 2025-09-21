// alive_job/job_lending.js
// Pull exactly one full UTC day (yesterday) for all lending pools
// and upsert into Supabase: update.lending_market_history

const fs = require("fs");
const { Client } = require("pg");
require("dotenv").config();

const API_KEY = process.env.DEFILLAMA_API_KEY;
if (!API_KEY) {
  console.error("Missing DEFILLAMA_API_KEY in .env");
  process.exit(1);
}

const client = new Client({
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASE,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  ssl: process.env.PGSSLMODE ? { rejectUnauthorized: false } : undefined,
});

// Helpers
const toUnix = (d) => Math.floor(d.getTime() / 1000);
const isoDay = (d) => d.toISOString().slice(0, 10);

// Yesterday’s UTC day
function getTargetDay() {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1, 0, 0, 0));
}

async function ensureTable() {
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
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await client.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uniq_market_ts'
      ) THEN
        ALTER TABLE update.lending_market_history
        ADD CONSTRAINT uniq_market_ts UNIQUE (market_id, ts);
      END IF;
    END$$;
  `);
}

async function run() {
  const t0 = Date.now();
  await client.connect();
  await ensureTable();

  // Load poollist.json (list of market_ids you already have in clean)
  const poollist = JSON.parse(fs.readFileSync("poollist.json", "utf-8"));
  console.log(`📦 Loaded ${poollist.length} market_ids from poollist.json`);

  const target = getTargetDay();
  const start = new Date(target);
  const end = new Date(target); end.setUTCHours(23, 59, 59, 999);

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

  let inserted = 0;
  for (let i = 0; i < poollist.length; i++) {
    const { market_id } = poollist[i];
    const url = `https://pro-api.llama.fi/${API_KEY}/yields/chartLendBorrow/${market_id}`;

    try {
      const res = await fetch(url);
      if (!res.ok) {
        console.warn(`⚠️  ${i + 1}/${poollist.length} ${market_id}: HTTP ${res.status}`);
        continue;
      }
      const data = await res.json();

      const rows = Array.isArray(data)
        ? data.filter(r => {
            const t = Math.floor(new Date(r.timestamp).getTime() / 1000);
            return t >= startTs && t <= endTs;
          })
        : [];

      await client.query("BEGIN");
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
      await client.query("COMMIT");
      console.log(`✅ ${i + 1}/${poollist.length} ${market_id}: upserted ${rows.length} rows`);
    } catch (e) {
      await client.query("ROLLBACK").catch(() => {});
      console.warn(`⚠️  ${i + 1}/${poollist.length} ${market_id}: ${e.message}`);
    }
  }

  console.log(`🎯 Done. ${inserted} rows upserted for ${dayStr} in ${Date.now() - t0}ms`);
  await client.end();
}

run().catch(async (e) => {
  console.error("❌ Job failed:", e.message);
  try { await client.query("ROLLBACK"); } catch {}
  try { await client.end(); } catch {}
  process.exit(1);
});