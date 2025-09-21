// alive_job/job_etf.js
// Pull exactly one full UTC day (yesterday by default) from DeFiLlama Pro
// and upsert into Supabase: update.raw_etf

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
  ssl: process.env.PGSSLMODE ? { rejectUnauthorized: false } : undefined
});

const toUnix = (d) => Math.floor(d.getTime() / 1000);
const isoDay = (d) => d.toISOString().slice(0,10);

// Determine the target day to ingest:
// - If TARGET_DAY=YYYY-MM-DD is set in .env, use that (manual re-run/backfill)
// - Otherwise use yesterday UTC (avoids partial “today” data)
function getTargetDay() {
  const override = process.env.TARGET_DAY; // e.g. 2025-09-21
  if (override) {
    const [y,m,d] = override.split("-").map(Number);
    return new Date(Date.UTC(y, m-1, d, 0, 0, 0));
  }
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1, 0, 0, 0));
}

async function ensureTable() {
  await client.query(`
    CREATE SCHEMA IF NOT EXISTS update;

    CREATE TABLE IF NOT EXISTS update.raw_etf (
      id              SERIAL PRIMARY KEY,
      gecko_id        TEXT NOT NULL,
      day             DATE NOT NULL,
      total_flow_usd  NUMERIC,
      created_at      TIMESTAMP DEFAULT NOW()
    );
  `);

  await client.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uniq_raw_gecko_day'
      ) THEN
        ALTER TABLE update.raw_etf
        ADD CONSTRAINT uniq_raw_gecko_day UNIQUE (gecko_id, day);
      END IF;
    END$$;
  `);
}

async function run() {
  const t0 = Date.now();
  await client.connect();
  await ensureTable();

  const target = getTargetDay();                // UTC midnight of target day
  const start = new Date(target);               // 00:00:00Z
  const end   = new Date(target); end.setUTCHours(23,59,59,999); // 23:59:59Z

  const startTs = toUnix(start);
  const endTs   = toUnix(end);
  const dayStr  = isoDay(target);

  const url = `https://pro-api.llama.fi/${API_KEY}/etfs/flows?start=${startTs}&end=${endTs}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API HTTP ${res.status} ${res.statusText}`);
  const data = await res.json();

  // Filter strictly to the target day
  const rows = Array.isArray(data) ? data.filter(r => r?.day && r.day.slice(0,10) === dayStr) : [];

  await client.query("BEGIN");
  const upsert = `
    INSERT INTO update.raw_etf (gecko_id, day, total_flow_usd)
    VALUES ($1, $2, $3)
    ON CONFLICT (gecko_id, day) DO UPDATE
      SET total_flow_usd = EXCLUDED.total_flow_usd
  `;

  let n = 0;
  for (const r of rows) {
    const gecko_id = r.gecko_id ?? null;
    const day = dayStr; // already normalized
    const total_flow_usd = r.total_flow_usd ?? null;
    if (!gecko_id) continue;
    await client.query(upsert, [gecko_id, day, total_flow_usd]);
    n++;
  }
  await client.query("COMMIT");

  console.log(`✅ ${dayStr}: upserted ${n} rows into update.raw_etf in ${Date.now()-t0}ms`);
  await client.end();
}

run().catch(async (e) => {
  console.error("❌ Job failed:", e.message);
  try { await client.query("ROLLBACK"); } catch {}
  try { await client.end(); } catch {}
  process.exit(1);
});