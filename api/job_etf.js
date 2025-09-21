// api/job_etf.js
const { Client } = require("pg");

const toUnix = (d) => Math.floor(d.getTime() / 1000);
const isoDay = (d) => d.toISOString().slice(0,10);

function getTargetDay() {
  const override = process.env.TARGET_DAY; // YYYY-MM-DD
  if (override) {
    const [y,m,d] = override.split("-").map(Number);
    return new Date(Date.UTC(y, m-1, d, 0, 0, 0));
  }
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1, 0, 0, 0));
}

async function runJobEtf() {
  const API_KEY = process.env.DEFILLAMA_API_KEY;
  if (!API_KEY) throw new Error("Missing DEFILLAMA_API_KEY");

  const client = new Client({
    host: process.env.PGHOST,
    port: process.env.PGPORT,
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    ssl: process.env.PGSSLMODE ? { rejectUnauthorized: false } : undefined
  });

  const t0 = Date.now();
  await client.connect();

  // ensure table & unique constraint
  await client.query(`
    CREATE SCHEMA IF NOT EXISTS update;
    CREATE TABLE IF NOT EXISTS update.raw_etf (
      id SERIAL PRIMARY KEY,
      gecko_id TEXT NOT NULL,
      day DATE NOT NULL,
      total_flow_usd NUMERIC,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);
  await client.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_raw_gecko_day') THEN
        ALTER TABLE update.raw_etf
        ADD CONSTRAINT uniq_raw_gecko_day UNIQUE (gecko_id, day);
      END IF;
    END$$;
  `);

  // target day window (UTC)
  const target = getTargetDay();
  const start = new Date(target);
  const end   = new Date(target); end.setUTCHours(23,59,59,999);
  const startTs = toUnix(start);
  const endTs   = toUnix(end);
  const dayStr  = isoDay(target);

  // fetch & filter
  const url = `https://pro-api.llama.fi/${API_KEY}/etfs/flows?start=${startTs}&end=${endTs}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API HTTP ${res.status} ${res.statusText}`);
  const data = await res.json();
  const rows = Array.isArray(data) ? data.filter(r => r?.day && r.day.slice(0,10) === dayStr) : [];

  // upsert
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
    const total_flow_usd = r.total_flow_usd ?? null;
    if (!gecko_id) continue;
    await client.query(upsert, [gecko_id, dayStr, total_flow_usd]);
    n++;
  }
  await client.query("COMMIT");
  await client.end();

  return { day: dayStr, inserted: n, ms: Date.now() - t0 };
}

module.exports = async (req, res) => {
  try {
    const result = await runJobEtf();
    res.status(200).json({ ok: true, ...result });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
};