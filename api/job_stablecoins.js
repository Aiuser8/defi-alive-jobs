// api/job_stablecoins.js
const { Client } = require("pg");

function toUnix(d) { return Math.floor(d.getTime() / 1000); }
function isoDay(d) { return d.toISOString().slice(0, 10); }

function getTargetDay() {
  const now = new Date();
  // yesterday UTC to avoid partial today
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1, 0, 0, 0));
}

async function ensureTable(client) {
  await client.query(`
    CREATE SCHEMA IF NOT EXISTS update;
    CREATE TABLE IF NOT EXISTS update.stablecoin_mcap_by_peg_daily (
      id SERIAL PRIMARY KEY,
      day DATE NOT NULL,
      peg TEXT NOT NULL,
      amount_usd NUMERIC,
      ingest_time TIMESTAMPTZ DEFAULT now(),
      UNIQUE (day, peg)
    );
  `);
}

module.exports = async (req, res) => {
  const API_KEY = process.env.DEFILLAMA_API_KEY;
  if (!API_KEY) return res.status(500).json({ ok: false, error: "Missing DEFILLAMA_API_KEY" });

  const client = new Client({
    host: process.env.PGHOST,
    port: process.env.PGPORT,
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    ssl: process.env.PGSSLMODE ? { rejectUnauthorized: false } : undefined,
  });

  const target = getTargetDay();
  const dayStr = isoDay(target);
  const startTs = toUnix(target);
  const end = new Date(target); end.setUTCHours(23, 59, 59, 999);
  const endTs = toUnix(end);

  const url = `https://pro-api.llama.fi/${API_KEY}/stablecoins/stablecoincharts/all`;
  const t0 = Date.now();
  let inserted = 0;

  try {
    await client.connect();
    await ensureTable(client);

    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`API HTTP ${resp.status} ${resp.statusText}`);
    const arr = await resp.json();

    // Filter rows for exactly target day
    const rows = arr.filter(r => {
      const ts = parseInt(r.date, 10);
      return ts >= startTs && ts <= endTs;
    });

    await client.query("BEGIN");

    const upsert = `
      INSERT INTO update.stablecoin_mcap_by_peg_daily (day, peg, amount_usd)
      VALUES ($1, $2, $3)
      ON CONFLICT (day, peg) DO UPDATE
      SET amount_usd = EXCLUDED.amount_usd,
          ingest_time = now();
    `;

    for (const r of rows) {
      const ts = parseInt(r.date, 10);
      const day = isoDay(new Date(ts * 1000));

      for (const [peg, amount] of Object.entries(r.totalCirculatingUSD || {})) {
        await client.query(upsert, [day, peg, amount]);
        inserted++;
      }
    }

    await client.query("COMMIT");
    await client.end();

    return res.status(200).json({
      ok: true,
      day: dayStr,
      rows_upserted: inserted,
      ms: Date.now() - t0,
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    try { await client.end(); } catch {}
    return res.status(500).json({ ok: false, error: e.message });
  }
};
