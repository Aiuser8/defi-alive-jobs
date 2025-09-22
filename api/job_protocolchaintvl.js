// api/job_protocolchaintvl.js
const { Client } = require("pg");
const fs = require("fs/promises");
const path = require("path");

function llamaProtocolUrl(apiKey, id) {
  return `https://pro-api.llama.fi/${apiKey}/api/protocol/${id}`;
}

function isoDayUTC(d) { return d.toISOString().slice(0, 10); }
function getYesterdayUTCDateOnly() {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1, 0, 0, 0));
}
function unixToDay(u) { return isoDayUTC(new Date(Number(u) * 1000)); }
function wantDayPredicate({ full, since, day }) {
  if (full) return () => true;
  if (since) return (recDay) => recDay >= since;
  const target = isoDayUTC(day);
  return (recDay) => recDay === target;
}

async function ensureTarget(client) {
  await client.query(`
    CREATE SCHEMA IF NOT EXISTS update;

    CREATE TABLE IF NOT EXISTS update.protocol_chain_tvl_daily (
      protocol_id         TEXT NOT NULL,
      chain               TEXT NOT NULL,
      series_type         TEXT NOT NULL,
      ts                  DATE NOT NULL,
      total_liquidity_usd NUMERIC NOT NULL,
      ingest_time         TIMESTAMPTZ NOT NULL DEFAULT now(),
      protocol_name       TEXT,
      symbol              TEXT,
      category            TEXT
    );

    CREATE UNIQUE INDEX IF NOT EXISTS uq_protocol_chain_tvl_daily_update
      ON update.protocol_chain_tvl_daily (protocol_id, chain, series_type, ts, symbol);
  `);
}

async function loadList(listPathEnv) {
  const listPath = listPathEnv || path.join(process.cwd(), "protocolchainlist.json");
  const raw = await fs.readFile(listPath, "utf8");
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) throw new Error("protocolchainlist.json must be an array");
  return parsed;
}

function normalizeTvlPoint(p) {
  if (Array.isArray(p) && p.length >= 2) {
    const ts = Number(p[0]);
    const val = Number(p[1]);
    if (!Number.isFinite(ts) || !Number.isFinite(val)) return null;
    return { day: unixToDay(ts), tvl: val };
  }
  if (p && typeof p === "object") {
    const ts = p.date ?? p.ts ?? p.timestamp;
    const val = p.totalLiquidityUSD ?? p.tvl_usd ?? p.tvlUsd ?? p.tvl;
    if (ts == null || val == null) return null;
    return { day: unixToDay(Number(ts)), tvl: Number(val) };
  }
  return null;
}

module.exports = async (req, res) => {
  const API_KEY = process.env.DEFILLAMA_API_KEY;
  if (!API_KEY) return res.status(500).json({ ok: false, error: "Missing DEFILLAMA_API_KEY" });

  const LIST_PATH = process.env.TVL_LIST_PATH;

  const offset = parseInt(req.query.offset || "0", 10);
  const limit  = parseInt(req.query.limit  || "20", 10); // default batch size = 20
  const full   = req.query.full === "1";
  const since  = req.query.since;
  const day    = getYesterdayUTCDateOnly();
  const keep   = wantDayPredicate({ full, since, day });

  const RECENT_POINTS = parseInt(req.query.recent_points || "1", 10);

  const client = new Client({
    host: process.env.PGHOST,
    port: process.env.PGPORT,
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    ssl: process.env.PGSSLMODE ? { rejectUnauthorized: false } : undefined,
  });

  const t0 = Date.now();
  const overall = { protocols_total: 0, protocols_processed: 0, considered: 0, inserted: 0, skipped: 0, invalid: 0, ms: 0 };
  const perProtocol = [];

  try {
    const list = await loadList(LIST_PATH);
    overall.protocols_total = list.length;
    const slice = list.slice(offset, offset + limit);
    overall.protocols_processed = slice.length;

    await client.connect();
    await ensureTarget(client);

    await client.query("BEGIN");

    const upsertSQL = `
      INSERT INTO update.protocol_chain_tvl_daily (
        protocol_id, chain, series_type, ts,
        total_liquidity_usd, ingest_time, protocol_name, symbol, category
      )
      VALUES ($1,$2,$3,$4,$5,COALESCE($6, now()),$7,$8,$9)
      ON CONFLICT (protocol_id, chain, series_type, ts, symbol)
      DO UPDATE SET
        total_liquidity_usd = EXCLUDED.total_liquidity_usd,
        ingest_time         = GREATEST(update.protocol_chain_tvl_daily.ingest_time, EXCLUDED.ingest_time),
        protocol_name       = COALESCE(EXCLUDED.protocol_name, update.protocol_chain_tvl_daily.protocol_name),
        category            = COALESCE(EXCLUDED.category,      update.protocol_chain_tvl_daily.category)
    `;

    for (const item of slice) {
      const { protocol_id, protocol_name, category } = item;
      const stats = { protocol_id, protocol_name, chains: 0, considered: 0, inserted: 0, skipped: 0, invalid: 0, error: null };

      try {
        const url = llamaProtocolUrl(API_KEY, protocol_id);
        const resp = await fetch(url);
        if (!resp.ok) throw new Error(`HTTP ${resp.status} ${resp.statusText}`);
        const json = await resp.json();

        const chainTvls = json?.chainTvls || {};
        const chainNames = Object.keys(chainTvls);
        stats.chains = chainNames.length;

        for (const chain of chainNames) {
          const series = chainTvls[chain]?.tvl || [];
          if (series.length === 0) continue;

          const tail = series.slice(-RECENT_POINTS);
          for (const point of tail) {
            const norm = normalizeTvlPoint(point);
            if (!norm) { stats.invalid++; overall.invalid++; continue; }
            if (!keep(norm.day)) { stats.skipped++; overall.skipped++; continue; }

            const params = [
              String(protocol_id),
              chain,
              "tvl",
              norm.day,
              norm.tvl,
              null,
              protocol_name,
              "TOTAL",
              category
            ];

            await client.query(upsertSQL, params);
            stats.considered++; overall.considered++;
            stats.inserted++;  overall.inserted++;
          }
        }
      } catch (e) {
        stats.error = e.message;
      }
      perProtocol.push(stats);
    }

    await client.query("COMMIT");
    await client.end();

    overall.ms = Date.now() - t0;
    return res.status(200).json({
      ok: true,
      mode: full ? "full" : (since ? `since:${since}` : `yesterday:${isoDayUTC(day)}`),
      batch: { offset, limit },
      overall,
      perProtocol
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    try { await client.end(); } catch {}
    return res.status(500).json({ ok: false, error: e.message });
  }
};


