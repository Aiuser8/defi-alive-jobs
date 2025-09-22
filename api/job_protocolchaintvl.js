// api/job_protocolchaintvl.js
const { Client } = require("pg");
const fs = require("fs/promises");
const path = require("path");
const { performance } = require("perf_hooks");

// ---------- config helpers ----------
function llamaProtocolUrl(apiKey, idOrSlug) {
  return `https://pro-api.llama.fi/${apiKey}/api/protocol/${encodeURIComponent(idOrSlug)}`;
}
function fetchWithTimeout(url, msTimeout) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), msTimeout);
  return fetch(url, { signal: ctrl.signal }).finally(() => clearTimeout(t));
}

// ---------- time/log helpers ----------
function now() { return performance.now(); }
function ms(s, e) { return Math.round(e - s); }
function logPhase(tag, obj = {}) {
  // shows up in Vercel → Functions → Logs
  try { console.log(JSON.stringify({ tag, ...obj })); } catch {}
}

// ---------- date helpers ----------
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

// ---------- storage ----------
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
  const tRead = now();
  const raw = await fs.readFile(listPath, "utf8");
  logPhase("list.read", { path: listPath, ms: ms(tRead, now()) });
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) throw new Error("protocolchainlist.json must be an array");
  return parsed;
}

// Normalize TVL points from [ts, val] or {date,totalLiquidityUSD}
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
    const tsNum = Number(ts);
    const valNum = Number(val);
    if (!Number.isFinite(tsNum) || !Number.isFinite(valNum)) return null;
    return { day: unixToDay(tsNum), tvl: valNum };
  }
  return null;
}

module.exports = async (req, res) => {
  const API_KEY = process.env.DEFILLAMA_API_KEY;
  if (!API_KEY) return res.status(500).json({ ok: false, error: "Missing DEFILLAMA_API_KEY" });

  const LIST_PATH = process.env.TVL_LIST_PATH; // optional override

  // batching + time filters
  const offset = parseInt(req.query.offset || "0", 10);
  const limit  = parseInt(req.query.limit  || "20", 10); // default batch size = 20
  const full   = req.query.full === "1";
  const since  = req.query.since; // YYYY-MM-DD
  const day    = getYesterdayUTCDateOnly();
  const keep   = wantDayPredicate({ full, since, day });

  // knobs
  const RECENT_POINTS = parseInt(req.query.recent_points || "2", 10);             // default 2 (fast + small safety)
  const REQ_TIMEOUT_MS = parseInt(process.env.TVL_REQ_TIMEOUT_MS || "4000", 10);  // per fetch timeout (ms)

  const client = new Client({
    host: process.env.PGHOST,
    port: process.env.PGPORT,
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    ssl: process.env.PGSSLMODE ? { rejectUnauthorized: false } : undefined,
    connectionTimeoutMillis: 3000
  });

  const tStart = now();
  logPhase("job.start", { offset, limit, recent_points: RECENT_POINTS, full, since });

  const overall = { protocols_total: 0, protocols_processed: 0, considered: 0, inserted: 0, skipped: 0, invalid: 0, ms: 0 };
  const perProtocol = [];

  try {
    const list = await loadList(LIST_PATH);
    overall.protocols_total = list.length;
    const slice = list.slice(offset, offset + limit);
    overall.protocols_processed = slice.length;

    const tConn = now();
    await client.connect();
    logPhase("db.connected", { ms: ms(tConn, now()) });

    const tEnsure = now();
    await ensureTarget(client);
    logPhase("db.ensure_target", { ms: ms(tEnsure, now()) });

    // Prepare UPSERT once
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

    // No big BEGIN/COMMIT: each UPSERT is idempotent; avoids long transactions.
    // Optional: session-level statement timeout (protect against rare slow queries)
    try { await client.query(`SET statement_timeout = '5s';`); } catch {}

    // Process each protocol sequentially (batching controlled by limit)
    for (const item of slice) {
      const { protocol_id, protocol_name, category } = item;
      const tPStart = now();
      const stats = { protocol_id, protocol_name, chains: 0, points_checked: 0, considered: 0, inserted: 0, skipped: 0, invalid: 0, error: null };

      try {
        // --- FETCH ---
        const url = llamaProtocolUrl(API_KEY, protocol_id);
        const tFetch = now();
        const resp = await fetchWithTimeout(url, REQ_TIMEOUT_MS);
        const fetchMs = ms(tFetch, now());
        if (!resp.ok) throw new Error(`HTTP ${resp.status} ${resp.statusText}`);

        const tParse = now();
        const json = await resp.json();
        const parseMs = ms(tParse, now());
        logPhase("protocol.fetch", { protocol_id, url, fetch_ms: fetchMs, parse_ms: parseMs });

        // --- PROCESS CHAINS (check only last N points) ---
        const chainTvls = json?.chainTvls || json?.chain_tvls || {};
        const chainNames = Object.keys(chainTvls);
        stats.chains = chainNames.length;

        for (const chain of chainNames) {
          const series = chainTvls[chain]?.tvl || [];
          if (series.length === 0) continue;

          const tail = series.slice(-Math.max(1, RECENT_POINTS));
          for (const point of tail) {
            const tNorm = now();
            const norm = normalizeTvlPoint(point);
            const normMs = ms(tNorm, now());
            if (!norm) { stats.invalid++; overall.invalid++; continue; }
            stats.points_checked++;

            if (!keep(norm.day)) { stats.skipped++; overall.skipped++; continue; }

            const tIns = now();
            await client.query(upsertSQL, [
              String(protocol_id),
              chain,
              "tvl",
              norm.day,
              norm.tvl,
              null,                 // ingest_time → default now()
              protocol_name || null,
              "TOTAL",
              category || null
            ]);
            const insertMs = ms(tIns, now());

            stats.considered++; overall.considered++;
            stats.inserted++;  overall.inserted++;

            if (insertMs > 200) logPhase("db.upsert.slow", { protocol_id, chain, day: norm.day, insert_ms: insertMs });
            if (normMs > 30)    logPhase("normalize.slow",  { protocol_id, chain, norm_ms: normMs });
          }
        }
      } catch (e) {
        stats.error = e.message;
        logPhase("protocol.error", { protocol_id, error: e.message });
      } finally {
        logPhase("protocol.done", { protocol_id, ms: ms(tPStart, now()), stats });
        perProtocol.push(stats);
      }
    }

    try { await client.end(); } catch {}
    overall.ms = ms(tStart, now());
    logPhase("job.end", { total_ms: overall.ms, overall });

    return res.status(200).json({
      ok: true,
      mode: full ? "full" : (since ? `since:${since}` : `yesterday:${isoDayUTC(day)}`),
      batch: { offset, limit, recent_points: RECENT_POINTS, req_timeout_ms: REQ_TIMEOUT_MS },
      overall,
      perProtocol
    });
  } catch (e) {
    logPhase("job.error", { error: e.message });
    try { await client.end(); } catch {}
    return res.status(500).json({ ok: false, error: e.message });
  }
};