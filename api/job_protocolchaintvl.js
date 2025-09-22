// api/job_protocolchaintvl.js
const { Client } = require("pg");
const fs = require("fs/promises");
const path = require("path");

const BASE = "https://pro-api.llama.fi/tvl";

// ----- helpers -----
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
function buildUrlFromSlug(slug, apiKey) {
  return `${BASE}/${encodeURIComponent(slug)}/${apiKey}`;
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
function normalizeRecord(r, defaults) {
  let tsUnix = null;
  let amount = null;
  let chain = null;

  if (Array.isArray(r) && r.length >= 2) {
    tsUnix = r[0];
    amount = r[1];
  } else if (r && typeof r === "object") {
    tsUnix = r.ts ?? r.date ?? r.timestamp ?? r.time ?? null;
    amount = r.tvl_usd ?? r.tvlUsd ?? r.tvl ?? r.total_liquidity_usd ?? null;
    chain = r.chain ?? r.chain_name ?? r.network ?? null;
  } else {
    return null;
  }

  if (!chain) return null; // require chain

  if (tsUnix == null || amount == null) return null;

  return {
    protocol_id: String(defaults.protocol_id),
    protocol_name: defaults.name ?? null,
    chain,
    day: unixToDay(tsUnix),
    total_liquidity_usd: Number(amount),
    ingest_time: r?.inserted_at ?? r?.ingest_time ?? null,
    category: r?.category ?? r?.type ?? null,
    series_type: "tvl",
    symbol: "TOTAL",
  };
}
async function loadList(listPathEnv) {
  const listPath = listPathEnv || path.join(process.cwd(), "protocolchaintvllist.json");
  const raw = await fs.readFile(listPath, "utf8");
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) throw new Error("protocolchaintvllist.json must be an array of { protocol_id, slug, name }");
  return parsed;
}

// ----- handler -----
module.exports = async (req, res) => {
  const API_KEY = process.env.DEFILLAMA_API_KEY;
  if (!API_KEY) return res.status(500).json({ ok: false, error: "Missing DEFILLAMA_API_KEY" });

  const LIST_PATH = process.env.TVL_LIST_PATH; // optional override for list file location

  // batching params
  const offset = parseInt(req.query.offset || "0", 10);
  const limit  = parseInt(req.query.limit  || "0", 10);

  // temporal params
  const full  = req.query.full === "1";
  const since = req.query.since; // YYYY-MM-DD
  const day   = getYesterdayUTCDateOnly();
  const keep  = wantDayPredicate({ full, since, day });

  const client = new Client({
    host: process.env.PGHOST,
    port: process.env.PGPORT,
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    ssl: process.env.PGSSLMODE ? { rejectUnauthorized: false } : undefined,
  });

  const t0 = Date.now();
  const overall = { protocols_total: 0, protocols_processed: 0, fetched: 0, considered: 0, inserted: 0, skipped: 0, invalid: 0, ms: 0 };
  const perProtocol = [];

  try {
    const list = await loadList(LIST_PATH);
    overall.protocols_total = list.length;

    // slice by offset/limit if provided
    const slice = (limit > 0) ? list.slice(offset, offset + limit) : list;
    overall.protocols_processed = slice.length;

    await client.connect();
    await ensureTarget(client);

    // lock to avoid overlap; add offset in key to allow separate batches to run safely if desired
    const lockKey1 = 881234;
    const lockKey2 = 991340 + (isNaN(offset) ? 0 : offset);
    const { rows: lockRows } = await client.query(`SELECT pg_try_advisory_lock($1, $2) AS got;`, [lockKey1, lockKey2]);
    if (!lockRows?.[0]?.got) {
      await client.end();
      return res.status(423).json({ ok: false, error: "Another job is running (advisory lock held)" });
    }

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

    await client.query("BEGIN");

    for (const item of slice) {
      const { protocol_id, slug, name } = item;
      const stats = { slug, protocol_id, name, fetched: 0, considered: 0, inserted: 0, skipped: 0, invalid: 0, error: null };

      try {
        if (!protocol_id || !slug) throw new Error("Missing protocol_id or slug in list entry");
        const url = buildUrlFromSlug(slug, API_KEY);

        const resp = await fetch(url);
        if (!resp.ok) throw new Error(`HTTP ${resp.status} ${resp.statusText}`);
        let data = await resp.json();

        // Some endpoints return { data: [...] }
        if (!Array.isArray(data) && Array.isArray(data?.data)) data = data.data;
        if (!Array.isArray(data)) throw new Error("Expected array or {data: array}");

        stats.fetched = data.length;
        overall.fetched += stats.fetched;

        for (const r of data) {
          const norm = normalizeRecord(r, { protocol_id, name });
          if (!norm) { stats.invalid++; overall.invalid++; continue; }
          if (!keep(norm.day)) { stats.skipped++; overall.skipped++; continue; }

          const params = [
            norm.protocol_id,
            norm.chain,
            norm.series_type,
            norm.day,
            norm.total_liquidity_usd,
            norm.ingest_time,
            norm.protocol_name,
            norm.symbol,
            norm.category
          ];

          await client.query(upsertSQL, params);
          stats.considered++; overall.considered++;
          stats.inserted++; overall.inserted++;
        }
      } catch (e) {
        stats.error = e.message;
      }

      perProtocol.push(stats);
    }

    await client.query("COMMIT");
    await client.query(`SELECT pg_advisory_unlock($1, $2);`, [lockKey1, lockKey2]);
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
    try {
      const lockKey1 = 881234;
      const lockKey2 = 991340 + (isNaN(offset) ? 0 : offset);
      await client.query(`SELECT pg_advisory_unlock($1, $2);`, [lockKey1, lockKey2]);
    } catch {}
    try { await client.end(); } catch {}
    return res.status(500).json({ ok: false, error: e.message });
  }
};