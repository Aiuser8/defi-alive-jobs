const { Client } = require("pg");

/**
 * Options:
 *  - ?full=1              → sync ALL rows from the view
 *  - ?since=YYYY-MM-DD    → sync rows with ts >= since
 *  - default (no params)  → sync "yesterday UTC" only (to avoid partial today)
 */

function isoDayUTC(d) { return d.toISOString().slice(0, 10); }

function getYesterdayUTCDateOnly() {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1, 0, 0, 0));
}

function buildWhereClause({ full, since, day }) {
  if (full) return ""; // no filter
  if (since) return `WHERE v.ts >= DATE '${since}'`;
  // default: yesterday only
  const d = isoDayUTC(day);
  return `WHERE v.ts = DATE '${d}'`;
}

async function ensureIndex(client) {
  await client.query(`
    CREATE SCHEMA IF NOT EXISTS update;

    -- Natural key for de-dupe / upsert
    CREATE UNIQUE INDEX IF NOT EXISTS uq_protocol_chain_tvl_daily_update
    ON update.protocol_chain_tvl_daily (protocol_id, chain, series_type, ts, symbol);
  `);
}

module.exports = async (req, res) => {
  const client = new Client({
    host: process.env.PGHOST,
    port: process.env.PGPORT,
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    ssl: process.env.PGSSLMODE ? { rejectUnauthorized: false } : undefined,
  });

  const t0 = Date.now();

  const full = req.query.full === "1";
  const since = req.query.since;
  const day = getYesterdayUTCDateOnly();

  try {
    await client.connect();

    const lockKey1 = 881234;
    const lockKey2 = 991337;
    const { rows: lockRows } = await client.query(
      `SELECT pg_try_advisory_lock($1, $2) AS got;`,
      [lockKey1, lockKey2]
    );
    if (!lockRows[0]?.got) {
      await client.end();
      return res.status(423).json({ ok: false, error: "Another tvl_sync is running (advisory lock held)" });
    }

    await ensureIndex(client);

    const whereSQL = buildWhereClause({ full, since, day });

    const sql = `
      WITH src AS (
        SELECT
          v.protocol_id,
          v.chain,
          v.ts,
          v.tvl_usd         AS total_liquidity_usd,
          v.inserted_at     AS ingest_time,
          v.protocol_name,
          v.category
        FROM clean.v_protocol_chain_tvl_enriched v
        ${whereSQL}
      ),
      up AS (
        INSERT INTO update.protocol_chain_tvl_daily (
          protocol_id, chain, series_type, ts,
          total_liquidity_usd, ingest_time, protocol_name, symbol, category
        )
        SELECT
          s.protocol_id,
          s.chain,
          'tvl'::text,
          s.ts,
          s.total_liquidity_usd,
          s.ingest_time,
          s.protocol_name,
          'TOTAL'::text,
          s.category
        FROM src s
        ON CONFLICT (protocol_id, chain, series_type, ts, symbol)
        DO UPDATE SET
          total_liquidity_usd = EXCLUDED.total_liquidity_usd,
          ingest_time         = GREATEST(update.protocol_chain_tvl_daily.ingest_time, EXCLUDED.ingest_time),
          protocol_name       = COALESCE(EXCLUDED.protocol_name, update.protocol_chain_tvl_daily.protocol_name),
          category            = COALESCE(EXCLUDED.category,      update.protocol_chain_tvl_daily.category)
        RETURNING 1
      )
      SELECT COUNT(*)::int AS affected FROM up;
    `;

    const { rows } = await client.query(sql);
    const affected = rows?.[0]?.affected ?? 0;

    await client.query(`SELECT pg_advisory_unlock($1, $2);`, [lockKey1, lockKey2]);
    await client.end();

    return res.status(200).json({
      ok: true,
      mode: full ? "full" : (since ? `since:${since}` : `yesterday:${isoDayUTC(day)}`),
      rows_affected: affected,
      ms: Date.now() - t0,
    });
  } catch (e) {
    try { await client.query(`SELECT pg_advisory_unlock(881234, 991337);`); } catch {}
    try { await client.end(); } catch {}
    return res.status(500).json({ ok: false, error: e.message });
  }
};
