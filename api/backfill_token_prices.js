// api/backfill_token_prices.js (CommonJS)
// Force Node runtime (pg not supported on Edge)
module.exports.config = { runtime: 'nodejs18.x' };

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

const EVM_CHAINS = new Set([
  'ethereum','unichain','base','arbitrum','optimism','polygon','bsc','avalanche',
  'linea','scroll','blast','fantom','celo','gnosis','zksync era','metis','mantle','aurora'
]);

function isValidAddress(chain, address) {
  if (!address || typeof address !== 'string') return false;
  const a = address.trim();
  if (EVM_CHAINS.has(chain)) return /^0x[0-9a-fA-F]{40}$/.test(a);
  return /^[A-Za-z0-9:_-]{4,}$/.test(a); // non-EVM: basic sanity, let API decide
}

function chunk(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

function yesterdayMidnightUtcUnix() {
  const now = new Date();
  const todayMidnight = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  return Math.floor((todayMidnight - 24*60*60*1000) / 1000);
}

async function fetchHistoricalForBatch(ts, coinIds, apiKey) {
  const coinsParam = encodeURIComponent(coinIds.join(','));
  const url = `https://pro-api.llama.fi/${apiKey}/coins/prices/historical/${ts}/${coinsParam}`;
  const res = await fetch(url); // Node 18+ global fetch
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText}${text ? ` | ${text}` : ''}`);
  }
  return res.json(); // { coins: { "<id>": { symbol, price, decimals, confidence, timestamp } } }
}

async function upsertBatch(client, records) {
  for (const r of records) {
    const tsIso = new Date(r.tsSec * 1000).toISOString();
    await client.query(
      `INSERT INTO "update".token_price_daily
         (coin_id, symbol, confidence, decimals, price_timestamp, price_usd)
       VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT (coin_id, price_timestamp)
       DO UPDATE SET
         price_usd  = EXCLUDED.price_usd,
         confidence = EXCLUDED.confidence,
         decimals   = EXCLUDED.decimals,
         symbol     = EXCLUDED.symbol`,
      [r.coinId, r.symbol, r.confidence, r.decimals, tsIso, r.price]
    );
  }
}

// Build a pg Pool from SUPABASE_DB_URL or PG* parts
function makePoolFromEnv() {
  const { SUPABASE_DB_URL } = process.env;
  if (SUPABASE_DB_URL) {
    return new Pool({
      connectionString: SUPABASE_DB_URL,
      ssl: { rejectUnauthorized: false },
      statement_timeout: 0,
      query_timeout: 0
    });
  }
  const {
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE
  } = process.env;

  if (!PGHOST || !PGPORT || !PGDATABASE || !PGUSER || !PGPASSWORD) {
    throw new Error('Missing DB env: need SUPABASE_DB_URL or PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD');
  }

  // Supabase pooler host usually requires port 6543. You currently have 5432.
  // If PGHOST looks like "*.pooler.supabase.com" and PGPORT is 5432, consider switching to 6543.
  const sslRequired = (PGSSLMODE || '').toLowerCase() === 'require';
  return new Pool({
    host: PGHOST,
    port: Number(PGPORT),  // For pooler use 6543; direct is 5432 (pooler recommended for serverless)
    database: PGDATABASE,
    user: PGUSER,
    password: PGPASSWORD,
    ssl: sslRequired ? { rejectUnauthorized: false } : undefined,
    statement_timeout: 0,
    query_timeout: 0
  });
}

module.exports = async (req, res) => {
  try {
    const { DEFILLAMA_API_KEY } = process.env;
    if (!DEFILLAMA_API_KEY) {
      return res.status(500).json({ error: 'Missing DEFILLAMA_API_KEY' });
    }

    // Parse offset/limit
    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const offset = Math.max(0, parseInt(url.searchParams.get('offset') || '0', 10));
    const limit  = Math.max(1, Math.min(2000, parseInt(url.searchParams.get('limit') || '500', 10)));

    // Load token list (at repo root)
    const filePath = path.join(process.cwd(), 'token_list.json');
    const entries = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
    const slice = entries.slice(offset, offset + limit);

    if (!slice.length) {
      return res.status(200).json({ message: 'No tokens in this slice.', offset, limit });
    }

    // Build coin IDs
    const coinIds = [];
    let skipped = 0;
    for (const item of slice) {
      const chain = String(item.chain || '').trim().toLowerCase();
      const address = String(item.address || '').trim();
      if (!chain || !isValidAddress(chain, address)) { skipped++; continue; }
      const addrForSlug = EVM_CHAINS.has(chain) ? address.toLowerCase() : address; // preserve case for non-EVM
      coinIds.push(`${chain}:${addrForSlug}`);
    }
    if (!coinIds.length) {
      return res.status(200).json({ message: 'All tokens in slice invalid.', offset, limit, skipped });
    }

    const ts = yesterdayMidnightUtcUnix();
    const BATCH_SIZE = 25;
    const SLEEP_MS = 120;
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));

    const pool = makePoolFromEnv();
    let inserted = 0, failed = 0;

    const client = await pool.connect();
    try {
      for (const group of chunk(coinIds, BATCH_SIZE)) {
        try {
          const data = await fetchHistoricalForBatch(ts, group, DEFILLAMA_API_KEY);
          const nodes = data?.coins || {};
          const toUpsert = [];

          for (const id of group) {
            const node = nodes[id];
            if (node && typeof node.price === 'number') {
              const tsSec = Number.isFinite(node.timestamp) ? node.timestamp : ts;
              toUpsert.push({
                coinId: id,
                symbol: node.symbol ?? null,
                confidence: node.confidence ?? null,
                decimals: node.decimals ?? null,
                tsSec,
                price: node.price
              });
            } else {
              failed++;
            }
          }

          if (toUpsert.length) {
            await upsertBatch(client, toUpsert);
            inserted += toUpsert.length;
          }
        } catch (e) {
          // whole group failed
          failed += group.length;
        }
        await sleep(SLEEP_MS);
      }
    } finally {
      client.release();
      await pool.end();
    }

    return res.status(200).json({
      date: new Date(ts * 1000).toISOString(),
      offset, limit,
      inserted, failed, skipped
    });
  } catch (e) {
    return res.status(500).json({ error: e.message, stack: e.stack });
  }
};