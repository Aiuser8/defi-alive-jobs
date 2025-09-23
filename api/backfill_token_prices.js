// Force Node runtime (not Edge)
module.exports.config = { runtime: 'nodejs18.x' };

// CommonJS + dotenv (works locally; Vercel env vars are provided automatically)
require('dotenv').config();

const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

// ---- config ----
const EVM_CHAINS = new Set([
  'ethereum','unichain','base','arbitrum','optimism','polygon','bsc','avalanche',
  'linea','scroll','blast','fantom','celo','gnosis','zksync era','metis','mantle','aurora'
]);

function isValidAddress(chain, address) {
  if (!address || typeof address !== 'string') return false;
  const a = address.trim();
  if (EVM_CHAINS.has(chain)) return /^0x[0-9a-fA-F]{40}$/.test(a);
  // non-EVM: allow and let API decide (basic sanity)
  return /^[A-Za-z0-9:_-]{4,}$/.test(a);
}

function chunk(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

function yesterdayMidnightUtcUnix() {
  const now = new Date();
  const todayMidnight = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  return Math.floor((todayMidnight - 24 * 60 * 60 * 1000) / 1000);
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

module.exports = async (req, res) => {
  try {
    const { SUPABASE_DB_URL, DEFILLAMA_API_KEY } = process.env;
    if (!SUPABASE_DB_URL) return res.status(500).json({ error: 'Missing SUPABASE_DB_URL' });
    if (!DEFILLAMA_API_KEY) return res.status(500).json({ error: 'Missing DEFILLAMA_API_KEY' });

    // Parse offset/limit (defaults)
    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const offset = Math.max(0, parseInt(url.searchParams.get('offset') || '0', 10));
    const limit  = Math.max(1, Math.min(2000, parseInt(url.searchParams.get('limit') || '500', 10)));

    // DB pool (use pooled connection string from Supabase; includes sslmode=require)
    const pool = new Pool({
      connectionString: SUPABASE_DB_URL,
      ssl: { rejectUnauthorized: false },
      statement_timeout: 0,
      query_timeout: 0
    });

    // Load and slice token list (expect file at project root)
    const filePath = path.join(process.cwd(), 'token_list.json');
    const entries = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
    const slice = entries.slice(offset, offset + limit);

    if (!slice.length) {
      await pool.end();
      return res.status(200).json({ message: 'No tokens in this slice.', offset, limit });
    }

    // Build coin ids
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
      await pool.end();
      return res.status(200).json({ message: 'All tokens in slice invalid.', offset, limit, skipped });
    }

    // Run for yesterday 00:00 UTC
    const ts = yesterdayMidnightUtcUnix();
    const BATCH_SIZE = 25;  // sub-batch for URL length / rate limits
    const SLEEP_MS = 120;
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));

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
    }

    await pool.end();
    return res.status(200).json({
      date: new Date(ts * 1000).toISOString(),
      offset, limit,
      inserted, failed, skipped
    });
  } catch (e) {
    // make sure we always return JSON on failure
    return res.status(500).json({ error: e.message, stack: e.stack });
  }
};