// Vercel Serverless Function – daily “yesterday” backfill
import fs from "fs";
import path from "path";
import fetch from "node-fetch";
import pg from "pg";

const EVM_CHAINS = new Set([
  "ethereum","unichain","base","arbitrum","optimism","polygon","bsc","avalanche",
  "linea","scroll","blast","fantom","celo","gnosis","zksync era","metis","mantle","aurora"
]);

function isValidAddress(chain, address) {
  if (!address || typeof address !== "string") return false;
  const a = address.trim();
  if (EVM_CHAINS.has(chain)) return /^0x[0-9a-fA-F]{40}$/.test(a);
  // non-EVM: allow; API will be source of truth
  return /^[A-Za-z0-9:_-]{4,}$/.test(a);
}

function chunk(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

function yesterdayMidnightUtcUnix() {
  const now = new Date();
  const y = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate())); // today 00:00Z
  return Math.floor((y.getTime() - 24 * 60 * 60 * 1000) / 1000); // yesterday 00:00Z in seconds
}

async function fetchHistoricalForBatch(ts, coinIds, apiKey) {
  const coinsParam = encodeURIComponent(coinIds.join(","));
  const url = `https://pro-api.llama.fi/${apiKey}/coins/prices/historical/${ts}/${coinsParam}`;
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText}${text ? ` | ${text}` : ""}`);
  }
  return res.json(); // { coins: { "<id>": { symbol, price, decimals, confidence, timestamp } } }
}

async function upsertBatch(client, records) {
  // records: [{ coinId, symbol, confidence, decimals, tsSec, price }]
  for (const r of records) {
    const tsIso = new Date(r.tsSec * 1000).toISOString();
    await client.query(
      `INSERT INTO token_price_update (coin_id, symbol, confidence, decimals, price_timestamp, price_usd)
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

export default async function handler(req, res) {
  const {
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, DEFILLAMA_API_KEY
  } = process.env;

  if (!DEFILLAMA_API_KEY) {
    return res.status(500).json({ error: "Missing DEFILLAMA_API_KEY" });
  }

  const pool = new pg.Pool({
    host: PGHOST,
    port: PGPORT,
    database: PGDATABASE,
    user: PGUSER,
    password: PGPASSWORD,
    // optional: increase timeouts for large batches
    statement_timeout: 0,
    query_timeout: 0
  });

  const ts = yesterdayMidnightUtcUnix();

  // Load token_list.json from project root
  const filePath = path.join(process.cwd(), "token_list.json");
  const entries = JSON.parse(fs.readFileSync(filePath, "utf-8"));

  // Build coinIds
  const coinIds = [];
  let skipped = 0;
  for (const item of entries) {
    const chain = String(item.chain || "").trim().toLowerCase();
    const address = String(item.address || "").trim();
    if (!chain || !isValidAddress(chain, address)) {
      skipped++;
      continue;
    }
    const addrForSlug = EVM_CHAINS.has(chain) ? address.toLowerCase() : address; // keep case for non-EVM
    coinIds.push(`${chain}:${addrForSlug}`);
  }

  if (!coinIds.length) {
    await pool.end();
    return res.status(200).json({ message: "No valid coin IDs to process.", skipped });
  }

  const BATCH_SIZE = 25; // keep URL length safe
  let inserted = 0, failed = 0;

  try {
    const client = await pool.connect();
    try {
      for (const group of chunk(coinIds, BATCH_SIZE)) {
        try {
          const data = await fetchHistoricalForBatch(ts, group, DEFILLAMA_API_KEY);
          const nodes = data?.coins || {};
          const toUpsert = [];

          for (const id of group) {
            const node = nodes[id];
            if (node && typeof node.price === "number") {
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
          // whole batch failed
          failed += group.length;
        }
      }
    } finally {
      client.release();
    }
  } catch (e) {
    await pool.end();
    return res.status(500).json({ error: e.message, inserted, failed, skipped });
  }

  await pool.end();
  return res.status(200).json({
    date: new Date(ts * 1000).toISOString(),
    inserted,
    failed,
    skipped
  });
}