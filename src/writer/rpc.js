/**
 * writer/rpc.js — RPC pool, sendWithRetry, priority fees.
 * Copied from worker/src/rpc.js — import path fixed to ../config.js.
 */

import { env } from '../config.js';

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Read RPC ──────────────────────────────────────────────────────────────────

function getPrimaryRpcUrl() {
  const secureRpc = env('HELIUS_SECURE_RPC');
  if (secureRpc) return secureRpc;
  const key = env('HELIUS_API_KEY');
  return key
    ? `https://mainnet.helius-rpc.com/?api-key=${key}`
    : 'https://api.mainnet-beta.solana.com';
}

/**
 * Read-only RPC calls with cascading fallback + retry.
 * Secure RPC -> standard Helius -> beta Helius -> public Solana.
 */
export async function rpcCall(method, params) {
  const urls = [getPrimaryRpcUrl()];
  const key = env('HELIUS_API_KEY');
  if (key) {
    const std = `https://mainnet.helius-rpc.com/?api-key=${key}`;
    const beta = `https://beta.helius-rpc.com/?api-key=${key}`;
    if (std !== urls[0]) urls.push(std);
    if (beta !== urls[0]) urls.push(beta);
  }
  urls.push('https://api.mainnet-beta.solana.com');

  let lastErr;
  const errors = [];
  for (let round = 0; round < 2; round++) {
    if (round > 0) await sleep(2000 * round);
    for (const url of urls) {
      try {
        const resp = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
        });
        if (!resp.ok) {
          const text = await resp.text().catch(() => '');
          throw new Error(`HTTP ${resp.status} — ${text.slice(0, 80)}`);
        }
        const data = await resp.json();
        if (data.error) throw new Error(JSON.stringify(data.error).slice(0, 100));
        return data.result;
      } catch (err) {
        const safeUrl = url.replace(/api-key=[^&]+/, 'api-key=***');
        errors.push(`${safeUrl}: ${err.message.slice(0, 60)}`);
        lastErr = err;
      }
    }
  }
  console.error(`[rpcCall] ${method} failed all ${errors.length} attempts:\n${errors.join('\n')}`);
  throw lastErr;
}

// ── Send RPC ──────────────────────────────────────────────────────────────────

export function buildSendPool() {
  const pool = [];
  const key = env('HELIUS_API_KEY');
  // NOTE: beta.helius-rpc.com returns 401 on sendTransaction — reads only!
  if (key) pool.push(`https://mainnet.helius-rpc.com/?api-key=${key}`);
  if (env('SHYFT_API_KEY'))
    pool.push(`https://rpc.shyft.to?api_key=${env('SHYFT_API_KEY')}`);
  if (env('CHAINSTACK_SOLANA_URL'))
    pool.push(env('CHAINSTACK_SOLANA_URL'));
  if (pool.length === 0)
    pool.push('https://api.mainnet-beta.solana.com');
  return pool;
}

const SEND_RPC_POOL = buildSendPool();
let sendPoolIndex = 0;
function getNextSendRpcUrl() {
  return SEND_RPC_POOL[sendPoolIndex++ % SEND_RPC_POOL.length];
}

async function rpcSend(encodedTx) {
  const url = getNextSendRpcUrl();
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'sendTransaction', params: [encodedTx, {
      encoding: 'base64', skipPreflight: true, maxRetries: 0,
    }] }),
  });
  if (resp.status === 429) throw new Error('RPC sendTransaction: ratelimited (HTTP 429)');
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`RPC sendTransaction: HTTP ${resp.status} — ${text.slice(0, 100)}`);
  }
  const data = await resp.json();
  if (data.error) throw new Error(`RPC sendTransaction: ${JSON.stringify(data.error)}`);
  return data.result;
}

export async function sendWithRetry(encoded) {
  const MAX_ATTEMPTS = 8;
  for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
    try {
      return await rpcSend(encoded);
    } catch (err) {
      if (attempt >= MAX_ATTEMPTS - 1) throw err;
      const isRate = err.message.includes('-32429') || err.message.includes('429') || err.message.toLowerCase().includes('ratelimit');
      const isAuthErr = err.message.includes('401') || err.message.includes('403') || err.message.toLowerCase().includes('unauthorized');
      const isTransient = isRate || isAuthErr
        || err.message.includes('blockhash')
        || err.message.includes('-32002')
        || err.message.includes('timeout')
        || err.message.includes('ECONNRESET')
        || err.message.includes('fetch');
      const delay = isRate ? Math.min(1000 * Math.pow(2, attempt), 20000) : isAuthErr ? 100 : 500 * (attempt + 1);
      if (isTransient) await sleep(delay);
      else throw err;
    }
  }
}

export async function fetchPriorityFee() {
  try {
    const fees = await rpcCall('getRecentPrioritizationFees', [[]]);
    if (!fees?.length) return 10_000;
    const sorted = fees.map(f => f.prioritizationFee).sort((a, b) => a - b);
    return Math.max(1_000, Math.min(500_000, sorted[Math.floor(sorted.length * 0.75)] || 0));
  } catch { return 10_000; }
}
