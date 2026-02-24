/**
 * indexer.js — Chain scanner for Freeze Dry pointer memos
 *
 * Scans Solana for FREEZEDRY: pointer memos from the configured SERVER_WALLET.
 * Default wallet is the official Freeze Dry inscriber, so new nodes auto-discover
 * all public Freeze Dry content out of the box. Change SERVER_WALLET in .env to
 * index a different inscriber's content.
 *
 * Helius plan auto-detection (no config needed):
 *   Paid key (Developer+) → Enhanced API (~50x cheaper, faster)
 *   Free key             → Standard RPC (getSignaturesForAddress + getTransaction)
 *   Override: USE_ENHANCED_API=true|false in .env
 */

import * as db from './db.js';

// Env vars read lazily — loadEnv() in server.js must run first
let HELIUS_API_KEY, HELIUS_RPC, SERVER_WALLET, REGISTRY_URL, POLL_INTERVAL;

// Auto-detected: true = paid key with Enhanced API, false = free key (RPC only)
let useEnhancedAPI = null; // null = not yet detected

function loadConfig() {
  HELIUS_API_KEY = process.env.HELIUS_API_KEY || '';
  HELIUS_RPC = process.env.HELIUS_RPC_URL || `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
  SERVER_WALLET = process.env.SERVER_WALLET || '6ao3hnvKQJfmQ94xTMV34uLUP6azVNHzCfip1ic5Nafj';
  REGISTRY_URL = process.env.REGISTRY_URL || '';
  POLL_INTERVAL = parseInt(process.env.POLL_INTERVAL || '120000', 10);
  // Allow explicit override via env var
  if (process.env.USE_ENHANCED_API === 'true') useEnhancedAPI = true;
  if (process.env.USE_ENHANCED_API === 'false') useEnhancedAPI = false;
}

// Rate limiting: 3 concurrent, 500ms stagger
const MAX_CONCURRENT = 3;
const STAGGER_MS = 500;

const MEMO_PROGRAM = 'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr';

let log = console;
let lastSignature = null; // pagination cursor

/**
 * Parse a FREEZEDRY pointer memo string into structured data.
 * Supports v3 (with lastChunkSig), v2, and v1 formats.
 */
function parsePointerMemo(memo) {
  if (!memo || typeof memo !== 'string' || !memo.startsWith('FREEZEDRY:')) return null;
  const parts = memo.split(':');
  // v3: FREEZEDRY:3:sha256:{hex}:{chunks}:{size}:{chunkSize}:{flags}:{inscriber}:{lastChunkSig}
  if (parts[1] === '3' && parts.length >= 10) {
    return {
      version: 3,
      hash: parts[2] + ':' + parts[3],
      chunkCount: parseInt(parts[4], 10),
      blobSize: parseInt(parts[5], 10),
      chunkSize: parseInt(parts[6], 10),
      flags: parts[7],
      inscriber: parts[8],
      lastChunkSig: parts[9],
    };
  }
  // v2: FREEZEDRY:2:sha256:{hex}:{chunks}:{size}:{chunkSize}:{flags}:{inscriber}
  if (parts[1] === '2' && parts.length >= 9) {
    return {
      version: 2,
      hash: parts[2] + ':' + parts[3],
      chunkCount: parseInt(parts[4], 10),
      blobSize: parseInt(parts[5], 10),
      chunkSize: parseInt(parts[6], 10),
      flags: parts[7],
      inscriber: parts[8],
      lastChunkSig: null,
    };
  }
  // v1: FREEZEDRY:sha256:{hex}:{chunks}:{size?}
  if (parts.length >= 4) {
    return {
      version: 1,
      hash: parts[1] + ':' + parts[2],
      chunkCount: parseInt(parts[3], 10),
      blobSize: parts[4] ? parseInt(parts[4], 10) : null,
      chunkSize: null,
      flags: null,
      inscriber: null,
      lastChunkSig: null,
    };
  }
  return null;
}

/** Strip v3 chunk header (FD:{hash8}:{index}:) from memo data. No-op for v2. */
function stripV3Header(str) {
  if (str.startsWith('FD:')) {
    const thirdColon = str.indexOf(':', str.indexOf(':', 3) + 1);
    if (thirdColon !== -1) return str.slice(thirdColon + 1);
  }
  return str;
}

/**
 * Start the indexer loop
 */
export function startIndexer(logger) {
  if (logger) log = logger;
  loadConfig();

  if (!HELIUS_API_KEY) {
    log.warn('HELIUS_API_KEY not set — indexer disabled (webhook-only mode)');
    return;
  }

  log.info(`Indexer: starting (poll every ${POLL_INTERVAL / 1000}s, wallet: ${SERVER_WALLET.slice(0, 8)}...)`);

  // Seed from registry on startup (backfill)
  if (REGISTRY_URL) {
    seedFromRegistry().catch(err => log.warn(`Registry seed failed: ${err.message}`));
  }

  // Start polling
  pollLoop();
}

async function pollLoop() {
  while (true) {
    try {
      await scanForPointerMemos();
      await fillIncomplete();
    } catch (err) {
      log.warn(`Indexer poll error: ${err.message}`);
    }
    await sleep(POLL_INTERVAL);
  }
}

// ─── Scanning: discover pointer memos ───

async function scanForPointerMemos() {
  if (useEnhancedAPI !== false) {
    try {
      await scanEnhanced();
      if (useEnhancedAPI === null) {
        useEnhancedAPI = true;
        log.info('Indexer: Enhanced API available — using optimized path');
      }
      return;
    } catch (err) {
      if (err.message.includes('403') || err.message.includes('401') || err.message.includes('Forbidden')) {
        useEnhancedAPI = false;
        log.info('Indexer: Enhanced API not available (free plan) — using standard RPC');
      } else {
        throw err; // re-throw non-auth errors
      }
    }
  }
  // Fallback: standard RPC
  await scanRPC();
}

/** Scan via Enhanced Transactions API (paid plans — 100 credits per 100 results) */
async function scanEnhanced() {
  const txs = await fetchEnhancedTransactions(SERVER_WALLET, lastSignature);
  if (!txs || txs.length === 0) return;

  log.info(`Indexer: scanning ${txs.length} transactions via Enhanced API`);

  for (const tx of txs) {
    if (tx.transactionError) continue;
    const memoData = extractEnhancedMemoData(tx);
    if (!memoData) continue;

    processPointerMemo(memoData, tx.signature);
  }

  lastSignature = txs[txs.length - 1].signature;
}

/** Scan via standard RPC (free plan — getSignaturesForAddress + getTransaction) */
async function scanRPC() {
  const params = { limit: 50 };
  if (lastSignature) params.before = lastSignature;

  const resp = await fetchRPC({
    jsonrpc: '2.0', id: 1,
    method: 'getSignaturesForAddress',
    params: [SERVER_WALLET, params],
  });
  const sigs = resp?.result || [];
  if (sigs.length === 0) return;

  log.info(`Indexer: scanning ${sigs.length} signatures via RPC`);

  for (const sigInfo of sigs) {
    if (sigInfo.err) continue;
    try {
      const txData = await fetchTransaction(sigInfo.signature);
      if (!txData) continue;
      const memoData = extractRPCMemoData(txData);
      if (!memoData) continue;
      processPointerMemo(memoData, sigInfo.signature);
      await sleep(STAGGER_MS);
    } catch (err) {
      log.warn(`Indexer: failed to process sig ${sigInfo.signature.slice(0, 12)}... — ${err.message}`);
    }
  }

  lastSignature = sigs[sigs.length - 1].signature;
}

/** Shared: process a potential pointer memo string */
function processPointerMemo(memoData, signature) {
  const pointer = parsePointerMemo(memoData);
  if (!pointer) return;
  if (!pointer.hash || isNaN(pointer.chunkCount) || pointer.chunkCount <= 0) return;

  const existing = db.getArtwork(pointer.hash);
  if (!existing) {
    db.upsertArtwork({
      hash: pointer.hash,
      chunkCount: pointer.chunkCount,
      blobSize: pointer.blobSize || null,
      width: null,
      height: null,
      mode: 'open',
      network: 'mainnet',
      pointerSig: signature,
      chunks: null,
    });
    log.info(`Indexer: discovered ${pointer.hash} (${pointer.chunkCount} chunks, v${pointer.version})`);
  }
}

// ─── Fill incomplete: fetch chunk data ───

async function fillIncomplete() {
  const incomplete = db.getIncomplete();
  if (incomplete.length === 0) return;

  log.info(`Indexer: ${incomplete.length} incomplete artworks to fill`);

  for (const artwork of incomplete) {
    try {
      await fetchAndCacheChunks(artwork);
      await sleep(STAGGER_MS * 2);
    } catch (err) {
      log.warn(`Indexer: failed to fill ${artwork.hash.slice(0, 16)}... — ${err.message}`);
    }
  }
}

async function fetchAndCacheChunks(artwork) {
  if (!artwork.pointer_sig) {
    log.info(`Indexer: no pointer sig for ${artwork.hash.slice(0, 16)}... — skipping`);
    return;
  }

  let chunks;
  if (useEnhancedAPI) {
    chunks = await fillChunksEnhanced(artwork);
  } else {
    chunks = await fillChunksRPC(artwork);
  }

  if (chunks && chunks.length > 0) {
    db.upsertArtwork({
      hash: artwork.hash,
      chunkCount: artwork.chunk_count,
      blobSize: artwork.blob_size,
      width: artwork.width,
      height: artwork.height,
      mode: artwork.mode,
      network: artwork.network,
      pointerSig: artwork.pointer_sig,
      chunks,
    });

    const cached = db.getChunkCount(artwork.hash);
    log.info(`Indexer: cached ${cached}/${artwork.chunk_count} chunks for ${artwork.hash.slice(0, 16)}...`);
  }
}

/** Fill chunks via Enhanced API (paid plan) */
async function fillChunksEnhanced(artwork) {
  const txs = await fetchEnhancedTransactions(SERVER_WALLET, artwork.pointer_sig, artwork.chunk_count + 10);
  if (!txs || txs.length === 0) return [];

  const chunks = [];
  for (let i = txs.length - 1; i >= 0 && chunks.length < artwork.chunk_count; i--) {
    const tx = txs[i];
    if (tx.transactionError) continue;
    const memoData = extractEnhancedMemoData(tx);
    if (!memoData || memoData.startsWith('FREEZEDRY:')) continue;
    // v3: strip self-identifying header, use embedded index if available
    const stripped = stripV3Header(memoData);
    chunks.push({ index: chunks.length, signature: tx.signature, data: stripped });
  }
  return chunks;
}

/** Fill chunks via standard RPC (free plan) */
async function fillChunksRPC(artwork) {
  const resp = await fetchRPC({
    jsonrpc: '2.0', id: 1,
    method: 'getSignaturesForAddress',
    params: [SERVER_WALLET, {
      before: artwork.pointer_sig,
      limit: artwork.chunk_count + 10,
    }],
  });
  const sigs = resp?.result || [];

  const chunks = [];
  let concurrent = 0;

  for (let i = sigs.length - 1; i >= 0 && chunks.length < artwork.chunk_count; i--) {
    const sigInfo = sigs[i];
    if (sigInfo.err) continue;

    concurrent++;
    if (concurrent >= MAX_CONCURRENT) {
      await sleep(STAGGER_MS);
      concurrent = 0;
    }

    try {
      const txData = await fetchTransaction(sigInfo.signature);
      if (!txData) continue;
      const memoData = extractRPCMemoData(txData);
      if (!memoData || memoData.startsWith('FREEZEDRY:')) continue;
      // v3: strip self-identifying header
      const stripped = stripV3Header(memoData);
      chunks.push({ index: chunks.length, signature: sigInfo.signature, data: stripped });
    } catch (_) {}
  }
  return chunks;
}

// ─── Seed from registry (startup backfill) ───

async function seedFromRegistry() {
  if (!REGISTRY_URL) return;

  log.info('Indexer: seeding from registry...');
  const url = REGISTRY_URL.includes('/api/registry')
    ? `${REGISTRY_URL}?action=list&limit=200`
    : `${REGISTRY_URL}/api/registry?action=list&limit=200`;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Registry returned ${resp.status}`);

  const { artworks } = await resp.json();
  if (!artworks || artworks.length === 0) return;

  let newCount = 0;
  for (const art of artworks) {
    const existing = db.getArtwork(art.hash);
    if (existing) continue;

    db.upsertArtwork({
      hash: art.hash,
      chunkCount: art.chunkCount || 0,
      blobSize: art.blobSize || null,
      width: art.width || null,
      height: art.height || null,
      mode: art.mode || 'open',
      network: art.network || 'mainnet',
      pointerSig: null,
      chunks: null,
    });
    newCount++;
  }

  log.info(`Indexer: seeded ${newCount} new artworks from registry`);
}

// ─── Enhanced API helpers (paid plans) ───

/**
 * Fetch via Helius Enhanced Transactions API.
 * 100 credits per 100 results — Developer+ plans only.
 */
async function fetchEnhancedTransactions(address, beforeSig, limit = 100) {
  let url = `https://api-mainnet.helius-rpc.com/v0/addresses/${address}/transactions/?api-key=${HELIUS_API_KEY}&limit=${Math.min(limit, 100)}`;
  if (beforeSig) url += `&before=${beforeSig}`;

  const resp = await fetch(url, { signal: AbortSignal.timeout(30000) });

  if (resp.status === 429) {
    log.warn('Indexer: rate limited (429) — backing off');
    await sleep(10000);
    return [];
  }
  if (resp.status === 403 || resp.status === 401) {
    throw new Error(`Enhanced API returned ${resp.status} — Forbidden`);
  }
  if (!resp.ok) throw new Error(`Enhanced API returned ${resp.status}`);
  return resp.json();
}

/** Extract memo data from Enhanced API transaction format */
function extractEnhancedMemoData(tx) {
  for (const ix of tx?.instructions || []) {
    if (ix.programId === MEMO_PROGRAM && ix.data) return ix.data;
  }
  for (const inner of tx?.innerInstructions || []) {
    for (const ix of inner?.instructions || []) {
      if (ix.programId === MEMO_PROGRAM && ix.data) return ix.data;
    }
  }
  return null;
}

// ─── Standard RPC helpers (free plan) ───

async function fetchRPC(body) {
  const resp = await fetch(HELIUS_RPC, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(30000),
  });

  if (resp.status === 429) {
    log.warn('Indexer: rate limited (429) — backing off');
    await sleep(10000);
    return null;
  }

  if (!resp.ok) throw new Error(`RPC returned ${resp.status}`);
  return resp.json();
}

async function fetchTransaction(signature) {
  const resp = await fetchRPC({
    jsonrpc: '2.0', id: 1,
    method: 'getTransaction',
    params: [signature, { encoding: 'jsonParsed', maxSupportedTransactionVersion: 0 }],
  });
  return resp?.result || null;
}

/** Extract memo data from standard jsonParsed RPC transaction format */
function extractRPCMemoData(tx) {
  const msg = tx?.transaction?.message;
  if (!msg) return null;
  for (const ix of msg.instructions || []) {
    if (ix.programId === MEMO_PROGRAM && ix.parsed) return ix.parsed;
  }
  for (const inner of tx.meta?.innerInstructions || []) {
    for (const ix of inner.instructions || []) {
      if (ix.programId === MEMO_PROGRAM && ix.parsed) return ix.parsed;
    }
  }
  return null;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
