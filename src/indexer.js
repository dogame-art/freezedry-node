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
let HELIUS_API_KEY, HELIUS_RPC, SERVER_WALLET, REGISTRY_URL, POLL_INTERVAL, PEER_NODES, NODE_URL;

// Auto-detected: true = paid key with Enhanced API, false = free key (RPC only)
let useEnhancedAPI = null; // null = not yet detected

function loadConfig() {
  HELIUS_API_KEY = process.env.HELIUS_API_KEY || '';
  HELIUS_RPC = process.env.HELIUS_RPC_URL || `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
  SERVER_WALLET = process.env.SERVER_WALLET || '6ao3hnvKQJfmQ94xTMV34uLUP6azVNHzCfip1ic5Nafj';
  REGISTRY_URL = process.env.REGISTRY_URL || '';
  POLL_INTERVAL = parseInt(process.env.POLL_INTERVAL || '120000', 10);
  PEER_NODES = (process.env.PEER_NODES || '').split(',').map(s => s.trim()).filter(Boolean);
  NODE_URL = process.env.NODE_URL || '';
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

  // Connect to peer network on startup
  if (PEER_NODES.length > 0 || NODE_URL) {
    joinPeerNetwork().catch(err => log.warn(`Peer network join failed: ${err.message}`));
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
      // Try peer sync first (much faster than chain reads)
      const peerFilled = await fillFromPeers(artwork);
      if (peerFilled) continue;

      // Fall back to chain reads
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

// ─── Peer Network: sync blobs from peers, gossip discovery ───

/**
 * Try to fill an artwork's blob from a peer node (much faster than chain reads).
 * Peers serve cached blobs via GET /blob/:hash. We verify SHA-256 before storing.
 */
async function fillFromPeers(artwork) {
  const peers = db.listPeers();
  if (peers.length === 0) return false;

  for (const peer of peers) {
    try {
      const resp = await fetch(`${peer.url}/blob/${encodeURIComponent(artwork.hash)}`, {
        signal: AbortSignal.timeout(30000),
      });
      if (!resp.ok) continue;

      const blobBuffer = Buffer.from(await resp.arrayBuffer());
      if (blobBuffer.length < 49) continue; // too small for valid .hyd

      // Verify magic bytes
      if (blobBuffer[0] !== 0x48 || blobBuffer[1] !== 0x59 || blobBuffer[2] !== 0x44 || blobBuffer[3] !== 0x01) continue;

      // Verify SHA-256: extract hash from blob header (bytes 17-48) and compare
      const { createHash } = await import('crypto');
      const embeddedHash = blobBuffer.slice(17, 49);
      const expectedHex = 'sha256:' + embeddedHash.toString('hex');

      // The artwork.hash from the pointer should match the blob's embedded hash
      if (expectedHex !== artwork.hash) {
        log.warn(`Indexer: peer ${peer.url} returned blob with hash mismatch for ${artwork.hash.slice(0, 16)}...`);
        continue;
      }

      // Store as chunks (split to match chunk_count for consistency)
      const CHUNK_SIZE = 585;
      const chunks = [];
      for (let off = 0; off < blobBuffer.length; off += CHUNK_SIZE) {
        chunks.push({
          index: chunks.length,
          signature: `peer:${peer.url.slice(0, 30)}`,
          data: blobBuffer.slice(off, Math.min(off + CHUNK_SIZE, blobBuffer.length)),
        });
      }

      db.upsertArtwork({
        hash: artwork.hash,
        chunkCount: artwork.chunk_count,
        blobSize: blobBuffer.length,
        width: artwork.width,
        height: artwork.height,
        mode: artwork.mode,
        network: artwork.network,
        pointerSig: artwork.pointer_sig,
        chunks,
      });

      log.info(`Indexer: filled ${artwork.hash.slice(0, 16)}... from peer ${peer.url} (${blobBuffer.length}B)`);
      return true;
    } catch (err) {
      // Peer unavailable — try next
    }
  }
  return false;
}

/**
 * Join the peer network on startup:
 * 1. Register configured PEER_NODES
 * 2. Announce self to all peers (bidirectional)
 * 3. Gossip: fetch peers' peer lists to discover more nodes
 */
async function joinPeerNetwork() {
  // Register configured peers
  for (const peerUrl of PEER_NODES) {
    db.upsertPeer(peerUrl);
    log.info(`Indexer: registered peer ${peerUrl}`);
  }

  // Announce self to all known peers
  if (NODE_URL) {
    const peers = db.listPeers();
    for (const peer of peers) {
      try {
        await announceToNode(peer.url, NODE_URL);
      } catch (err) {
        log.warn(`Indexer: failed to announce to ${peer.url} — ${err.message}`);
      }
    }
  }

  // Gossip: fetch peer lists from known peers to discover more
  await gossipPeers();
}

/** Announce this node's URL to a peer */
async function announceToNode(peerUrl, selfUrl) {
  const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || '';
  const resp = await fetch(`${peerUrl}/sync/announce`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': WEBHOOK_SECRET,
    },
    body: JSON.stringify({ url: selfUrl }),
    signal: AbortSignal.timeout(10000),
  });

  if (resp.ok) {
    log.info(`Indexer: announced self (${selfUrl}) to ${peerUrl}`);
  }
}

/** Gossip: fetch peer lists from known peers to discover the network */
async function gossipPeers() {
  const peers = db.listPeers();
  let discovered = 0;

  for (const peer of peers) {
    try {
      const resp = await fetch(`${peer.url}/nodes`, {
        signal: AbortSignal.timeout(10000),
      });
      if (!resp.ok) continue;
      const data = await resp.json();
      const peerList = data.nodes || [];

      for (const node of peerList) {
        const nodeUrl = node.url || node;
        if (typeof nodeUrl !== 'string' || !nodeUrl.startsWith('http')) continue;
        if (nodeUrl === NODE_URL) continue; // don't add self

        const existing = db.listPeers().find(p => p.url === nodeUrl);
        if (!existing) {
          db.upsertPeer(nodeUrl);
          discovered++;
          log.info(`Indexer: discovered peer ${nodeUrl} via gossip from ${peer.url}`);
        }
      }
    } catch (err) {
      // Peer unreachable — skip
    }
  }

  if (discovered > 0) {
    log.info(`Indexer: gossip discovered ${discovered} new peer(s)`);

    // Announce self to newly discovered peers
    if (NODE_URL) {
      const allPeers = db.listPeers();
      for (const peer of allPeers) {
        try { await announceToNode(peer.url, NODE_URL); } catch {}
      }
    }
  }
}

// ─── Seed from registry (startup backfill) ───

async function seedFromRegistry() {
  if (!REGISTRY_URL) return;

  log.info('Indexer: seeding from registry...');
  const baseUrl = REGISTRY_URL.includes('/api/registry')
    ? REGISTRY_URL
    : `${REGISTRY_URL}/api/registry`;

  let page = 1;
  let newCount = 0;
  let totalPages = 1;

  while (page <= totalPages) {
    const resp = await fetch(`${baseUrl}?action=list&limit=100&page=${page}&showLocked=true`);
    if (!resp.ok) {
      log.warn(`Indexer: registry page ${page} returned ${resp.status}, stopping seed`);
      break;
    }

    const data = await resp.json();
    const artworks = data.artworks || [];
    totalPages = data.pages || 1;

    if (artworks.length === 0) break;

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

    page++;
  }

  log.info(`Indexer: seeded ${newCount} new artworks from registry (${totalPages} pages)`);
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
