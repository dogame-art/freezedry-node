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
let HELIUS_API_KEY, HELIUS_RPC, SERVER_WALLET, REGISTRY_URL, POLL_INTERVAL, PEER_NODES, NODE_URL, GENESIS_SIG;

// Auto-detected: true = paid key with Enhanced API, false = free key (RPC only)
let useEnhancedAPI = null; // null = not yet detected

function loadConfig() {
  HELIUS_API_KEY = process.env.HELIUS_API_KEY || '';
  HELIUS_RPC = process.env.HELIUS_RPC_URL || `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
  SERVER_WALLET = process.env.SERVER_WALLET || '6ao3hnvKQJfmQ94xTMV34uLUP6azVNHzCfip1ic5Nafj';
  REGISTRY_URL = process.env.REGISTRY_URL || 'https://freezedry.art';
  POLL_INTERVAL = parseInt(process.env.POLL_INTERVAL || '120000', 10);
  PEER_NODES = (process.env.PEER_NODES || '').split(',').map(s => s.trim()).filter(Boolean);
  NODE_URL = process.env.NODE_URL || '';
  // Hard stop for backwards pagination — don't scan older than this signature.
  // Saves RPC credits on first scan. Set to the first Freeze Dry inscription sig.
  GENESIS_SIG = process.env.GENESIS_SIG || '';
  // Allow explicit override via env var
  if (process.env.USE_ENHANCED_API === 'true') useEnhancedAPI = true;
  if (process.env.USE_ENHANCED_API === 'false') useEnhancedAPI = false;
}

// Rate limiting: 3 concurrent, 500ms stagger
const MAX_CONCURRENT = 3;
const STAGGER_MS = 500;

const MEMO_PROGRAM = 'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr';

let log = console;
// Initial history scan cursor (persisted to SQLite — survives restarts)
// Once the full history is scanned, this stays at the oldest known sig.
let oldestScannedSig = null;
let initialScanDone = false;

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

  // Restore scan state from SQLite
  oldestScannedSig = db.getKV('oldest_scanned_sig') || null;
  initialScanDone = db.getKV('initial_scan_done') === 'true';
  if (initialScanDone) {
    log.info('Indexer: resuming — initial history scan already complete');
  }

  // Seed from registry on startup (backfill)
  if (REGISTRY_URL) {
    seedFromRegistry().catch(err => log.warn(`Registry seed failed: ${err.message}`));
  }

  // Discover peers from coordinator + configured peers
  discoverFromCoordinator().catch(err => log.warn(`Coordinator discovery failed: ${err.message}`));

  // Connect to peer network on startup
  if (PEER_NODES.length > 0 || NODE_URL) {
    joinPeerNetwork().catch(err => log.warn(`Peer network join failed: ${err.message}`));
  }

  // Start polling
  pollLoop();
}

let pollCount = 0;

async function pollLoop() {
  while (true) {
    try {
      await scanForPointerMemos();
      await fillIncomplete();

      // Every 10 polls (~20 min): clean stale peers, discover from coordinator, re-gossip
      pollCount++;
      if (pollCount % 10 === 0) {
        db.cleanStalePeers();
        await discoverFromCoordinator().catch(() => {});
        await gossipPeers().catch(() => {});
      }
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

/**
 * Scan via Enhanced Transactions API (paid plans — 100 credits per 100 results).
 *
 * Strategy: always start from newest (before=null), paginate backwards.
 * Stop when we hit an already-known artwork hash — everything older is already indexed.
 * On first run, walks the full history. On subsequent runs, only fetches new txs.
 */
async function scanEnhanced() {
  let before = null;       // start from newest
  let discovered = 0;
  const MAX_PAGES = 100;   // safety cap: 10,000 txs max per poll

  for (let page = 0; page < MAX_PAGES; page++) {
    const txs = await fetchEnhancedTransactions(SERVER_WALLET, before);
    if (!txs || txs.length === 0) break;

    let hitKnown = false;
    for (const tx of txs) {
      if (tx.transactionError) continue;
      const memoData = extractEnhancedMemoData(tx);
      if (!memoData) continue;

      const pointer = parsePointerMemo(memoData);
      if (!pointer || !pointer.hash || isNaN(pointer.chunkCount) || pointer.chunkCount <= 0) continue;

      // Check if already known — if so, we've reached indexed territory
      const existing = db.getArtwork(pointer.hash);
      if (existing) {
        hitKnown = true;
        // Backfill pointer_sig on registry-seeded records that lack it
        processPointerMemo(memoData, tx.signature);
        continue; // keep scanning this page (multiple pointers per batch)
      }

      processPointerMemo(memoData, tx.signature);
      discovered++;
    }

    // If we hit a known artwork, everything older is already indexed — stop
    if (hitKnown && initialScanDone) break;

    // Update backward cursor for initial full-history scan
    before = txs[txs.length - 1].signature;
    db.setKV('oldest_scanned_sig', before);

    // Genesis sig: hard stop — don't scan older than the first Freeze Dry inscription
    if (GENESIS_SIG && txs.some(tx => tx.signature === GENESIS_SIG)) {
      if (!initialScanDone) {
        initialScanDone = true;
        db.setKV('initial_scan_done', 'true');
        log.info('Indexer: reached genesis signature — initial scan complete');
      }
      break;
    }

    // If batch was smaller than limit, we've reached the end of history
    if (txs.length < 100) {
      if (!initialScanDone) {
        initialScanDone = true;
        db.setKV('initial_scan_done', 'true');
        log.info('Indexer: initial full history scan complete');
      }
      break;
    }

    await sleep(200); // rate limit between pages
  }

  if (discovered > 0) {
    log.info(`Indexer: discovered ${discovered} new artwork(s) via Enhanced API`);
  }
}

/**
 * Scan via standard RPC (free plan — getSignaturesForAddress + getTransaction).
 * Same strategy: newest-first, stop on known artwork.
 */
async function scanRPC() {
  let before = null;
  let discovered = 0;
  const MAX_PAGES = 100;

  for (let page = 0; page < MAX_PAGES; page++) {
    const params = { limit: 50 };
    if (before) params.before = before;

    const resp = await fetchRPC({
      jsonrpc: '2.0', id: 1,
      method: 'getSignaturesForAddress',
      params: [SERVER_WALLET, params],
    });
    const sigs = resp?.result || [];
    if (sigs.length === 0) break;

    let hitKnown = false;
    for (const sigInfo of sigs) {
      if (sigInfo.err) continue;
      try {
        const txData = await fetchTransaction(sigInfo.signature);
        if (!txData) continue;
        const memoData = extractRPCMemoData(txData);
        if (!memoData) continue;

        const pointer = parsePointerMemo(memoData);
        if (pointer && pointer.hash) {
          const existing = db.getArtwork(pointer.hash);
          if (existing) {
            hitKnown = true;
            // Backfill pointer_sig on registry-seeded records that lack it
            processPointerMemo(memoData, sigInfo.signature);
            continue;
          }
        }

        processPointerMemo(memoData, sigInfo.signature);
        if (pointer && !db.getArtwork(pointer?.hash)) discovered++;
        await sleep(STAGGER_MS);
      } catch (err) {
        log.warn(`Indexer: failed to process sig ${sigInfo.signature.slice(0, 12)}... — ${err.message}`);
      }
    }

    if (hitKnown && initialScanDone) break;

    before = sigs[sigs.length - 1].signature;
    db.setKV('oldest_scanned_sig', before);

    // Genesis sig: hard stop
    if (GENESIS_SIG && sigs.some(s => s.signature === GENESIS_SIG)) {
      if (!initialScanDone) {
        initialScanDone = true;
        db.setKV('initial_scan_done', 'true');
        log.info('Indexer: reached genesis signature — initial scan complete');
      }
      break;
    }

    if (sigs.length < 50) {
      if (!initialScanDone) {
        initialScanDone = true;
        db.setKV('initial_scan_done', 'true');
        log.info('Indexer: initial full history scan complete');
      }
      break;
    }

    await sleep(200);
  }

  if (discovered > 0) {
    log.info(`Indexer: discovered ${discovered} new artwork(s) via RPC`);
  }
}

/** Shared: process a potential pointer memo string */
function processPointerMemo(memoData, signature) {
  const pointer = parsePointerMemo(memoData);
  if (!pointer) return;
  if (!pointer.hash || isNaN(pointer.chunkCount) || pointer.chunkCount <= 0) return;

  const existing = db.getArtwork(pointer.hash);
  if (existing) {
    // Existing record from registry seed may lack pointer_sig — backfill it
    if (!existing.pointer_sig && signature) {
      db.upsertArtwork({
        hash: pointer.hash,
        chunkCount: pointer.chunkCount || existing.chunk_count,
        blobSize: pointer.blobSize || existing.blob_size,
        width: existing.width,
        height: existing.height,
        mode: existing.mode || 'open',
        network: existing.network || 'mainnet',
        pointerSig: signature,
        chunks: null,
      });
      log.info(`Indexer: backfilled pointer sig for ${pointer.hash.slice(0, 24)}...`);
    }
    return;
  }

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
 * Peers serve cached blobs via GET /blob/:hash.
 *
 * Trustless verification (3 layers):
 *   1. Header hash matches pointer memo's hash (blob claims the right identity)
 *   2. SHA-256 of actual file data matches the embedded hash (data is authentic)
 *   3. Chain spot-check: fetch 1-2 random memo txs and compare to blob chunks
 *      (proves the blob matches what's actually on-chain)
 */
async function fillFromPeers(artwork) {
  const peers = db.listPeers();
  if (peers.length === 0) return false;

  const { createHash } = await import('crypto');

  for (const peer of peers) {
    try {
      const fetchOpts = { signal: AbortSignal.timeout(30000) };
      if (NODE_URL) fetchOpts.headers = { 'X-Node-URL': NODE_URL };
      const resp = await fetch(`${peer.url}/blob/${encodeURIComponent(artwork.hash)}`, fetchOpts);
      if (!resp.ok) continue;

      const blobBuffer = Buffer.from(await resp.arrayBuffer());
      if (blobBuffer.length < 49) continue;

      // ── Layer 1: Magic bytes + header hash matches pointer ──
      if (blobBuffer[0] !== 0x48 || blobBuffer[1] !== 0x59 || blobBuffer[2] !== 0x44 || blobBuffer[3] !== 0x01) {
        log.warn(`Indexer: peer ${peer.url} returned invalid blob (bad magic) for ${artwork.hash.slice(0, 16)}...`);
        continue;
      }

      const embeddedHash = blobBuffer.slice(17, 49);
      const expectedHex = 'sha256:' + embeddedHash.toString('hex');
      if (expectedHex !== artwork.hash) {
        log.warn(`Indexer: peer ${peer.url} header hash mismatch for ${artwork.hash.slice(0, 16)}...`);
        continue;
      }

      // ── Layer 2: Compute SHA-256 of actual data, compare to embedded hash ──
      const mode = blobBuffer[4];
      const isDirect = mode >= 3;
      const view = new DataView(blobBuffer.buffer, blobBuffer.byteOffset, blobBuffer.byteLength);
      const avifLen = view.getUint32(9, true);
      const deltaLen = view.getUint32(13, true);

      if (isDirect) {
        // Direct mode: hash the file bytes (stored in delta slot after header)
        const fileBytes = blobBuffer.slice(49, 49 + deltaLen);
        const computed = createHash('sha256').update(fileBytes).digest();
        if (!computed.equals(embeddedHash)) {
          log.warn(`Indexer: peer ${peer.url} data hash mismatch (direct mode) for ${artwork.hash.slice(0, 16)}...`);
          continue;
        }
      } else {
        // Pixel Perfect: hash is of reconstructed pixels (can't cheaply verify)
        // Fall through to Layer 3 chain spot-check for verification
      }

      // ── Layer 3: Chain spot-check — verify random chunks against on-chain memos ──
      if (artwork.pointer_sig && artwork.chunk_count > 0) {
        const spotCheckPassed = await chainSpotCheck(blobBuffer, artwork);
        if (spotCheckPassed === false) {
          log.warn(`Indexer: peer ${peer.url} FAILED chain spot-check for ${artwork.hash.slice(0, 16)}...`);
          continue;
        }
        // spotCheckPassed === null means we couldn't check (no RPC) — accept with Layer 1+2
      }

      // ── All checks passed — store ──
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

      log.info(`Indexer: filled ${artwork.hash.slice(0, 16)}... from peer ${peer.url} (${blobBuffer.length}B, verified)`);
      return true;
    } catch (err) {
      // Peer unavailable — try next
    }
  }
  return false;
}

/**
 * Chain spot-check: fetch 1-2 random on-chain memo chunks and compare to the blob.
 * This proves the peer's blob matches what's actually inscribed on Solana.
 * Returns true (passed), false (failed), or null (couldn't check).
 */
async function chainSpotCheck(blobBuffer, artwork) {
  if (!artwork.pointer_sig || !HELIUS_API_KEY) return null;

  try {
    // Get the list of chunk signatures from the chain (just the sigs, not full txs)
    const resp = await fetchRPC({
      jsonrpc: '2.0', id: 1,
      method: 'getSignaturesForAddress',
      params: [SERVER_WALLET, {
        before: artwork.pointer_sig,
        limit: Math.min(artwork.chunk_count + 5, 50),
      }],
    });
    const sigs = resp?.result || [];
    if (sigs.length === 0) return null;

    // Pick 1-2 random indices to spot-check
    const checkCount = Math.min(2, sigs.length);
    const indices = [];
    while (indices.length < checkCount) {
      const idx = Math.floor(Math.random() * Math.min(sigs.length, artwork.chunk_count));
      if (!indices.includes(idx)) indices.push(idx);
    }

    // Shred the blob the same way as inscription (585B chunks)
    const PAYLOAD_SIZE = 585;
    const blobChunks = [];
    for (let off = 0; off < blobBuffer.length; off += PAYLOAD_SIZE) {
      blobChunks.push(blobBuffer.slice(off, Math.min(off + PAYLOAD_SIZE, blobBuffer.length)));
    }

    // For each spot-check index, fetch the chain memo and compare
    for (const idx of indices) {
      // sigs are newest-first, chunks are oldest-first — reverse index
      const sigIdx = sigs.length - 1 - idx;
      if (sigIdx < 0 || sigIdx >= sigs.length) continue;
      if (sigs[sigIdx].err) continue;

      const txData = await fetchTransaction(sigs[sigIdx].signature);
      if (!txData) continue;

      const memoData = extractRPCMemoData(txData);
      if (!memoData || memoData.startsWith('FREEZEDRY:')) continue;

      // Strip v3 header to get the base64 payload
      const stripped = stripV3Header(memoData);

      // Decode the on-chain base64 data
      const chainBytes = Buffer.from(stripped, 'base64');

      // Compare to the blob's chunk at the same index
      if (idx < blobChunks.length) {
        const blobChunk = blobChunks[idx];
        if (!chainBytes.equals(blobChunk)) {
          log.warn(`Indexer: spot-check FAILED at chunk ${idx} — on-chain data doesn't match peer blob`);
          return false;
        }
      }

      await sleep(STAGGER_MS);
    }

    log.info(`Indexer: spot-check passed (${checkCount} chunks verified against chain) for ${artwork.hash.slice(0, 16)}...`);
    return true;
  } catch (err) {
    log.warn(`Indexer: spot-check error — ${err.message}`);
    return null; // couldn't check, don't reject
  }
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

// ─── Coordinator discovery: learn about peers from freezedry.art ───

const COORDINATOR_URL = process.env.COORDINATOR_URL || 'https://freezedry.art';

/**
 * Discover peer nodes from the coordinator's node registry.
 * Nodes register with the coordinator (wallet-authed), and we discover them here.
 * Runs on startup + every gossip cycle.
 */
async function discoverFromCoordinator() {
  try {
    const resp = await fetch(`${COORDINATOR_URL}/api/nodes?action=list`, {
      signal: AbortSignal.timeout(10000),
    });
    if (!resp.ok) return;
    const data = await resp.json();
    const nodes = data.nodes || [];

    let discovered = 0;
    for (const node of nodes) {
      if (!node.nodeUrl || node.nodeUrl === NODE_URL) continue; // skip self
      if (node.role !== 'reader' && node.role !== 'both') continue; // only sync from readers
      const existing = db.listPeers().find(p => p.url === node.nodeUrl);
      if (!existing) {
        db.upsertPeer(node.nodeUrl);
        discovered++;
        log.info(`Indexer: discovered peer ${node.nodeUrl} from coordinator`);
      }
    }
    if (discovered > 0) log.info(`Indexer: coordinator discovery found ${discovered} new peer(s)`);
  } catch (err) {
    log.warn(`Indexer: coordinator discovery failed — ${err.message}`);
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
