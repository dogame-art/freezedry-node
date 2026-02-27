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

  // Auto-register with coordinator (wallet auth), then discover peers
  registerWithCoordinator()
    .then(() => discoverFromCoordinator())
    .catch(err => log.warn(`Coordinator startup failed: ${err.message}`));

  // Connect to peer network on startup (manual PEER_NODES + gossip)
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

      // Every 10 polls (~20 min): refresh coordinator registration, discover peers, gossip
      pollCount++;
      if (pollCount % 10 === 0) {
        db.cleanStalePeers();
        await registerWithCoordinator().catch(() => {});
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

  // If any artworks lack pointer_sig, force full scan to backfill them
  const needsBackfill = db.getIncomplete().some(a => !a.pointer_sig);
  if (needsBackfill) {
    log.info('Indexer: artworks need pointer_sig backfill — doing full scan');
  }

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
        if (existing.pointer_sig) hitKnown = true;
        // Backfill pointer_sig on registry-seeded records that lack it
        processPointerMemo(memoData, tx.signature);
        continue; // keep scanning this page (multiple pointers per batch)
      }

      processPointerMemo(memoData, tx.signature);
      discovered++;
    }

    // If we hit a known artwork, everything older is already indexed — stop
    // BUT: skip this optimization if artworks need pointer_sig backfill
    if (hitKnown && initialScanDone && !needsBackfill) break;

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

  // If any artworks lack pointer_sig, force full scan to backfill them
  const needsBackfill = db.getIncomplete().some(a => !a.pointer_sig);

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
            if (existing.pointer_sig) hitKnown = true;
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

    // Skip early-exit if artworks need pointer_sig backfill
    if (hitKnown && initialScanDone && !needsBackfill) break;

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

  // Phase 1: Parallel peer sync (fast — just HTTP downloads, no RPC)
  const PEER_BATCH = 4;
  const peerRemaining = [];
  for (let i = 0; i < incomplete.length; i += PEER_BATCH) {
    const batch = incomplete.slice(i, i + PEER_BATCH);
    const results = await Promise.allSettled(
      batch.map(art => fillFromPeers(art).then(ok => ({ art, ok })))
    );
    for (const r of results) {
      if (r.status === 'rejected' || !r.value.ok) {
        peerRemaining.push(r.status === 'fulfilled' ? r.value.art : batch[results.indexOf(r)]);
      }
    }
  }

  // Phase 2: Sequential chain reads for anything peers couldn't fill
  for (const artwork of peerRemaining) {
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

/** Fill chunks via Enhanced API (paid plan) — paginates through all chunks */
async function fillChunksEnhanced(artwork) {
  const needed = artwork.chunk_count;
  // v3 hash prefix for filtering — first 8 chars after "sha256:"
  const hash8 = artwork.hash.startsWith('sha256:') ? artwork.hash.slice(7, 15) : artwork.hash.slice(0, 8);
  const chunks = [];
  let beforeSig = artwork.pointer_sig;

  // Paginate: Enhanced API returns max 100 per call.
  // Other artworks' chunks are interspersed — filter by hash8 prefix.
  // Safety: max 100 pages = 10,000 txs.
  const MAX_PAGES = 100;
  const allTxs = [];

  for (let page = 0; page < MAX_PAGES; page++) {
    const txs = await fetchEnhancedTransactions(SERVER_WALLET, beforeSig, 100);
    if (!txs || txs.length === 0) break;
    allTxs.push(...txs);
    if (txs.length < 100) break; // last page
    beforeSig = txs[txs.length - 1].signature;
    await sleep(200); // rate limit between pages

    // Early exit: count chunks matching THIS artwork's hash
    let matched = 0;
    for (const tx of allTxs) {
      if (tx.transactionError) continue;
      const memoData = extractEnhancedMemoData(tx);
      if (!memoData || memoData.startsWith('FREEZEDRY:')) continue;
      if (memoData.startsWith('FD:') && !memoData.startsWith(`FD:${hash8}:`)) continue;
      matched++;
    }
    if (matched >= needed) break;
  }

  if (allTxs.length === 0) return [];

  // Extract only chunks belonging to this artwork (chronological order)
  for (let i = allTxs.length - 1; i >= 0 && chunks.length < needed; i--) {
    const tx = allTxs[i];
    if (tx.transactionError) continue;
    const memoData = extractEnhancedMemoData(tx);
    if (!memoData || memoData.startsWith('FREEZEDRY:')) continue;
    // v3: filter by hash prefix — skip chunks from other artworks
    if (memoData.startsWith('FD:') && !memoData.startsWith(`FD:${hash8}:`)) continue;
    // Use embedded index from v3 header if available
    const v3Index = parseV3Index(memoData);
    const stripped = stripV3Header(memoData);
    chunks.push({ index: v3Index !== null ? v3Index : chunks.length, signature: tx.signature, data: stripped });
  }
  return chunks;
}

/** Extract chunk index from v3 header: FD:{hash8}:{index}:{data} → index */
function parseV3Index(str) {
  if (!str.startsWith('FD:')) return null;
  const parts = str.split(':');
  if (parts.length < 4) return null;
  const idx = parseInt(parts[2], 10);
  return isNaN(idx) ? null : idx;
}

/** Fill chunks via standard RPC (free plan) — paginates through all sigs */
async function fillChunksRPC(artwork) {
  const needed = artwork.chunk_count;
  const hash8 = artwork.hash.startsWith('sha256:') ? artwork.hash.slice(7, 15) : artwork.hash.slice(0, 8);
  const allSigs = [];
  let beforeSig = artwork.pointer_sig;

  // Paginate: getSignaturesForAddress returns max 1000 per call.
  // Other artworks' txs are interspersed — we over-fetch then filter by hash8.
  // Safety: max 20 pages = 20,000 sigs.
  const MAX_PAGES = 20;
  for (let page = 0; page < MAX_PAGES; page++) {
    const batchLimit = 1000;
    const resp = await fetchRPC({
      jsonrpc: '2.0', id: 1,
      method: 'getSignaturesForAddress',
      params: [SERVER_WALLET, { before: beforeSig, limit: batchLimit }],
    });
    const sigs = resp?.result || [];
    if (sigs.length === 0) break;
    allSigs.push(...sigs);

    // Rough proxy — need more sigs than needed chunks due to interleaving
    const validCount = allSigs.filter(s => !s.err).length;
    if (validCount >= needed * 2) break; // 2x buffer for interleaved txs

    if (sigs.length < batchLimit) break; // last page
    beforeSig = sigs[sigs.length - 1].signature;
    await sleep(200);
  }

  const chunks = [];
  let concurrent = 0;

  for (let i = allSigs.length - 1; i >= 0 && chunks.length < needed; i--) {
    const sigInfo = allSigs[i];
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
      // v3: filter by hash prefix — skip chunks from other artworks
      if (memoData.startsWith('FD:') && !memoData.startsWith(`FD:${hash8}:`)) continue;
      const v3Index = parseV3Index(memoData);
      const stripped = stripV3Header(memoData);
      chunks.push({ index: v3Index !== null ? v3Index : chunks.length, signature: sigInfo.signature, data: stripped });
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
      if (blobBuffer.length < 10) continue;

      // ── Verification: support both HYD format and legacy (raw) blobs ──
      const hasHydMagic = blobBuffer[0] === 0x48 && blobBuffer[1] === 0x59 &&
                          blobBuffer[2] === 0x44 && blobBuffer[3] === 0x01;
      let verified = false;

      if (hasHydMagic && blobBuffer.length >= 49) {
        // HYD format: verify embedded hash + file data hash
        const embeddedHash = blobBuffer.slice(17, 49);
        const expectedHex = 'sha256:' + embeddedHash.toString('hex');
        if (expectedHex !== artwork.hash) {
          log.warn(`Indexer: peer ${peer.url} HYD header hash mismatch for ${artwork.hash.slice(0, 16)}...`);
          continue;
        }
        const mode = blobBuffer[4];
        if (mode >= 3) {
          const view = new DataView(blobBuffer.buffer, blobBuffer.byteOffset, blobBuffer.byteLength);
          const deltaLen = view.getUint32(13, true);
          const fileBytes = blobBuffer.slice(49, 49 + deltaLen);
          const computed = createHash('sha256').update(fileBytes).digest();
          if (!computed.equals(embeddedHash)) {
            log.warn(`Indexer: peer ${peer.url} HYD data hash mismatch for ${artwork.hash.slice(0, 16)}...`);
            continue;
          }
        }
        verified = true;
      }

      // ── Peer trust: registered + liveness-verified peers are trusted ──
      // Peers are already liveness-checked before registration (/sync/announce).
      // For non-HYD blobs (chain-reconstructed data), trust the peer.
      // Chain is still source of truth — indexer re-validates from chain on future cycles.
      if (!verified) {
        // Basic size sanity: blob should be roughly chunk_count * ~585-600 bytes
        const minExpected = artwork.chunk_count * 500;
        const maxExpected = artwork.chunk_count * 700;
        if (blobBuffer.length >= minExpected && blobBuffer.length <= maxExpected) {
          log.info(`Indexer: accepting non-HYD blob from registered peer ${peer.url} for ${artwork.hash.slice(0, 16)}... (${blobBuffer.length}B, ~${artwork.chunk_count} chunks)`);
          verified = true;
        } else {
          log.warn(`Indexer: peer ${peer.url} blob size mismatch for ${artwork.hash.slice(0, 16)}... — got ${blobBuffer.length}B, expected ~${minExpected}-${maxExpected}B`);
          continue;
        }
      }

      // ── Verified — store complete blob ──
      db.storeBlob(artwork.hash, blobBuffer);
      db.upsertArtwork({
        hash: artwork.hash,
        chunkCount: artwork.chunk_count,
        blobSize: blobBuffer.length,
        width: artwork.width,
        height: artwork.height,
        mode: artwork.mode,
        network: artwork.network,
        pointerSig: artwork.pointer_sig,
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
  const resp = await fetch(`${peerUrl}/sync/announce`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
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
      const fetchOpts = { signal: AbortSignal.timeout(10000) };
      if (NODE_URL) fetchOpts.headers = { 'X-Node-URL': NODE_URL };
      const resp = await fetch(`${peer.url}/nodes`, fetchOpts);
      if (!resp.ok) continue;
      const data = await resp.json();
      const peerList = data.nodes || [];

      const MAX_GOSSIP_PEERS = 20;  // cap per response to prevent flooding
      const MAX_TOTAL_PEERS = 50;   // cap total peer list size
      let accepted = 0;

      for (const node of peerList) {
        if (accepted >= MAX_GOSSIP_PEERS) break;
        if (db.listPeers().length >= MAX_TOTAL_PEERS) break;

        const nodeUrl = node.url || node;
        if (typeof nodeUrl !== 'string') continue;
        // Validate: must be HTTPS, no private IPs
        try {
          const parsed = new URL(nodeUrl);
          if (parsed.protocol !== 'https:') continue;
          if (/^(10\.|172\.(1[6-9]|2\d|3[01])\.|192\.168\.|169\.254\.|127\.|0\.)/.test(parsed.hostname)) continue;
        } catch { continue; }
        if (nodeUrl === NODE_URL) continue; // don't add self

        const existing = db.listPeers().find(p => p.url === nodeUrl);
        if (!existing) {
          db.upsertPeer(nodeUrl);
          discovered++;
          accepted++;
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

// NOTE: COORDINATOR_URL read lazily via function — process.env isn't populated
// at module-init time because loadEnv() in server.js runs after static imports.
function getCoordinatorUrl() {
  return process.env.COORDINATOR_URL || 'https://freezedry.art';
}

/**
 * Discover peer nodes from the coordinator's node registry.
 * Nodes register with the coordinator (wallet-authed), and we discover them here.
 * Runs on startup + every gossip cycle.
 */
async function discoverFromCoordinator() {
  try {
    const resp = await fetch(`${getCoordinatorUrl()}/api/nodes?action=list`, {
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

// ─── Coordinator registration: auto-register with wallet auth ───

// DER prefix for Ed25519 PKCS8 private key (RFC 8410)
const ED25519_PKCS8_PREFIX = Buffer.from('302e020100300506032b657004220420', 'hex');

/**
 * Auto-register this node with the coordinator using ed25519 wallet signature.
 * Requires NODE_URL + WALLET_KEYPAIR in .env.
 * Runs on startup + periodically to refresh liveness.
 */
async function registerWithCoordinator() {
  if (!NODE_URL) return;

  const keypairJson = process.env.WALLET_KEYPAIR || '';
  if (!keypairJson) {
    log.info('Indexer: no WALLET_KEYPAIR — skipping coordinator registration (discovery still works)');
    return;
  }

  try {
    const { sign, createPrivateKey } = await import('crypto');

    // Parse Solana keypair: [0..31] = ed25519 seed, [32..63] = public key
    const keypairBytes = new Uint8Array(JSON.parse(keypairJson));
    const seedBytes = keypairBytes.slice(0, 32);
    const pubBytes = keypairBytes.slice(32, 64);
    const walletPubkey = encodeBase58(pubBytes);

    // Sign registration message
    const ROLE = process.env.ROLE || 'both';
    const timestamp = Math.floor(Date.now() / 1000);
    const message = `FreezeDry:node-register:${NODE_URL}:${timestamp}`;
    const messageBytes = Buffer.from(message, 'utf-8');

    const privateKeyObj = createPrivateKey({
      key: Buffer.concat([ED25519_PKCS8_PREFIX, Buffer.from(seedBytes)]),
      format: 'der',
      type: 'pkcs8',
    });
    const sigBytes = sign(null, messageBytes, privateKeyObj);
    const signature = sigBytes.toString('base64');

    // POST to coordinator
    const resp = await fetch(`${getCoordinatorUrl()}/api/nodes?action=register`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        nodeId: process.env.NODE_ID || 'freezedry-node',
        nodeUrl: NODE_URL,
        role: ROLE,
        walletPubkey,
        message,
        signature,
      }),
      signal: AbortSignal.timeout(15000),
    });

    if (resp.ok) {
      const data = await resp.json();
      log.info(`Indexer: registered with coordinator (wallet: ${walletPubkey.slice(0, 8)}..., status: ${data.status})`);
    } else {
      const text = await resp.text().catch(() => '');
      log.warn(`Indexer: coordinator registration failed (${resp.status}): ${text.slice(0, 200)}`);
    }
  } catch (err) {
    log.warn(`Indexer: coordinator registration error — ${err.message}`);
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
      if (existing) {
        // Backfill pointer_sig from registry if missing locally
        if (!existing.pointer_sig && art.pointerSig) {
          db.upsertArtwork({
            hash: art.hash,
            chunkCount: art.chunkCount || existing.chunk_count,
            blobSize: art.blobSize || existing.blob_size,
            width: art.width || existing.width,
            height: art.height || existing.height,
            mode: art.mode || existing.mode || 'open',
            network: art.network || existing.network || 'mainnet',
            pointerSig: art.pointerSig,
            chunks: null,
          });
          log.info(`Indexer: backfilled pointer_sig from registry for ${art.hash.slice(0, 24)}...`);
          newCount++;
        }
        continue;
      }

      db.upsertArtwork({
        hash: art.hash,
        chunkCount: art.chunkCount || art.sigCount || 0,
        blobSize: art.blobSize || null,
        width: art.width || null,
        height: art.height || null,
        mode: art.mode || 'open',
        network: art.network || 'mainnet',
        pointerSig: art.pointerSig || null,
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

/** Extract memo data from Enhanced API transaction format.
 *  Helius Enhanced API returns memo instruction data as base58 — decode to UTF-8. */
function extractEnhancedMemoData(tx) {
  for (const ix of tx?.instructions || []) {
    if (ix.programId === MEMO_PROGRAM && ix.data) return decodeBase58Memo(ix.data);
  }
  for (const inner of tx?.innerInstructions || []) {
    for (const ix of inner?.instructions || []) {
      if (ix.programId === MEMO_PROGRAM && ix.data) return decodeBase58Memo(ix.data);
    }
  }
  return null;
}

/** Decode base58-encoded memo data to UTF-8 string.
 *  Inline decoder — no external dependency needed. */
const B58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
const B58_MAP = new Uint8Array(128).fill(255);
for (let i = 0; i < B58_ALPHABET.length; i++) B58_MAP[B58_ALPHABET.charCodeAt(i)] = i;

function decodeBase58(str) {
  const bytes = [];
  for (let i = 0; i < str.length; i++) {
    let carry = B58_MAP[str.charCodeAt(i)];
    if (carry === 255) throw new Error('Invalid base58 char');
    for (let j = 0; j < bytes.length; j++) {
      carry += bytes[j] * 58;
      bytes[j] = carry & 0xff;
      carry >>= 8;
    }
    while (carry > 0) {
      bytes.push(carry & 0xff);
      carry >>= 8;
    }
  }
  // Leading '1's = leading zero bytes
  for (let i = 0; i < str.length && str[i] === '1'; i++) bytes.push(0);
  return Buffer.from(bytes.reverse());
}

function decodeBase58Memo(data) {
  if (data.startsWith('FREEZEDRY:') || data.startsWith('FD:')) return data;
  try {
    return decodeBase58(data).toString('utf-8');
  } catch {
    return data;
  }
}

/** Encode bytes to base58 string (for public key → base58 address) */
function encodeBase58(bytes) {
  if (bytes.length === 0) return '';
  const digits = [0];
  for (let i = 0; i < bytes.length; i++) {
    let carry = bytes[i];
    for (let j = 0; j < digits.length; j++) {
      carry += digits[j] << 8;
      digits[j] = carry % 58;
      carry = (carry / 58) | 0;
    }
    while (carry > 0) {
      digits.push(carry % 58);
      carry = (carry / 58) | 0;
    }
  }
  let output = '';
  for (let i = 0; i < bytes.length && bytes[i] === 0; i++) output += B58_ALPHABET[0];
  for (let i = digits.length - 1; i >= 0; i--) output += B58_ALPHABET[digits[i]];
  return output;
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
