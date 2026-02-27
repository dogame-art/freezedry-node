/**
 * db.js — SQLite database layer for Freeze Dry Node
 * WAL mode for concurrent reads, single-writer safety
 */

import Database from 'better-sqlite3';
import { createHash } from 'crypto';
import { existsSync, mkdirSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_DIR = join(__dirname, '..', 'db');
const DB_PATH = join(DB_DIR, 'freezedry.db');

if (!existsSync(DB_DIR)) mkdirSync(DB_DIR, { recursive: true });

const db = new Database(DB_PATH);

// Performance: WAL mode + larger cache
db.pragma('journal_mode = WAL');
db.pragma('cache_size = -64000'); // 64MB cache
db.pragma('synchronous = NORMAL');

// Schema
db.exec(`
  CREATE TABLE IF NOT EXISTS artworks (
    hash TEXT PRIMARY KEY,
    chunk_count INTEGER NOT NULL,
    blob_size INTEGER,
    width INTEGER,
    height INTEGER,
    mode TEXT DEFAULT 'open',
    network TEXT DEFAULT 'mainnet',
    indexed_at INTEGER NOT NULL,
    pointer_sig TEXT,
    complete INTEGER DEFAULT 0
  );

  CREATE TABLE IF NOT EXISTS chunks (
    hash TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    signature TEXT NOT NULL,
    data BLOB NOT NULL,
    PRIMARY KEY (hash, chunk_index)
  );

  CREATE TABLE IF NOT EXISTS peers (
    url TEXT PRIMARY KEY,
    last_seen INTEGER,
    status TEXT DEFAULT 'active'
  );

  CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT
  );

  CREATE TABLE IF NOT EXISTS pod_receipts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hash TEXT NOT NULL,
    node_id TEXT NOT NULL,
    bytes INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    hmac TEXT NOT NULL,
    submitted INTEGER DEFAULT 0,
    memo_sig TEXT
  );

  CREATE INDEX IF NOT EXISTS idx_chunks_hash ON chunks(hash);
  CREATE INDEX IF NOT EXISTS idx_artworks_complete ON artworks(complete);
  CREATE INDEX IF NOT EXISTS idx_artworks_indexed ON artworks(indexed_at);
  CREATE INDEX IF NOT EXISTS idx_pod_submitted ON pod_receipts(submitted);
  CREATE INDEX IF NOT EXISTS idx_pod_timestamp ON pod_receipts(timestamp);
`);

// Prepared statements
const stmts = {
  upsertArtwork: db.prepare(`
    INSERT INTO artworks (hash, chunk_count, blob_size, width, height, mode, network, indexed_at, pointer_sig, complete)
    VALUES (@hash, @chunkCount, @blobSize, @width, @height, @mode, @network, @indexedAt, @pointerSig, @complete)
    ON CONFLICT(hash) DO UPDATE SET
      chunk_count = COALESCE(@chunkCount, chunk_count),
      blob_size = COALESCE(@blobSize, blob_size),
      width = COALESCE(@width, width),
      height = COALESCE(@height, height),
      pointer_sig = COALESCE(@pointerSig, pointer_sig),
      complete = MAX(complete, @complete)
  `),

  insertChunk: db.prepare(`
    INSERT OR IGNORE INTO chunks (hash, chunk_index, signature, data)
    VALUES (@hash, @chunkIndex, @signature, @data)
  `),

  getArtwork: db.prepare(`SELECT * FROM artworks WHERE hash = ?`),

  getBlob: db.prepare(`
    SELECT data FROM chunks WHERE hash = ? ORDER BY chunk_index ASC
  `),

  getChunkCount: db.prepare(`SELECT COUNT(*) as count FROM chunks WHERE hash = ?`),

  listArtworks: db.prepare(`
    SELECT hash, chunk_count, blob_size, width, height, mode, network, indexed_at, complete
    FROM artworks ORDER BY indexed_at DESC LIMIT ? OFFSET ?
  `),

  countArtworks: db.prepare(`SELECT COUNT(*) as count FROM artworks`),
  countComplete: db.prepare(`SELECT COUNT(*) as count FROM artworks WHERE complete = 1`),
  countChunks: db.prepare(`SELECT COUNT(*) as count FROM chunks`),

  getIncomplete: db.prepare(`SELECT * FROM artworks WHERE complete = 0`),

  markComplete: db.prepare(`UPDATE artworks SET complete = 1 WHERE hash = ?`),

  upsertPeer: db.prepare(`
    INSERT INTO peers (url, last_seen, status) VALUES (@url, @lastSeen, 'active')
    ON CONFLICT(url) DO UPDATE SET last_seen = @lastSeen, status = 'active'
  `),

  listPeers: db.prepare(`SELECT * FROM peers WHERE status = 'active' ORDER BY last_seen DESC`),
  stalePeers: db.prepare(`UPDATE peers SET status = 'stale' WHERE status = 'active' AND last_seen < ?`),

  // POD receipts
  insertPodReceipt: db.prepare(`
    INSERT INTO pod_receipts (hash, node_id, bytes, timestamp, hmac)
    VALUES (@hash, @nodeId, @bytes, @timestamp, @hmac)
  `),
  getUnsubmittedReceipts: db.prepare(`
    SELECT * FROM pod_receipts WHERE submitted = 0 ORDER BY timestamp ASC LIMIT ?
  `),
  markReceiptsSubmitted: db.prepare(`
    UPDATE pod_receipts SET submitted = 1, memo_sig = @memoSig WHERE id IN (SELECT id FROM pod_receipts WHERE submitted = 0 ORDER BY timestamp ASC LIMIT @limit)
  `),
  getPodStats: db.prepare(`
    SELECT node_id, COUNT(*) as count, SUM(bytes) as total_bytes, COUNT(DISTINCT hash) as unique_hashes
    FROM pod_receipts GROUP BY node_id
  `),
  countPodReceipts: db.prepare(`SELECT COUNT(*) as count FROM pod_receipts`),
  countPodUnsubmitted: db.prepare(`SELECT COUNT(*) as count FROM pod_receipts WHERE submitted = 0`),
};

// Transaction wrapper for bulk inserts
const ingestArtwork = db.transaction((artwork) => {
  const { hash, chunkCount, blobSize, width, height, mode, network, pointerSig, chunks } = artwork;
  const now = Date.now();

  stmts.upsertArtwork.run({
    hash, chunkCount, blobSize, width, height,
    mode: mode || 'open',
    network: network || 'mainnet',
    indexedAt: now,
    pointerSig: pointerSig || null,
    complete: chunks && chunks.length === chunkCount ? 1 : 0,
  });

  if (chunks) {
    for (const chunk of chunks) {
      stmts.insertChunk.run({
        hash,
        chunkIndex: chunk.index,
        signature: chunk.signature,
        data: typeof chunk.data === 'string' ? Buffer.from(chunk.data, 'base64') : chunk.data,
      });
    }

    // Check if now complete
    const { count } = stmts.getChunkCount.get(hash);
    if (count >= chunkCount) {
      stmts.markComplete.run(hash);
    }
  }
});

export function upsertArtwork(artwork) {
  return ingestArtwork(artwork);
}

export function getArtwork(hash) {
  return stmts.getArtwork.get(hash);
}

export function getBlob(hash) {
  const rows = stmts.getBlob.all(hash);
  if (rows.length === 0) return null;
  return Buffer.concat(rows.map(r => r.data));
}

export function listArtworks(limit = 50, offset = 0) {
  return stmts.listArtworks.all(limit, offset);
}

export function getStats() {
  return {
    artworks: stmts.countArtworks.get().count,
    complete: stmts.countComplete.get().count,
    chunks: stmts.countChunks.get().count,
  };
}

export function getIncomplete() {
  return stmts.getIncomplete.all();
}

export function markComplete(hash) {
  stmts.markComplete.run(hash);
}

/** Store a complete blob from peer sync — replaces any partial chain-sourced chunks. */
export function storeBlob(hash, blobBuffer) {
  db.transaction(() => {
    db.prepare('DELETE FROM chunks WHERE hash = ?').run(hash);
    db.prepare('INSERT INTO chunks (hash, chunk_index, signature, data) VALUES (?, 0, ?, ?)').run(
      hash, 'peer-sync', blobBuffer
    );
    stmts.markComplete.run(hash);
  })();
}

export function getChunkCount(hash) {
  return stmts.getChunkCount.get(hash).count;
}

export function upsertPeer(url) {
  stmts.upsertPeer.run({ url, lastSeen: Date.now() });
}

export function listPeers() {
  return stmts.listPeers.all();
}

/** Mark peers as stale if not seen in the given window (default 24h) */
export function cleanStalePeers(maxAgeMs = 24 * 60 * 60 * 1000) {
  const cutoff = Date.now() - maxAgeMs;
  return stmts.stalePeers.run(cutoff);
}

// KV store — persist indexer cursors across restarts
const kvGet = db.prepare(`SELECT value FROM kv WHERE key = ?`);
const kvSet = db.prepare(`INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`);

export function getKV(key) {
  const row = kvGet.get(key);
  return row ? row.value : null;
}

export function setKV(key, value) {
  kvSet.run(key, value);
}

// ── POD receipt functions ─────────────────────────────────────────────────

export function insertPodReceipt({ hash, nodeId, bytes, timestamp, hmac }) {
  stmts.insertPodReceipt.run({ hash, nodeId, bytes, timestamp, hmac });
}

export function getUnsubmittedReceipts(limit = 100) {
  return stmts.getUnsubmittedReceipts.all(limit);
}

export function markReceiptsSubmitted(limit, memoSig) {
  stmts.markReceiptsSubmitted.run({ limit, memoSig });
}

export function getPodStats() {
  return {
    perNode: stmts.getPodStats.all(),
    total: stmts.countPodReceipts.get().count,
    unsubmitted: stmts.countPodUnsubmitted.get().count,
  };
}

// ── Blob repair functions ─────────────────────────────────────────────────

/** Reset a corrupt blob so the indexer re-fetches it from chain. */
export function resetCorruptBlob(hash) {
  db.transaction(() => {
    db.prepare('DELETE FROM chunks WHERE hash = ?').run(hash);
    db.prepare('UPDATE artworks SET complete = 0 WHERE hash = ?').run(hash);
  })();
}

/**
 * Verify all complete blobs against their expected SHA-256 hash.
 * Returns { verified, corrupt, missing } counts + list of corrupt hashes.
 */
export function repairCorruptBlobs() {
  const complete = db.prepare('SELECT hash FROM artworks WHERE complete = 1').all();
  let verified = 0, corrupt = 0, missing = 0;
  const corruptHashes = [];

  for (const { hash } of complete) {
    const rows = stmts.getBlob.all(hash);
    if (rows.length === 0) {
      missing++;
      db.prepare('UPDATE artworks SET complete = 0 WHERE hash = ?').run(hash);
      corruptHashes.push({ hash, reason: 'missing-chunks' });
      continue;
    }
    const blob = Buffer.concat(rows.map(r => r.data));
    const computed = 'sha256:' + createHash('sha256').update(blob).digest('hex');
    if (computed === hash) {
      verified++;
    } else {
      corrupt++;
      corruptHashes.push({ hash, reason: 'hash-mismatch', computed: computed.slice(0, 24), blobSize: blob.length });
      // Reset for re-index from chain
      db.prepare('DELETE FROM chunks WHERE hash = ?').run(hash);
      db.prepare('UPDATE artworks SET complete = 0 WHERE hash = ?').run(hash);
    }
  }

  return { verified, corrupt, missing, total: complete.length, corruptHashes };
}

export { db };
