/**
 * db.js — SQLite database layer for Freeze Dry Node
 * WAL mode for concurrent reads, single-writer safety
 */

import Database from 'better-sqlite3';
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

  CREATE INDEX IF NOT EXISTS idx_chunks_hash ON chunks(hash);
  CREATE INDEX IF NOT EXISTS idx_artworks_complete ON artworks(complete);
  CREATE INDEX IF NOT EXISTS idx_artworks_indexed ON artworks(indexed_at);
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

export { db };
