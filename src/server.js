/**
 * server.js — Freeze Dry Node HTTP server
 * Fastify-based API for serving cached artworks + receiving webhooks
 */

import Fastify from 'fastify';
import { readFileSync, existsSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { createHash } from 'crypto';
import * as db from './db.js';
import { startIndexer } from './indexer.js';

const __dirname = dirname(fileURLToPath(import.meta.url));

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

// Load .env manually (no dotenv dependency)
function loadEnv() {
  const envPath = join(__dirname, '..', '.env');
  if (!existsSync(envPath)) return;
  const lines = readFileSync(envPath, 'utf8').split('\n');
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eq = trimmed.indexOf('=');
    if (eq === -1) continue;
    const key = trimmed.slice(0, eq).trim();
    const val = trimmed.slice(eq + 1).trim();
    if (!process.env[key]) process.env[key] = val;
  }
}
loadEnv();

const PORT = parseInt(process.env.PORT || '3100', 10);
const NODE_ID = process.env.NODE_ID || 'freezedry-node';
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || '';
const startTime = Date.now();

// ─── Startup validation ───
if (!WEBHOOK_SECRET) {
  console.warn('⚠️  WARNING: WEBHOOK_SECRET is empty — /ingest and /webhook/helius are UNPROTECTED.');
  console.warn('   Set WEBHOOK_SECRET in .env to secure your node.');
}
if (!process.env.HELIUS_API_KEY) {
  console.warn('⚠️  WARNING: HELIUS_API_KEY not set — indexer will run in webhook-only mode.');
}

// ─── Rate limiting (in-memory, per-IP) ───
const rateLimits = new Map();
const RATE_WINDOW = 60_000; // 1 minute
const RATE_MAX_READ = 120;  // 120 reads/min
const RATE_MAX_WRITE = 10;  // 10 writes/min

function checkRate(ip, isWrite = false) {
  const now = Date.now();
  const key = `${ip}:${isWrite ? 'w' : 'r'}`;
  const entry = rateLimits.get(key);
  const max = isWrite ? RATE_MAX_WRITE : RATE_MAX_READ;
  if (!entry || now - entry.start > RATE_WINDOW) {
    rateLimits.set(key, { start: now, count: 1 });
    return true;
  }
  if (entry.count >= max) return false;
  entry.count++;
  return true;
}

// Clean up rate limit map every 5 minutes
setInterval(() => {
  const now = Date.now();
  for (const [key, entry] of rateLimits) {
    if (now - entry.start > RATE_WINDOW) rateLimits.delete(key);
  }
}, 300_000);

/** Validate webhook/ingest auth header */
function requireWebhookAuth(req, reply) {
  if (!WEBHOOK_SECRET) {
    // No secret configured — reject all write requests for safety
    reply.status(403);
    return { error: 'WEBHOOK_SECRET not configured — node is in read-only mode' };
  }
  const authHeader = req.headers['authorization'] || '';
  if (authHeader !== WEBHOOK_SECRET) {
    reply.status(401);
    return { error: 'Unauthorized' };
  }
  return null; // auth passed
}

const app = Fastify({ logger: true });

// CORS
app.addHook('onRequest', (req, reply, done) => {
  reply.header('Access-Control-Allow-Origin', '*');
  reply.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  reply.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') {
    reply.status(204).send();
    return;
  }
  done();
});

// ─── Health ───

app.get('/health', () => {
  const mem = process.memoryUsage();
  const stats = db.getStats();
  const peers = db.listPeers();
  return {
    status: 'ok',
    service: 'freezedry-node',
    nodeId: NODE_ID,
    uptime: Math.floor((Date.now() - startTime) / 1000),
    memory: {
      rss: (mem.rss / 1024 / 1024).toFixed(1) + ' MB',
      heap: (mem.heapUsed / 1024 / 1024).toFixed(1) + ' MB',
    },
    indexed: stats,
    peers: peers.length,
  };
});

// ─── Artwork metadata ───

app.get('/artwork/:hash', (req, reply) => {
  const ip = req.ip || 'unknown';
  if (!checkRate(ip)) { reply.status(429); return { error: 'Rate limit exceeded' }; }
  const artwork = db.getArtwork(req.params.hash);
  if (!artwork) return { error: 'Not found', status: 404 };
  return artwork;
});

// ─── List artworks ───

app.get('/artworks', (req, reply) => {
  const ip = req.ip || 'unknown';
  if (!checkRate(ip)) { reply.status(429); return { error: 'Rate limit exceeded' }; }
  const limit = Math.min(parseInt(req.query.limit || '50', 10), 200);
  const offset = parseInt(req.query.offset || '0', 10);
  const artworks = db.listArtworks(limit, offset);
  const stats = db.getStats();
  return { artworks, total: stats.artworks };
});

// ─── Serve cached blob ───

/**
 * Serve cached blob — peer-gated with liveness verification.
 *
 * The metadata (artwork list, pointers, verify) is free — that's the directory.
 * Blob data represents real cost (RPC credits, indexing time, compute).
 * To get blobs, a peer must be:
 *   1. Registered (via /sync/announce)
 *   2. LIVE right now (we ping their /health before serving)
 *
 * This prevents hit-and-run: register, scrape everything, disconnect.
 * If your node isn't reachable, you read the chain yourself.
 *
 * Set BLOB_PUBLIC=true in .env to skip all checks (open cache mode).
 */
const BLOB_PUBLIC = (process.env.BLOB_PUBLIC || 'false') === 'true';

// Cache liveness checks for 5 minutes to avoid hammering peers on every request
const livenessCache = new Map();
const LIVENESS_TTL = 5 * 60 * 1000;

async function isPeerLive(nodeUrl) {
  if (!nodeUrl) return false;

  // Check cache first
  const cached = livenessCache.get(nodeUrl);
  if (cached && Date.now() - cached.time < LIVENESS_TTL) return cached.alive;

  // Ping their /health
  try {
    const resp = await fetch(`${nodeUrl}/health`, { signal: AbortSignal.timeout(5000) });
    const alive = resp.ok;
    livenessCache.set(nodeUrl, { alive, time: Date.now() });
    if (alive) db.upsertPeer(nodeUrl); // refresh last_seen on success
    return alive;
  } catch {
    livenessCache.set(nodeUrl, { alive: false, time: Date.now() });
    return false;
  }
}

app.get('/blob/:hash', async (req, reply) => {
  if (!BLOB_PUBLIC) {
    // Auth bypass for trusted callers
    const authHeader = req.headers['authorization'] || '';
    const isAuthed = WEBHOOK_SECRET && authHeader === WEBHOOK_SECRET;

    if (!isAuthed) {
      // Must be a registered peer AND currently live
      const nodeUrl = req.headers['x-node-url'] || '';
      const peers = db.listPeers();
      const isPeer = nodeUrl && peers.some(p => p.url === nodeUrl);

      if (!isPeer) {
        reply.status(403);
        return {
          error: 'Blob data requires active peer registration',
          hint: 'Use /sync/announce to register, then include X-Node-URL header',
        };
      }

      // Liveness check — is the peer actually running right now?
      const live = await isPeerLive(nodeUrl);
      if (!live) {
        reply.status(403);
        return {
          error: 'Peer node not reachable — blob access requires a live node',
          hint: 'Make sure your node is running and publicly accessible',
          checked: nodeUrl,
        };
      }
    }
  }

  const blob = db.getBlob(req.params.hash);
  if (!blob) {
    reply.status(404);
    return { error: 'Blob not cached' };
  }
  reply.header('Content-Type', 'application/octet-stream');
  reply.header('Content-Length', blob.length);
  reply.header('Cache-Control', 'public, max-age=31536000, immutable');
  return reply.send(blob);
});

// ─── SHA-256 verification ───

app.get('/verify/:hash', (req) => {
  const blob = db.getBlob(req.params.hash);
  if (!blob) return { error: 'Not cached', verified: false };

  const computed = 'sha256:' + createHash('sha256').update(blob).digest('hex');
  const match = computed === req.params.hash;

  return {
    verified: match,
    expected: req.params.hash,
    computed,
    blobSize: blob.length,
  };
});

// ─── Ingest (Vercel push or peer sync) — requires webhook secret ───

app.post('/ingest', async (req, reply) => {
  const authErr = requireWebhookAuth(req, reply);
  if (authErr) return authErr;

  const ip = req.ip || 'unknown';
  if (!checkRate(ip, true)) {
    reply.status(429);
    return { error: 'Rate limit exceeded' };
  }

  const body = req.body;
  if (!body || !body.hash || !body.chunkCount) {
    return { error: 'Missing hash or chunkCount' };
  }

  db.upsertArtwork({
    hash: body.hash,
    chunkCount: body.chunkCount,
    blobSize: body.blobSize || null,
    width: body.width || null,
    height: body.height || null,
    mode: body.mode || 'open',
    network: body.network || 'mainnet',
    pointerSig: body.pointerSig || null,
    chunks: body.chunks || null,
  });

  const cachedCount = db.getChunkCount(body.hash);
  return {
    ok: true,
    hash: body.hash,
    cached: cachedCount,
    expected: body.chunkCount,
    complete: cachedCount >= body.chunkCount,
  };
});

// ─── Helius Webhook (real-time push) ───

app.post('/webhook/helius', async (req, reply) => {
  const authErr = requireWebhookAuth(req, reply);
  if (authErr) return authErr;

  const transactions = Array.isArray(req.body) ? req.body : [req.body];
  let processed = 0;

  for (const tx of transactions) {
    try {
      // Helius enhanced format — look for memo instructions
      const sig = tx.signature;
      if (!sig) continue;

      // Check all instructions for memo data
      const instructions = tx.instructions || [];
      for (const ix of instructions) {
        // Memo Program v2: MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr
        if (ix.programId === 'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr') {
          const memoData = ix.data || '';

          // Pointer memo v1/v2
          if (memoData.startsWith('FREEZEDRY:')) {
            const pointer = parsePointerMemo(memoData);
            if (pointer) {
              db.upsertArtwork({
                hash: pointer.hash,
                chunkCount: pointer.chunkCount,
                blobSize: pointer.blobSize || null,
                width: null,
                height: null,
                mode: 'open',
                network: 'mainnet',
                pointerSig: sig,
                chunks: null,
              });

              app.log.info(`Webhook: discovered pointer for ${pointer.hash} (${pointer.chunkCount} chunks, v${pointer.version})`);
              processed++;
            }
          }
          // Chunk memo: base64-encoded data (not a pointer)
          // These get indexed when we fetch the full artwork via the indexer
        }
      }
    } catch (err) {
      app.log.warn(`Webhook: failed to process tx — ${err.message}`);
    }
  }

  return { ok: true, processed };
});

// ─── Peer Sync endpoints ───

/** Check if the requesting node is a registered active peer */
function requireActivePeer(req, reply) {
  // Check by auth header (preferred) or by IP matching a known peer URL
  const authHeader = req.headers['authorization'] || '';
  if (WEBHOOK_SECRET && authHeader === WEBHOOK_SECRET) return true;

  // Check if requester's origin matches a known peer
  const origin = req.headers['origin'] || req.headers['referer'] || '';
  const peers = db.listPeers();
  const isPeer = peers.some(p => origin.startsWith(p.url) || req.headers['x-node-url'] === p.url);
  if (isPeer) return true;

  reply.status(403);
  return false;
}

app.get('/sync/list', (req, reply) => {
  if (requireActivePeer(req, reply) === false) {
    return { error: 'Peer sync requires active peer registration. Use /sync/announce first.' };
  }
  const limit = Math.min(parseInt(req.query.limit || '100', 10), 500);
  const artworks = db.listArtworks(limit, 0);
  return {
    nodeId: NODE_ID,
    artworks: artworks.map(a => ({
      hash: a.hash,
      chunkCount: a.chunk_count,
      blobSize: a.blob_size,
      complete: a.complete,
    })),
  };
});

app.get('/sync/chunks/:hash', (req, reply) => {
  if (requireActivePeer(req, reply) === false) {
    return { error: 'Peer sync requires active peer registration. Use /sync/announce first.' };
  }
  const blob = db.getBlob(req.params.hash);
  if (!blob) {
    reply.status(404);
    return { error: 'Not cached' };
  }
  // Return base64-encoded blob for peer sync
  return {
    hash: req.params.hash,
    data: blob.toString('base64'),
    size: blob.length,
  };
});

// List known peers (public — enables gossip protocol)
app.get('/nodes', (req, reply) => {
  const ip = req.ip || 'unknown';
  if (!checkRate(ip)) { reply.status(429); return { error: 'Rate limit exceeded' }; }
  const peers = db.listPeers();
  const NODE_URL = process.env.NODE_URL || '';
  return {
    nodeId: NODE_ID,
    self: NODE_URL || null,
    nodes: peers.map(p => ({ url: p.url, lastSeen: p.last_seen })),
  };
});

// Announce a peer — requires webhook secret to prevent fake registrations.
// Bidirectional: when a peer announces, we announce back if we have NODE_URL.
app.post('/sync/announce', async (req, reply) => {
  const authErr = requireWebhookAuth(req, reply);
  if (authErr) return authErr;

  const { url } = req.body || {};
  if (!url) return { error: 'Missing url' };
  db.upsertPeer(url);

  // Bidirectional: announce back to the peer
  const NODE_URL = process.env.NODE_URL || '';
  if (NODE_URL && url !== NODE_URL) {
    try {
      await fetch(`${url}/sync/announce`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': WEBHOOK_SECRET,
        },
        body: JSON.stringify({ url: NODE_URL }),
        signal: AbortSignal.timeout(5000),
      });
    } catch {} // best-effort
  }

  return { ok: true, registered: url };
});

// ─── Start ───

async function start() {
  try {
    await app.listen({ port: PORT, host: '0.0.0.0' });
    app.log.info(`Freeze Dry Node (${NODE_ID}) listening on :${PORT}`);

    // Start the chain indexer
    startIndexer(app.log);
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
}

start();
