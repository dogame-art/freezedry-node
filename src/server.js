/**
 * server.js — Freeze Dry Node HTTP server
 * Fastify-based API for serving cached artworks + receiving webhooks.
 * Supports ROLE-based startup: "reader", "writer", or "both" (default).
 */

import Fastify from 'fastify';
import { readFileSync, existsSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { createHash, createHmac, timingSafeEqual } from 'crypto';
import * as db from './db.js';
import { startIndexer } from './indexer.js';
// Writer routes loaded dynamically — requires @solana/web3.js which reader-only nodes may not have

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
const ROLE = (process.env.ROLE || 'both').toLowerCase(); // "reader", "writer", or "both"
const startTime = Date.now();

const isReader = ROLE === 'reader' || ROLE === 'both';
const isWriter = ROLE === 'writer' || ROLE === 'both';

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
  const expected = Buffer.from(WEBHOOK_SECRET);
  const provided = Buffer.from(authHeader);
  if (expected.length !== provided.length || !timingSafeEqual(expected, provided)) {
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
  reply.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-API-Key, X-Node-URL, X-Gossip-Origin');
  if (req.method === 'OPTIONS') {
    reply.status(204).send();
    return;
  }
  done();
});

// ─── Input validation ───

/** Validate hash format: sha256:{64 hex chars} */
function isValidHash(hash) {
  return typeof hash === 'string' && /^sha256:[0-9a-f]{64}$/.test(hash);
}

/** Validate peer URL: must be https://, no internal/private IPs */
function isValidPeerUrl(urlStr) {
  try {
    const u = new URL(urlStr);
    if (u.protocol !== 'https:') return false;
    const host = u.hostname;
    // Block private/internal IPs (SSRF protection)
    if (host === 'localhost' || host === '127.0.0.1' || host === '::1') return false;
    if (host.startsWith('10.')) return false;
    if (host.startsWith('192.168.')) return false;
    if (host.startsWith('169.254.')) return false;  // link-local / cloud metadata
    if (host.startsWith('172.') && /^172\.(1[6-9]|2\d|3[01])\./.test(host)) return false;
    if (host.endsWith('.internal') || host.endsWith('.local')) return false;
    return true;
  } catch {
    return false;
  }
}

// ─── Health ───

app.get('/health', () => {
  const stats = db.getStats();
  return {
    status: 'ok',
    service: 'freezedry-node',
    nodeId: NODE_ID,
    indexed: { artworks: stats.artworks, complete: stats.complete },
    peers: db.listPeers().length,
  };
});

// ─── Artwork metadata ───

app.get('/artwork/:hash', (req, reply) => {
  const ip = req.ip || 'unknown';
  if (!checkRate(ip)) { reply.status(429); return { error: 'Rate limit exceeded' }; }
  if (!isValidHash(req.params.hash)) { reply.status(400); return { error: 'Invalid hash format' }; }
  const artwork = db.getArtwork(req.params.hash);
  if (!artwork) { reply.status(404); return { error: 'Not found' }; }
  return {
    hash: artwork.hash,
    chunkCount: artwork.chunk_count,
    blobSize: artwork.blob_size,
    width: artwork.width,
    height: artwork.height,
    mode: artwork.mode,
    complete: !!artwork.complete,
  };
});

// ─── List artworks ───

app.get('/artworks', (req, reply) => {
  const ip = req.ip || 'unknown';
  if (!checkRate(ip)) { reply.status(429); return { error: 'Rate limit exceeded' }; }
  const limit = Math.min(parseInt(req.query.limit || '50', 10), 200);
  const offset = parseInt(req.query.offset || '0', 10);
  const artworks = db.listArtworks(limit, offset);
  const stats = db.getStats();
  return {
    artworks: artworks.map(a => ({
      hash: a.hash,
      chunkCount: a.chunk_count,
      blobSize: a.blob_size,
      width: a.width,
      height: a.height,
      mode: a.mode,
      complete: !!a.complete,
    })),
    total: stats.artworks,
  };
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
  if (!nodeUrl || !isValidPeerUrl(nodeUrl)) return false;

  // Check cache first
  const cached = livenessCache.get(nodeUrl);
  if (cached && Date.now() - cached.time < LIVENESS_TTL) return cached.alive;

  // Ping their /health (no redirects — SSRF protection)
  try {
    const resp = await fetch(`${nodeUrl}/health`, { signal: AbortSignal.timeout(5000), redirect: 'manual' });
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
  if (!isValidHash(req.params.hash)) { reply.status(400); return { error: 'Invalid hash format' }; }
  if (!BLOB_PUBLIC) {
    // Auth bypass for trusted callers (timing-safe)
    const authHeader = req.headers['authorization'] || '';
    const expected = Buffer.from(WEBHOOK_SECRET || '');
    const provided = Buffer.from(authHeader);
    const isAuthed = WEBHOOK_SECRET && expected.length === provided.length && timingSafeEqual(expected, provided);

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
        };
      }
    }
  }

  // Only serve complete blobs — partial data wastes bandwidth and fails peer verification
  const artwork = db.getArtwork(req.params.hash);
  if (!artwork || !artwork.complete) {
    reply.status(404);
    return { error: artwork ? 'Blob incomplete — still indexing' : 'Blob not cached' };
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

app.get('/verify/:hash', (req, reply) => {
  if (!isValidHash(req.params.hash)) { reply.status(400); return { error: 'Invalid hash format' }; }
  const blob = db.getBlob(req.params.hash);
  if (!blob) return { error: 'Not cached', verified: false };

  // Verify via manifest hash (bytes 17-48 of HYD header), not SHA-256(entire blob).
  // The pointer/work-record hash is the manifest hash — a content hash embedded during .hyd creation.
  const result = { expected: req.params.hash, blobSize: blob.length };
  const HYD_MAGIC = blob.length >= 49 && blob[0] === 0x48 && blob[1] === 0x59 && blob[2] === 0x44 && blob[3] === 0x01;

  if (HYD_MAGIC) {
    const manifestHash = 'sha256:' + Buffer.from(blob.slice(17, 49)).toString('hex');
    result.verified = manifestHash === req.params.hash;
    result.manifestHash = manifestHash;
    result.method = 'hyd-header';
  } else {
    // Non-HYD blob fallback: SHA-256 of entire content
    const computed = 'sha256:' + createHash('sha256').update(blob).digest('hex');
    result.verified = computed === req.params.hash;
    result.computed = computed;
    result.method = 'sha256-full';
  }

  return result;
});

// ─── Blob repair — verify all blobs and re-index corrupt ones ───

app.get('/repair', async (req, reply) => {
  // Auth: webhook secret required (destructive — resets corrupt blobs)
  const authErr = requireWebhookAuth(req, reply);
  if (authErr) return authErr;

  app.log.info('Repair: starting blob verification scan...');
  const result = db.repairCorruptBlobs();
  app.log.info(`Repair: ${result.verified} verified, ${result.corrupt} corrupt (reset), ${result.missing} missing — total ${result.total}`);

  return {
    ok: true,
    ...result,
    message: result.corrupt > 0
      ? `Reset ${result.corrupt} corrupt blob(s) for re-index from chain. Next indexer cycle will refetch.`
      : 'All blobs verified — no corruption found.',
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
  const complete = cachedCount >= body.chunkCount;

  // Gossip to peers when blob is complete
  if (complete) {
    gossipBlob(body.hash, body.chunkCount, []).catch(() => {});
  }

  return {
    ok: true,
    hash: body.hash,
    cached: cachedCount,
    expected: body.chunkCount,
    complete,
  };
});

// ─── Gossip (epidemic blob propagation) ───

const NODE_URL = process.env.NODE_URL || '';
const GOSSIP_TIMEOUT = 10_000; // 10s per peer

/**
 * Push a complete blob to all known peers that haven't seen it yet.
 * Fire-and-forget — failures are silent (peers will catch up via indexer).
 * @param {string} hash - Blob hash (sha256:...)
 * @param {number} chunkCount - Expected chunk count
 * @param {string[]} origins - Node URLs that already have this blob (loop prevention)
 */
async function gossipBlob(hash, chunkCount, origins) {
  const peers = db.listPeers();
  const originSet = new Set(origins);
  if (NODE_URL) originSet.add(NODE_URL);
  const originHeader = [...originSet].join(',');

  // Filter: skip self, skip nodes that already have it
  const targets = peers.filter(p => !originSet.has(p.url));
  if (targets.length === 0) return;

  // Get blob data to send
  const blob = db.getBlob(hash);
  if (!blob) return;

  const results = await Promise.allSettled(
    targets.map(async (peer) => {
      const resp = await fetch(`${peer.url}/sync/push`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Node-URL': NODE_URL,
          'X-Gossip-Origin': originHeader,
        },
        body: JSON.stringify({
          hash,
          chunkCount,
          data: blob.toString('base64'),
          size: blob.length,
        }),
        signal: AbortSignal.timeout(GOSSIP_TIMEOUT),
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      return peer.url;
    })
  );

  const pushed = results.filter(r => r.status === 'fulfilled').length;
  if (pushed > 0) {
    app.log.info(`Gossip: pushed ${hash.slice(0, 20)}... to ${pushed}/${targets.length} peer(s)`);
  }
}

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
  // Check by auth header (timing-safe comparison)
  const authHeader = req.headers['authorization'] || '';
  const expected = Buffer.from(WEBHOOK_SECRET || '');
  const provided = Buffer.from(authHeader);
  if (WEBHOOK_SECRET && expected.length === provided.length && timingSafeEqual(expected, provided)) {
    return true;
  }

  // Check if X-Node-URL matches a known peer (liveness verified at announce time)
  const nodeUrl = req.headers['x-node-url'] || '';
  if (nodeUrl) {
    const peers = db.listPeers();
    if (peers.some(p => p.url === nodeUrl)) return true;
  }

  reply.status(403);
  return false;
}

app.get('/sync/list', (req, reply) => {
  if (requireActivePeer(req, reply) === false) {
    return { error: 'Peer sync requires active peer registration. Use /sync/announce first.' };
  }
  const limit = Math.min(parseInt(req.query.limit || '100', 10), 500);
  const offset = parseInt(req.query.offset || '0', 10);
  const artworks = db.listArtworks(limit, offset);
  return {
    artworks: artworks.map(a => ({
      hash: a.hash,
      chunkCount: a.chunk_count,
      complete: !!a.complete,
    })),
  };
});

app.get('/sync/chunks/:hash', (req, reply) => {
  if (requireActivePeer(req, reply) === false) {
    return { error: 'Peer sync requires active peer registration. Use /sync/announce first.' };
  }
  if (!isValidHash(req.params.hash)) { reply.status(400); return { error: 'Invalid hash format' }; }

  // Only serve complete blobs (same check as /blob/:hash)
  const artwork = db.getArtwork(req.params.hash);
  if (!artwork || !artwork.complete) {
    reply.status(404);
    return { error: artwork ? 'Blob incomplete' : 'Not cached' };
  }
  const blob = db.getBlob(req.params.hash);
  if (!blob) { reply.status(404); return { error: 'Not cached' }; }
  return {
    hash: req.params.hash,
    data: blob.toString('base64'),
    size: blob.length,
  };
});

// List known peers — peer-gated to prevent network enumeration
app.get('/nodes', (req, reply) => {
  const ip = req.ip || 'unknown';
  if (!checkRate(ip)) { reply.status(429); return { error: 'Rate limit exceeded' }; }
  if (requireActivePeer(req, reply) === false) {
    return { error: 'Peer list requires active peer registration' };
  }
  const peers = db.listPeers();
  return {
    nodeId: NODE_ID,
    count: peers.length,
    nodes: peers.map(p => ({ url: p.url })),
  };
});

// Announce a peer — validated via URL format check + liveness ping.
// Bidirectional: when a peer announces, we announce back if we have NODE_URL.
app.post('/sync/announce', async (req, reply) => {
  const ip = req.headers['x-real-ip'] || req.ip;
  if (!checkRate(ip, true)) {
    reply.status(429);
    return { error: 'Rate limited' };
  }

  const { url } = req.body || {};
  if (!url || typeof url !== 'string') return { error: 'Missing url' };

  // Block internal IPs and non-https URLs (SSRF protection)
  if (!isValidPeerUrl(url)) {
    reply.status(400);
    return { error: 'Invalid peer URL — must be https:// and public IP' };
  }

  // Verify the peer is actually running before registering
  const live = await isPeerLive(url);
  if (!live) {
    reply.status(400);
    return { error: 'Peer not reachable — must be a live Freeze Dry node' };
  }

  db.upsertPeer(url);

  // Bidirectional: announce back to the peer (no auth needed — they'll liveness-check us too)
  if (NODE_URL && url !== NODE_URL) {
    try {
      await fetch(`${url}/sync/announce`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: NODE_URL }),
        signal: AbortSignal.timeout(5000),
      });
    } catch {} // best-effort
  }

  return { ok: true, registered: url };
});

// ─── Gossip push receiver (peer-to-peer blob sync) ───

app.post('/sync/push', async (req, reply) => {
  if (requireActivePeer(req, reply) === false) {
    return { error: 'Gossip push requires active peer registration. Use /sync/announce first.' };
  }

  const { hash, chunkCount, data, size } = req.body || {};
  if (!hash || !chunkCount || !data) {
    reply.status(400);
    return { error: 'Missing hash, chunkCount, or data' };
  }
  if (!isValidHash(hash)) {
    reply.status(400);
    return { error: 'Invalid hash format' };
  }

  // Short-circuit: already have this blob complete
  const existing = db.getArtwork(hash);
  if (existing && existing.complete) {
    return { status: 'already-complete', hash };
  }

  // Decode and verify integrity — HYD blobs use manifest hash (bytes 17-48), not SHA-256(blob)
  const blobBuf = Buffer.from(data, 'base64');
  let hashMatch = false;
  const isHYD = blobBuf.length >= 49 && blobBuf[0] === 0x48 && blobBuf[1] === 0x59 && blobBuf[2] === 0x44 && blobBuf[3] === 0x01;
  if (isHYD) {
    const manifestHash = 'sha256:' + blobBuf.slice(17, 49).toString('hex');
    hashMatch = manifestHash === hash;
  } else {
    const computed = 'sha256:' + createHash('sha256').update(blobBuf).digest('hex');
    hashMatch = computed === hash;
  }
  if (!hashMatch) {
    reply.status(400);
    return { error: 'Hash mismatch — blob integrity check failed', expected: hash.slice(0, 24) };
  }

  // Store the blob
  db.upsertArtwork({
    hash,
    chunkCount,
    blobSize: size || blobBuf.length,
    width: null,
    height: null,
    mode: 'open',
    network: 'mainnet',
    pointerSig: null,
    chunks: null,
  });
  db.storeBlob(hash, blobBuf);

  app.log.info(`Gossip: received ${hash.slice(0, 20)}... (${blobBuf.length}B) from peer`);

  // Forward to remaining peers (accumulate origin list)
  const incomingOrigins = (req.headers['x-gossip-origin'] || '').split(',').filter(Boolean);
  const senderUrl = req.headers['x-node-url'] || '';
  const allOrigins = [...new Set([...incomingOrigins, senderUrl].filter(Boolean))];
  gossipBlob(hash, chunkCount, allOrigins).catch(() => {});

  return { status: 'accepted', hash };
});

// ─── POD (Proof of Delivery) ───
//
// CDN sends HMAC-signed delivery tickets after each blob serve.
// Phase 1: HMAC-SHA256 shared secret (POD_SIGNING_KEY).
// Phase 2: Ed25519 — CDN pubkey in Anchor POD program config PDA,
//          on-chain verification via Ed25519 precompile.

const POD_SIGNING_KEY = process.env.POD_SIGNING_KEY || '';
const POD_FLUSH_INTERVAL = 5 * 60 * 1000; // 5 min batch window for devnet memos

/** Verify HMAC-SHA256 delivery ticket from CDN (timing-safe) */
function verifyPodTicket(ticket) {
  if (!POD_SIGNING_KEY) return false;
  const { hash, nodeId, bytes, timestamp, hmac } = ticket;
  if (!hash || !nodeId || !bytes || !timestamp || !hmac) return false;
  const message = `${hash}|${nodeId}|${bytes}|${timestamp}`;
  const expected = createHmac('sha256', POD_SIGNING_KEY).update(message).digest('hex');
  if (expected.length !== hmac.length) return false;
  return timingSafeEqual(Buffer.from(expected), Buffer.from(hmac));
}

// Receive signed delivery ticket from CDN
// Auth: HMAC ticket IS the authentication — no webhook secret needed.
// The CDN signs the ticket with POD_SIGNING_KEY, proving it's genuine.
app.post('/pod/receipt', (req, reply) => {
  if (!POD_SIGNING_KEY) {
    reply.status(503);
    return { error: 'POD not configured — set POD_SIGNING_KEY' };
  }

  const ticket = req.body;
  if (!verifyPodTicket(ticket)) {
    reply.status(401);
    return { error: 'Invalid POD ticket — HMAC verification failed' };
  }

  // Verify this ticket is for us
  if (ticket.nodeId !== NODE_ID) {
    reply.status(400);
    return { error: 'Ticket nodeId does not match this node' };
  }

  // Reject stale tickets (>1 hour old)
  if (Math.abs(Date.now() - ticket.timestamp) > 3600_000) {
    reply.status(400);
    return { error: 'Ticket expired' };
  }

  db.insertPodReceipt({
    hash: ticket.hash,
    nodeId: ticket.nodeId,
    bytes: ticket.bytes,
    timestamp: ticket.timestamp,
    hmac: ticket.hmac,
  });

  return { ok: true, stored: true };
});

// POD stats — open for debugging
app.get('/pod/stats', (req) => {
  const ip = req.ip || 'unknown';
  if (!checkRate(ip)) return { error: 'Rate limited' };
  return db.getPodStats();
});

// ─── POD devnet memo writer (periodic batch flush) ───

const NODE_WALLET_KEY = process.env.NODE_WALLET_KEY || '';
let _podFlushTimer = null;

async function flushPodReceipts() {
  if (!NODE_WALLET_KEY) return; // no wallet = no on-chain writes

  const receipts = db.getUnsubmittedReceipts(50);
  if (receipts.length === 0) return;

  try {
    // Dynamic import — @solana/web3.js may not be installed on reader-only nodes
    const { Keypair, Transaction, TransactionInstruction, PublicKey, Connection, ComputeBudgetProgram } = await import('@solana/web3.js');
    const MEMO_PROGRAM = new PublicKey('MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr');
    const conn = new Connection('https://api.devnet.solana.com', 'confirmed');
    const kp = Keypair.fromSecretKey(new Uint8Array(JSON.parse(NODE_WALLET_KEY)));

    // Aggregate receipts into POD memo
    const epoch = Math.floor(Date.now() / 3600_000);
    const agg = {};
    for (const r of receipts) {
      if (!agg[r.node_id]) agg[r.node_id] = { count: 0, bytes: 0 };
      agg[r.node_id].count++;
      agg[r.node_id].bytes += r.bytes;
    }
    const parts = Object.entries(agg).map(([id, s]) => `${id}=${s.count}:${s.bytes}`).join('|');
    const memo = `POD:1:${epoch}:${parts}`;

    const tx = new Transaction();
    tx.add(
      ComputeBudgetProgram.setComputeUnitLimit({ units: 50_000 }),
      ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 1_000 }),
      new TransactionInstruction({
        keys: [{ pubkey: kp.publicKey, isSigner: true, isWritable: false }],
        programId: MEMO_PROGRAM,
        data: Buffer.from(memo),
      }),
    );

    const { blockhash, lastValidBlockHeight } = await conn.getLatestBlockhash('confirmed');
    tx.recentBlockhash = blockhash;
    tx.feePayer = kp.publicKey;
    tx.sign(kp);

    const sig = await conn.sendRawTransaction(tx.serialize(), { skipPreflight: false, preflightCommitment: 'confirmed' });
    await conn.confirmTransaction({ signature: sig, blockhash, lastValidBlockHeight }, 'confirmed');

    db.markReceiptsSubmitted(receipts.length, sig);
    app.log.info(`POD: flushed ${receipts.length} receipts → devnet memo ${sig}`);
  } catch (err) {
    app.log.warn(`POD: flush failed — ${err.message}`);
  }
}

function startPodFlusher() {
  if (!NODE_WALLET_KEY) {
    app.log.info('POD: no NODE_WALLET_KEY — receipts stored locally only (no devnet memos)');
    return;
  }
  app.log.info('POD: devnet memo flusher started (every 5 min)');
  _podFlushTimer = setInterval(() => flushPodReceipts().catch(() => {}), POD_FLUSH_INTERVAL);
}

// ─── Marketplace status route ───

const MARKETPLACE_ENABLED = (process.env.MARKETPLACE_ENABLED || 'false') === 'true';
let _claimerStatus = null;
let _attesterStatus = null;

app.get('/marketplace/status', () => {
  return {
    enabled: MARKETPLACE_ENABLED,
    claimer: _claimerStatus ? _claimerStatus() : null,
    attester: _attesterStatus ? _attesterStatus() : null,
  };
});

// ─── Start ───

async function start() {
  try {
    // Register writer routes before listening (if writer role)
    if (isWriter) {
      try {
        const { registerWriterRoutes } = await import('./writer/routes.js');
        registerWriterRoutes(app);
      } catch (err) {
        console.warn(`Writer routes unavailable (${err.message}) — running in reader-only mode`);
      }
    }

    await app.listen({ port: PORT, host: '0.0.0.0' });
    app.log.info(`Freeze Dry Node (${NODE_ID}) listening on :${PORT} [role=${ROLE}]`);

    // Start the chain indexer (if reader role)
    if (isReader) {
      startIndexer(app.log);
    }

    // Start POD devnet memo flusher (if wallet configured)
    if (POD_SIGNING_KEY) {
      startPodFlusher();
    }

    // Start marketplace claimer/attester (if enabled)
    if (MARKETPLACE_ENABLED) {
      if (isWriter) {
        try {
          const { startClaimer, getClaimerStatus } = await import('./writer/claimer.js');
          startClaimer();
          _claimerStatus = getClaimerStatus;
        } catch (err) {
          console.warn(`Marketplace claimer unavailable (${err.message})`);
        }
      }
      if (isReader) {
        try {
          const { startAttester, getAttesterStatus } = await import('./reader/attester.js');
          startAttester();
          _attesterStatus = getAttesterStatus;
        } catch (err) {
          console.warn(`Marketplace attester unavailable (${err.message})`);
        }
      }
    }
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
}

start();
