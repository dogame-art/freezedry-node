/**
 * writer/routes.js — HTTP endpoints for the writer role.
 * POST /inscribe  — accept inscription job
 * GET  /status/:jobId — check job progress
 * /health extended with writer fields
 */

import { env } from '../config.js';
import { getServerKeypair } from '../wallet.js';
import { jobs, processInscription, cleanupJobs, setShuttingDown } from './inscribe.js';

const CAPACITY = parseInt(env('CAPACITY') || '3', 10);
const API_KEY = env('API_KEY');

/** Validate X-API-Key header */
function requireApiKey(req, reply) {
  if (!API_KEY) {
    reply.status(503);
    return { error: 'Writer API_KEY not configured' };
  }
  const provided = req.headers['x-api-key'] || '';
  if (provided !== API_KEY) {
    reply.status(401);
    return { error: 'Invalid API key' };
  }
  return null;
}

/** Count active (writing) jobs */
function activeJobCount() {
  let count = 0;
  for (const job of jobs.values()) {
    if (job.status === 'writing') count++;
  }
  return count;
}

/**
 * Register writer routes on the Fastify app.
 * @param {import('fastify').FastifyInstance} app
 */
export function registerWriterRoutes(app) {
  // Validate writer can start
  try {
    getServerKeypair();
  } catch (err) {
    console.error(`[Writer] Cannot start: ${err.message}`);
    console.error('[Writer] Set WALLET_KEYPAIR in .env to enable writer role');
    return;
  }

  const walletAddress = getServerKeypair().publicKey.toBase58();
  console.log(`[Writer] Writer role active — wallet: ${walletAddress}, capacity: ${CAPACITY}`);

  // Periodic cleanup of old finished jobs (every 10 minutes)
  setInterval(() => cleanupJobs(), 10 * 60 * 1000);

  // Graceful shutdown
  process.on('SIGTERM', () => setShuttingDown(true));
  process.on('SIGINT', () => setShuttingDown(true));

  // ── POST /inscribe — accept a new inscription job ──────────────────────

  app.post('/inscribe', {
    config: { rawBody: true },
    bodyLimit: 6 * 1024 * 1024, // 6MB — blob + JSON overhead
  }, async (req, reply) => {
    const authErr = requireApiKey(req, reply);
    if (authErr) return authErr;

    const { jobId, blob, chunkCount, hash, callbackUrl } = req.body || {};

    if (!jobId || !blob || !chunkCount || !hash) {
      reply.status(400);
      return { error: 'Missing required fields: jobId, blob, chunkCount, hash' };
    }

    // Check capacity
    const active = activeJobCount();
    if (active >= CAPACITY) {
      reply.status(503);
      return {
        error: 'At capacity',
        activeJobs: active,
        capacity: CAPACITY,
        hint: 'Try another writer or wait',
      };
    }

    // Check for duplicate job
    if (jobs.has(jobId)) {
      const existing = jobs.get(jobId);
      if (existing.status === 'writing') {
        return {
          accepted: false,
          reason: 'Job already in progress',
          status: existing.status,
          chunksWritten: existing.chunksWritten,
          chunksTotal: existing.chunksTotal,
        };
      }
      // Allow re-submission of failed/completed jobs
      jobs.delete(jobId);
    }

    // Decode blob from base64
    let blobBuffer;
    try {
      blobBuffer = Buffer.from(blob, 'base64');
    } catch (err) {
      reply.status(400);
      return { error: 'Invalid base64 blob data' };
    }

    // Estimate time: ~150 chunks per 30s
    const estimatedSeconds = Math.ceil(chunkCount / 150 * 30);

    // Start inscription in background (non-blocking)
    processInscription(jobId, blobBuffer, chunkCount, hash, callbackUrl || null)
      .catch(err => console.error(`[Writer] Job ${jobId} uncaught: ${err.message}`));

    reply.status(202);
    return {
      accepted: true,
      jobId,
      estimatedSeconds,
      activeJobs: active + 1,
      capacity: CAPACITY,
    };
  });

  // ── GET /status/:jobId — check job progress ───────────────────────────

  app.get('/status/:jobId', (req) => {
    const job = jobs.get(req.params.jobId);
    if (!job) {
      return { error: 'Job not found', jobId: req.params.jobId };
    }
    return {
      jobId: job.jobId,
      status: job.status,
      chunksWritten: job.chunksWritten,
      chunksTotal: job.chunksTotal,
      manifestHash: job.manifestHash,
      pointerSig: job.pointerSig || null,
      error: job.error || null,
      startedAt: job.startedAt,
      completedAt: job.completedAt || null,
      ...(job.status === 'complete' ? { signatures: job.signatures } : {}),
    };
  });

  // ── Extend /health with writer info ────────────────────────────────────

  app.get('/writer/health', () => {
    const active = activeJobCount();
    const jobSummaries = [];
    for (const [id, job] of jobs) {
      jobSummaries.push({
        jobId: id,
        status: job.status,
        chunksWritten: job.chunksWritten,
        chunksTotal: job.chunksTotal,
      });
    }
    return {
      role: 'writer',
      wallet: walletAddress,
      activeJobs: active,
      capacity: CAPACITY,
      jobs: jobSummaries,
    };
  });
}
