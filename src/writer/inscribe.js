/**
 * writer/inscribe.js — Core inscription loop for Freeze Dry writer nodes.
 * Extracted from worker/src/worker.js processJob (lines 389-743).
 *
 * All Redis/Blob coupling removed. State lives in-memory (jobs Map).
 * Progress reported via callback POST to coordinator.
 */

import { createHash } from 'crypto';
import {
  Transaction, TransactionInstruction, PublicKey, ComputeBudgetProgram,
} from '@solana/web3.js';
import {
  MEMO_PROGRAM_ID, SEND_CONCURRENCY,
  CONFIRM_WAIT_MS, CONFIRM_RETRIES, CONFIRM_RETRY_WAIT,
  PROGRESS_SAVE_INTERVAL, MAX_JOB_RUNTIME_MS,
} from '../config.js';
import { getServerKeypair } from '../wallet.js';
import { rpcCall, sendWithRetry, fetchPriorityFee } from './rpc.js';
import { buildV3ChunkData, splitIntoChunks, extractManifestHash } from './chunks.js';
import { confirmAllSigs, surgicalRetry, verifyChunkZero } from './confirm.js';
import { sendPointerMemo } from './pointer.js';

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── In-memory job state ──────────────────────────────────────────────────────

/** @type {Map<string, object>} */
export const jobs = new Map();

// ── Dynamic send concurrency ─────────────────────────────────────────────────

function getEffectiveSendConcurrency() {
  const active = [...jobs.values()].filter(j => j.status === 'writing').length;
  if (active <= 1) return SEND_CONCURRENCY; // 25
  if (active === 2) return 15;
  return 10; // 3+ jobs: 10 each
}

// ── Blockhash cache — refresh every 30s ──────────────────────────────────────

let _cachedBlockhash = null;
let _blockhashFetchedAt = 0;
const BLOCKHASH_REFRESH_MS = 30_000;

async function getFreshBlockhash() {
  const now = Date.now();
  if (_cachedBlockhash && (now - _blockhashFetchedAt) < BLOCKHASH_REFRESH_MS) {
    return _cachedBlockhash;
  }
  const result = await rpcCall('getLatestBlockhash', [{ commitment: 'confirmed' }]);
  _cachedBlockhash = result.value.blockhash;
  _blockhashFetchedAt = Date.now();
  return _cachedBlockhash;
}

// ── Progress callback to coordinator ─────────────────────────────────────────

async function reportProgress(callbackUrl, jobId, status, data) {
  if (!callbackUrl) return;
  try {
    await fetch(callbackUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ jobId, status, ...data }),
      signal: AbortSignal.timeout(5000),
    });
  } catch {} // non-fatal — coordinator also polls /status
}

// ── Graceful shutdown flag ───────────────────────────────────────────────────

let _shuttingDown = false;
export function setShuttingDown(val) { _shuttingDown = val; }

// ── Main inscription function ────────────────────────────────────────────────

/**
 * Process a single inscription job.
 * @param {string} jobId
 * @param {Buffer} blobBuffer - raw .hyd blob
 * @param {number} chunkCount - expected chunk count
 * @param {string} hash - artwork hash (sha256:...)
 * @param {string} callbackUrl - coordinator URL for progress/completion POSTs
 */
export async function processInscription(jobId, blobBuffer, chunkCount, hash, callbackUrl) {
  const startTime = Date.now();

  // Initialize job state
  const job = {
    jobId,
    status: 'writing',
    chunksWritten: 0,
    chunksTotal: chunkCount,
    signatures: [],
    manifestHash: hash,
    blobSize: blobBuffer.length,
    chunkCount,
    startedAt: startTime,
    error: null,
    pointerSig: null,
  };
  jobs.set(jobId, job);

  try {
    const allChunks = splitIntoChunks(blobBuffer);
    const manifestHash = extractManifestHash(blobBuffer);
    job.manifestHash = manifestHash;

    // Verify chunk count matches
    if (allChunks.length !== chunkCount) {
      console.log(`[Job ${jobId}] Chunk count mismatch: got ${allChunks.length}, expected ${chunkCount}. Using actual.`);
      job.chunksTotal = allChunks.length;
    }

    const totalChunks = allChunks.length;
    const serverKeypair = getServerKeypair();
    const payerKey = serverKeypair.publicKey;
    const memoProgramId = new PublicKey(MEMO_PROGRAM_ID);
    const microLamports = await fetchPriorityFee();

    console.log(`[Job ${jobId}] Starting inscription: ${totalChunks} chunks, ${(blobBuffer.length / 1024).toFixed(1)} KB`);

    // ── Main inscription loop ────────────────────────────────────────────
    for (let b = 0; b < totalChunks; ) {
      const sendBatch = getEffectiveSendConcurrency();

      // Check shutdown + runtime limit
      if (_shuttingDown) {
        console.log(`[Job ${jobId}] Shutdown — saving progress`);
        job.status = 'interrupted';
        break;
      }
      if (Date.now() - startTime > MAX_JOB_RUNTIME_MS) {
        console.log(`[Job ${jobId}] Hit ${MAX_JOB_RUNTIME_MS / 60000} min runtime limit`);
        job.status = 'interrupted';
        break;
      }

      const batchChunks = allChunks.slice(b, b + sendBatch);

      // Build + sign + send sub-batch
      let blockhash = await getFreshBlockhash();
      let batchSigs = await Promise.all(batchChunks.map((chunk, j) => {
        const chunkIdx = b + j;
        const tx = new Transaction({ recentBlockhash: blockhash, feePayer: payerKey })
          .add(ComputeBudgetProgram.setComputeUnitLimit({ units: 350_000 }))
          .add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports }))
          .add(new TransactionInstruction({
            keys: [{ pubkey: payerKey, isSigner: true, isWritable: false }],
            programId: memoProgramId,
            data: buildV3ChunkData(chunk, chunkIdx, manifestHash),
          }));
        tx.sign(serverKeypair);
        return sendWithRetry(tx.serialize().toString('base64'));
      }));

      // Confirm sub-batch
      await sleep(CONFIRM_WAIT_MS);
      for (let retry = 0; retry < CONFIRM_RETRIES; retry++) {
        const statuses = await rpcCall('getSignatureStatuses', [batchSigs]);
        const needResend = [];
        (statuses.value || []).forEach((s, j) => {
          const ok = s && !s.err && (s.confirmationStatus === 'confirmed' || s.confirmationStatus === 'finalized');
          if (!ok) needResend.push(j);
        });

        if (needResend.length === 0) break;

        if (retry < CONFIRM_RETRIES - 1) {
          console.log(`[Job ${jobId}] sub-batch ${b}: ${needResend.length} unconfirmed — re-sending (attempt ${retry + 1})`);
          _blockhashFetchedAt = 0; // force refresh
          blockhash = await getFreshBlockhash();
          for (const j of needResend) {
            try {
              const chunkIdx = b + j;
              const tx = new Transaction({ recentBlockhash: blockhash, feePayer: payerKey })
                .add(ComputeBudgetProgram.setComputeUnitLimit({ units: 350_000 }))
                .add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports }))
                .add(new TransactionInstruction({
                  keys: [{ pubkey: payerKey, isSigner: true, isWritable: false }],
                  programId: memoProgramId,
                  data: buildV3ChunkData(batchChunks[j], chunkIdx, manifestHash),
                }));
              tx.sign(serverKeypair);
              batchSigs[j] = await sendWithRetry(tx.serialize().toString('base64'));
            } catch (resendErr) {
              console.log(`[Job ${jobId}] re-send chunk ${b + j} failed: ${resendErr.message}`);
            }
          }
          await sleep(CONFIRM_RETRY_WAIT);
        }
      }

      job.signatures.push(...batchSigs);
      job.chunksWritten = job.signatures.length;

      // Report progress periodically
      if (job.chunksWritten % PROGRESS_SAVE_INTERVAL < sendBatch) {
        console.log(`[Job ${jobId}] Progress: ${job.chunksWritten}/${totalChunks}`);
        reportProgress(callbackUrl, jobId, 'writing', {
          chunksWritten: job.chunksWritten,
          chunksTotal: totalChunks,
        });
      }

      b += sendBatch;
      if (b < totalChunks) await sleep(400);
    }

    // If interrupted, stop here
    if (job.status === 'interrupted') {
      reportProgress(callbackUrl, jobId, 'interrupted', {
        chunksWritten: job.chunksWritten,
        chunksTotal: job.chunksTotal,
        signatures: job.signatures,
      });
      return;
    }

    // Trim excess sigs
    job.signatures = job.signatures.slice(0, chunkCount);
    job.chunksWritten = job.signatures.length;

    // ── Confirmation + completion ────────────────────────────────────────

    console.log(`[Job ${jobId}] All chunks sent — confirming...`);
    let failedIdxs = await confirmAllSigs(job.signatures);

    // Surgical retry for dropped txs
    if (failedIdxs.length > 0) {
      console.log(`[Job ${jobId}] ${failedIdxs.length} dropped txs — surgical retry`);
      const stillBroken = await surgicalRetry(job.signatures, allChunks, failedIdxs, manifestHash, microLamports);

      if (stillBroken.length > 0) {
        console.error(`[Job ${jobId}] ${stillBroken.length} chunks still broken after retry`);
        job.status = 'failed';
        job.error = `${stillBroken.length} chunks broken after retry`;
        reportProgress(callbackUrl, jobId, 'failed', {
          error: job.error,
          chunksWritten: job.chunksWritten,
          chunksTotal: job.chunksTotal,
          signatures: job.signatures,
        });
        return;
      }
    }

    // Final verify — read chunk 0 from chain
    try {
      await verifyChunkZero(job.signatures[0]);
      console.log(`[Job ${jobId}] Final verify passed`);
    } catch (verifyErr) {
      console.error(`[Job ${jobId}] Final verify failed: ${verifyErr.message}`);
      job.status = 'failed';
      job.error = `Final verify failed: ${verifyErr.message}`;
      reportProgress(callbackUrl, jobId, 'failed', {
        error: job.error,
        signatures: job.signatures,
      });
      return;
    }

    // ── Send pointer memo ────────────────────────────────────────────────

    await sleep(3000); // wait after batch to avoid rate limits
    try {
      const pointerSig = await sendPointerMemo(job);
      job.pointerSig = pointerSig;
    } catch (ptrErr) {
      console.error(`[Job ${jobId}] Pointer memo failed (non-fatal): ${ptrErr.message}`);
    }

    // Mark complete
    job.status = 'complete';
    job.completedAt = Date.now();
    job.blobHash = 'sha256:' + createHash('sha256').update(blobBuffer).digest('hex');
    const elapsed = (Date.now() - startTime) / 1000;
    console.log(`[Job ${jobId}] Complete — ${totalChunks} chunks in ${Math.round(elapsed)}s`);

    // Report completion to coordinator
    reportProgress(callbackUrl, jobId, 'complete', {
      chunksWritten: job.chunksWritten,
      chunksTotal: job.chunksTotal,
      signatures: job.signatures,
      manifestHash: job.manifestHash,
      pointerSig: job.pointerSig,
      blobHash: job.blobHash,
      elapsedSeconds: Math.round(elapsed),
    });

  } catch (err) {
    console.error(`[Job ${jobId}] FAILED: ${err.message}`);
    job.status = 'failed';
    job.error = err.message;
    reportProgress(callbackUrl, jobId, 'failed', {
      error: err.message,
      chunksWritten: job.chunksWritten,
      chunksTotal: job.chunksTotal,
      signatures: job.signatures,
    });
  }
}

/**
 * Clean up completed/failed jobs older than maxAge (default 1 hour).
 */
export function cleanupJobs(maxAgeMs = 60 * 60 * 1000) {
  const now = Date.now();
  for (const [id, job] of jobs) {
    if (job.status === 'complete' || job.status === 'failed') {
      const age = now - (job.completedAt || job.startedAt);
      if (age > maxAgeMs) jobs.delete(id);
    }
  }
}
