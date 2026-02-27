/**
 * writer/claimer.js — On-chain job marketplace claimer for writer nodes.
 *
 * Polling loop (runs when MARKETPLACE_ENABLED=true):
 *   1. Poll open jobs from freezedry_jobs program
 *   2. Pick oldest (FIFO by job_id) within local capacity
 *   3. Claim on-chain → fetch blob → inscribe → submit receipt
 *
 * Uses tx-builder.js for instruction construction (no Anchor dependency).
 */

import { Connection, PublicKey } from '@solana/web3.js';
import { createHash } from 'crypto';
import { env } from '../config.js';
import { getServerKeypair } from '../wallet.js';
import { rpcCall, sendWithRetry, fetchPriorityFee } from './rpc.js';
import { processInscription, jobs as activeJobs } from './inscribe.js';
import {
  JOBS_PROGRAM_ID, REGISTRY_PROGRAM_ID,
  JOB_DISC, deriveConfigPDA, deriveJobPDA, deriveNodePDA,
  deriveRegistryConfigPDA, parseNodeAccount, parseRegistryConfig,
  parseJobAccount, buildClaimJobIx, buildSubmitReceiptIx, buildSignedTx,
} from '../chain/tx-builder.js';

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

const POLL_INTERVAL = parseInt(env('CLAIM_POLL_INTERVAL') || '30000', 10);
const CLAIM_TIMEOUT = parseInt(env('CLAIM_TIMEOUT_MS') || '1800000', 10); // 30 min
const MAX_CONCURRENT_CLAIMS = parseInt(env('CAPACITY') || '2', 10);

// ── Priority claim delay (3-tier on-chain stake verification) ────────────────
// Tier 1: Node staked to FreezeDry preferred validator → 0ms (instant claim)
// Tier 2: Node staked to any other validator → 0-7s (scaled by amount, 500 SOL = 0s)
// Tier 3: No verified stake or stale (>7 days) → 15s delay
const TIER3_DELAY_MS     = 15_000;        // unstaked nodes wait 15s
const TIER2_MAX_DELAY_MS = 7_000;         // staked (non-preferred) max delay
const TIER2_ZERO_AT_SOL  = 500;           // 500 SOL delegation = 0s in Tier 2
const STAKE_FRESH_WINDOW = 7 * 24 * 3600; // 7 days in seconds

let _claimDelayMs = TIER3_DELAY_MS; // default to unstaked, updated on startup
let _stakeTier = 'unstaked';         // 'preferred-validator' | 'staked' | 'unstaked'

async function computeClaimDelay() {
  try {
    const keypair = getServerKeypair();
    const [nodePDA] = deriveNodePDA(keypair.publicKey);
    const conn = getConnection();

    // 1. Read NodeAccount PDA to get verified stake fields
    const nodeInfo = await conn.getAccountInfo(nodePDA);
    if (!nodeInfo) {
      console.log(`[Claimer] NodeAccount PDA not found — Tier 3 (${TIER3_DELAY_MS}ms delay)`);
      _stakeTier = 'unstaked';
      return TIER3_DELAY_MS;
    }

    const node = parseNodeAccount(nodePDA, Buffer.from(nodeInfo.data));
    if (!node) {
      console.log(`[Claimer] Failed to parse NodeAccount — Tier 3 (${TIER3_DELAY_MS}ms delay)`);
      _stakeTier = 'unstaked';
      return TIER3_DELAY_MS;
    }

    // 2. Check if stake is verified and fresh
    const now = Math.floor(Date.now() / 1000);
    if (node.verifiedStake === 0 || (now - node.stakeVerifiedAt) > STAKE_FRESH_WINDOW) {
      const reason = node.verifiedStake === 0 ? 'no verified stake' : 'stake verification stale';
      console.log(`[Claimer] ${reason} — Tier 3 (${TIER3_DELAY_MS}ms delay)`);
      _stakeTier = 'unstaked';
      return TIER3_DELAY_MS;
    }

    // 3. Read RegistryConfig PDA for preferred validator
    const [configPDA] = deriveRegistryConfigPDA();
    const configInfo = await conn.getAccountInfo(configPDA);
    let preferredValidator = null;

    if (configInfo) {
      const config = parseRegistryConfig(configPDA, Buffer.from(configInfo.data));
      if (config) preferredValidator = config.preferredValidator;
    }

    // 4. Tier 1: staked to preferred validator
    if (preferredValidator && node.stakeVoter.equals(preferredValidator)) {
      const stakeSol = (node.verifiedStake / 1e9).toFixed(2);
      console.log(`[Claimer] Staked to preferred validator (${stakeSol} SOL) — Tier 1 (0ms delay)`);
      _stakeTier = 'preferred-validator';
      return 0;
    }

    // 5. Tier 2: staked to any validator — delay scales inversely with amount
    const stakeSol = node.verifiedStake / 1e9;
    const ratio = Math.min(1, stakeSol / TIER2_ZERO_AT_SOL);
    const delay = Math.round(TIER2_MAX_DELAY_MS * (1 - ratio));
    console.log(`[Claimer] Staked ${stakeSol.toFixed(2)} SOL to ${node.stakeVoter.toBase58().slice(0, 8)}... — Tier 2 (${delay}ms delay)`);
    _stakeTier = 'staked';
    return delay;

  } catch (err) {
    console.warn(`[Claimer] Could not read stake: ${err.message} — Tier 3 default`);
    _stakeTier = 'unstaked';
    return TIER3_DELAY_MS;
  }
}

// Track claimed jobs in-progress
const claimedJobs = new Map(); // jobId → { startedAt, status, pointerSig }

let _running = false;
let _pollTimer = null;

// ── RPC connection ───────────────────────────────────────────────────────────

function getJobsRpcUrl() {
  return env('JOBS_RPC_URL') || env('NODE_REGISTRY_RPC')
    || 'https://lauretta-raoq23-fast-devnet.helius-rpc.com';
}

let _conn = null;
function getConnection() {
  if (!_conn) _conn = new Connection(getJobsRpcUrl(), 'confirmed');
  return _conn;
}

// ── Fetch open jobs from chain ───────────────────────────────────────────────

async function fetchOpenJobs() {
  const conn = getConnection();
  const accounts = await conn.getProgramAccounts(JOBS_PROGRAM_ID, {
    filters: [{
      memcmp: { offset: 0, bytes: JOB_DISC.toString('base64'), encoding: 'base64' },
    }],
  });

  const jobs = [];
  for (const { pubkey, account } of accounts) {
    const parsed = parseJobAccount(pubkey, account.data);
    if (parsed && parsed.status === 'open') jobs.push(parsed);
  }

  // FIFO: sort by job_id ascending
  jobs.sort((a, b) => a.jobId - b.jobId);
  return jobs;
}

// ── Claim a job on-chain ─────────────────────────────────────────────────────

async function claimJob(job) {
  const keypair = getServerKeypair();
  const writerPubkey = keypair.publicKey;

  const [configPDA] = deriveConfigPDA();
  const [nodePDA] = deriveNodePDA(writerPubkey);
  const jobPDA = job.address; // already a PublicKey from parseJobAccount

  const ix = buildClaimJobIx(jobPDA, configPDA, nodePDA, writerPubkey);

  const blockhash = (await rpcCall('getLatestBlockhash', [{ commitment: 'confirmed' }])).value.blockhash;
  const txBase64 = buildSignedTx(ix, blockhash, keypair);
  const sig = await sendWithRetry(txBase64);

  // Wait for confirmation
  await sleep(2000);
  const status = await rpcCall('getSignatureStatuses', [[sig]]);
  const s = status.value?.[0];
  if (s?.err) throw new Error(`claim_job tx failed: ${JSON.stringify(s.err)}`);

  console.log(`[Claimer] Claimed job #${job.jobId} — tx: ${sig}`);
  return sig;
}

// ── Submit receipt after inscription completes ───────────────────────────────

async function submitReceipt(job, pointerSig) {
  const keypair = getServerKeypair();
  const jobPDA = job.address;

  const ix = buildSubmitReceiptIx(jobPDA, keypair.publicKey, pointerSig);

  const blockhash = (await rpcCall('getLatestBlockhash', [{ commitment: 'confirmed' }])).value.blockhash;
  const txBase64 = buildSignedTx(ix, blockhash, keypair);
  const sig = await sendWithRetry(txBase64);

  // Wait for confirmation
  await sleep(2000);
  const status = await rpcCall('getSignatureStatuses', [[sig]]);
  const s = status.value?.[0];
  if (s?.err) throw new Error(`submit_receipt tx failed: ${JSON.stringify(s.err)}`);

  console.log(`[Claimer] Submitted receipt for job #${job.jobId} — pointer: ${pointerSig}, tx: ${sig}`);
  return sig;
}

// ── Fetch blob for a job ─────────────────────────────────────────────────────

async function fetchJobBlob(job) {
  // The blob URL is stored in the job's content_hash context.
  // The coordinator exposes blobs at known URLs. Try the coordinator API first,
  // then fall back to Vercel Blob direct URL if available.
  const coordinatorUrl = env('COORDINATOR_URL') || 'https://freezedry.art';
  const hash = job.contentHash;

  // Try coordinator fetch-chain endpoint (returns blob data)
  const urls = [
    `${coordinatorUrl}/api/fetch-chain?hash=${encodeURIComponent(hash)}&format=blob`,
  ];

  for (const url of urls) {
    try {
      const resp = await fetch(url, {
        signal: AbortSignal.timeout(30_000),
        redirect: 'manual',
      });
      if (!resp.ok) continue;
      const buf = Buffer.from(await resp.arrayBuffer());

      // Verify manifest hash — HYD blobs use bytes 17-48, non-HYD use SHA-256(blob)
      let hashMatch = false;
      if (buf.length >= 49 && buf[0] === 0x48 && buf[1] === 0x59 && buf[2] === 0x44 && buf[3] === 0x01) {
        const manifestHash = 'sha256:' + Buffer.from(buf.slice(17, 49)).toString('hex');
        hashMatch = manifestHash === hash;
      } else {
        const computed = 'sha256:' + createHash('sha256').update(buf).digest('hex');
        hashMatch = computed === hash;
      }
      if (!hashMatch) {
        console.warn(`[Claimer] Hash mismatch for job #${job.jobId}: expected ${hash.slice(0, 24)}`);
        continue;
      }

      return buf;
    } catch (err) {
      console.warn(`[Claimer] Blob fetch failed from ${url}: ${err.message}`);
    }
  }

  throw new Error(`Could not fetch blob for job #${job.jobId} (hash: ${hash})`);
}

// ── Execute a claimed job (fetch blob → inscribe → submit receipt) ───────────

async function executeClaimedJob(job) {
  const jobId = String(job.jobId);
  claimedJobs.set(job.jobId, { startedAt: Date.now(), status: 'inscribing', pointerSig: null });

  try {
    // Fetch the blob
    console.log(`[Claimer] Fetching blob for job #${job.jobId} (${job.chunkCount} chunks)...`);
    const blobBuffer = await fetchJobBlob(job);

    // Run inscription (reuses existing inscription pipeline)
    const localJobId = `chain-${job.jobId}`;
    await processInscription(localJobId, blobBuffer, job.chunkCount, job.contentHash, null);

    // Get the inscription result
    const result = activeJobs.get(localJobId);
    if (!result || result.status === 'error') {
      throw new Error(result?.error || 'Inscription failed');
    }

    const pointerSig = result.pointerSig;
    if (!pointerSig) {
      throw new Error('Inscription completed but no pointer sig produced');
    }

    // Submit receipt on-chain
    claimedJobs.get(job.jobId).status = 'submitting';
    await submitReceipt(job, pointerSig);

    claimedJobs.get(job.jobId).status = 'completed';
    claimedJobs.get(job.jobId).pointerSig = pointerSig;
    console.log(`[Claimer] Job #${job.jobId} fully completed — pointer: ${pointerSig}`);

    // Clean up after a delay (keep for status queries)
    setTimeout(() => {
      claimedJobs.delete(job.jobId);
      activeJobs.delete(localJobId);
    }, 5 * 60_000);

  } catch (err) {
    console.error(`[Claimer] Job #${job.jobId} failed: ${err.message}`);
    const tracked = claimedJobs.get(job.jobId);
    if (tracked) tracked.status = 'error';

    // Clean up failed job after 2 minutes
    setTimeout(() => claimedJobs.delete(job.jobId), 2 * 60_000);
  }
}

// ── Main polling loop ────────────────────────────────────────────────────────

async function pollAndClaim() {
  if (!_running) return;

  try {
    // Check local capacity
    const activeClaims = [...claimedJobs.values()].filter(
      c => c.status === 'inscribing' || c.status === 'submitting'
    ).length;

    if (activeClaims >= MAX_CONCURRENT_CLAIMS) {
      return; // at capacity
    }

    // Check for stale claims (timeout)
    const now = Date.now();
    for (const [jobId, claim] of claimedJobs) {
      if ((claim.status === 'inscribing' || claim.status === 'submitting')
          && now - claim.startedAt > CLAIM_TIMEOUT) {
        console.warn(`[Claimer] Job #${jobId} timed out after ${Math.round((now - claim.startedAt) / 60_000)}min — marking failed`);
        claim.status = 'timeout';
      }
    }

    // Fetch open jobs
    const openJobs = await fetchOpenJobs();
    if (openJobs.length === 0) return;

    // Pick the oldest job we haven't already claimed
    const alreadyClaimed = new Set(claimedJobs.keys());
    const available = openJobs.filter(j => !alreadyClaimed.has(j.jobId));
    if (available.length === 0) return;

    const target = available[0]; // FIFO — oldest first
    console.log(`[Claimer] Found open job #${target.jobId} (${target.chunkCount} chunks, ${target.escrowLamports / 1e9} SOL escrow)`);

    // Priority delay: high-stake nodes claim immediately, low-stake nodes wait
    // This gives validators first crack at jobs before free-tier nodes
    if (_claimDelayMs > 0) {
      console.log(`[Claimer] Waiting ${_claimDelayMs}ms (stake-based priority delay)...`);
      await sleep(_claimDelayMs);
      // Re-check: job may have been claimed by a higher-priority node during our delay
      if (!_running) return;
    }

    // Claim it on-chain
    await claimJob(target);

    // Execute in background (don't block the polling loop)
    executeClaimedJob(target).catch(err => {
      console.error(`[Claimer] Background execution failed for job #${target.jobId}: ${err.message}`);
    });

  } catch (err) {
    console.error(`[Claimer] Poll error: ${err.message}`);
  }
}

// ── Public API ───────────────────────────────────────────────────────────────

export async function startClaimer() {
  if (_running) return;
  _running = true;

  // Compute stake-based claim delay once on startup
  _claimDelayMs = await computeClaimDelay();

  console.log(`[Claimer] Starting marketplace claimer (poll every ${POLL_INTERVAL / 1000}s, max ${MAX_CONCURRENT_CLAIMS} concurrent, delay ${_claimDelayMs}ms)`);

  // Initial poll after a short delay (let server start up)
  setTimeout(() => {
    pollAndClaim();
    _pollTimer = setInterval(pollAndClaim, POLL_INTERVAL);
  }, 5000);
}

export function stopClaimer() {
  _running = false;
  if (_pollTimer) {
    clearInterval(_pollTimer);
    _pollTimer = null;
  }
  console.log('[Claimer] Stopped');
}

export function getClaimerStatus() {
  const claims = [];
  for (const [jobId, claim] of claimedJobs) {
    claims.push({
      jobId,
      status: claim.status,
      startedAt: claim.startedAt,
      runningMs: Date.now() - claim.startedAt,
      pointerSig: claim.pointerSig,
    });
  }
  return {
    running: _running,
    pollIntervalMs: POLL_INTERVAL,
    maxConcurrent: MAX_CONCURRENT_CLAIMS,
    claimDelayMs: _claimDelayMs,
    stakeTier: _stakeTier,
    activeClaims: claims.filter(c => c.status === 'inscribing' || c.status === 'submitting').length,
    claims,
  };
}
