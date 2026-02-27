/**
 * reader/attester.js — On-chain job attestation for reader nodes.
 *
 * Polling loop (runs when MARKETPLACE_ENABLED=true):
 *   1. Poll submitted jobs from freezedry_jobs program
 *   2. For each: check if we already attested (attestation PDA exists)
 *   3. Verify: fetch blob → compute SHA-256 → compare to content_hash
 *   4. Attest(true) if valid, Attest(false) if mismatch
 *
 * Uses tx-builder.js for instruction construction (no Anchor dependency).
 */

import { Connection, PublicKey } from '@solana/web3.js';
import { createHash } from 'crypto';
import { env } from '../config.js';
import { getServerKeypair } from '../wallet.js';
import {
  JOBS_PROGRAM_ID, REGISTRY_PROGRAM_ID,
  JOB_DISC, deriveConfigPDA, deriveNodePDA, deriveAttestationPDA,
  parseJobAccount, buildAttestIx, buildSignedTx,
} from '../chain/tx-builder.js';
import * as db from '../db.js';

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

const POLL_INTERVAL = parseInt(env('CLAIM_POLL_INTERVAL') || '30000', 10);

// Track attestations we've made this session (avoid re-checking PDA existence)
const attestedJobIds = new Set();

let _running = false;
let _pollTimer = null;
let _attestCount = 0;

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

// ── RPC helpers (lightweight — no full rpc.js dependency for reader) ─────────

async function rpcCall(method, params) {
  const url = getJobsRpcUrl();
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
    signal: AbortSignal.timeout(30_000),
  });
  if (!resp.ok) throw new Error(`RPC ${method} HTTP ${resp.status}`);
  const json = await resp.json();
  if (json.error) throw new Error(`RPC ${method}: ${json.error.message}`);
  return json.result;
}

async function sendAndConfirm(txBase64) {
  const sig = await rpcCall('sendTransaction', [txBase64, { skipPreflight: false, preflightCommitment: 'confirmed' }]);
  await sleep(2000);
  const status = await rpcCall('getSignatureStatuses', [[sig]]);
  const s = status.value?.[0];
  if (s?.err) throw new Error(`tx failed: ${JSON.stringify(s.err)}`);
  return sig;
}

// ── Fetch submitted jobs ─────────────────────────────────────────────────────

async function fetchSubmittedJobs() {
  const conn = getConnection();
  const accounts = await conn.getProgramAccounts(JOBS_PROGRAM_ID, {
    filters: [{
      memcmp: { offset: 0, bytes: JOB_DISC.toString('base64'), encoding: 'base64' },
    }],
  });

  const jobs = [];
  for (const { pubkey, account } of accounts) {
    const parsed = parseJobAccount(pubkey, account.data);
    if (parsed && parsed.status === 'submitted') jobs.push(parsed);
  }

  return jobs;
}

// ── Check if we already attested a job ───────────────────────────────────────

async function hasAttested(jobId) {
  if (attestedJobIds.has(jobId)) return true;

  const conn = getConnection();
  const keypair = getServerKeypair();
  const [attestPDA] = deriveAttestationPDA(jobId, keypair.publicKey);
  const info = await conn.getAccountInfo(attestPDA);
  if (info) {
    attestedJobIds.add(jobId);
    return true;
  }
  return false;
}

// ── Fetch and verify blob data ───────────────────────────────────────────────

/**
 * Verify a blob's manifest hash matches the expected content hash.
 * For HYD blobs: reads bytes 17-48 (embedded content hash).
 * For non-HYD: falls back to SHA-256(entire blob).
 */
function verifyBlobManifest(blob, expectedHash) {
  if (blob.length >= 49 && blob[0] === 0x48 && blob[1] === 0x59 && blob[2] === 0x44 && blob[3] === 0x01) {
    const manifestHash = 'sha256:' + Buffer.from(blob.slice(17, 49)).toString('hex');
    return manifestHash === expectedHash;
  }
  const computed = 'sha256:' + createHash('sha256').update(blob).digest('hex');
  return computed === expectedHash;
}

async function verifyJobBlob(job) {
  const hash = job.contentHash;

  // Try local cache first
  const localBlob = db.getBlob(hash);
  if (localBlob) {
    return verifyBlobManifest(localBlob, hash);
  }

  // Try coordinator
  const coordinatorUrl = env('COORDINATOR_URL') || 'https://freezedry.art';
  try {
    const resp = await fetch(
      `${coordinatorUrl}/api/fetch-chain?hash=${encodeURIComponent(hash)}&format=blob`,
      { signal: AbortSignal.timeout(30_000), redirect: 'manual' }
    );
    if (!resp.ok) return null; // can't verify — skip, don't attest false

    const buf = Buffer.from(await resp.arrayBuffer());
    return verifyBlobManifest(buf, hash);
  } catch (err) {
    console.warn(`[Attester] Blob fetch failed for ${hash}: ${err.message}`);
    return null; // indeterminate — skip
  }
}

// ── Attest on-chain ──────────────────────────────────────────────────────────

async function attestJob(job, isValid) {
  const keypair = getServerKeypair();
  const readerPubkey = keypair.publicKey;

  const [configPDA] = deriveConfigPDA();
  const [nodePDA] = deriveNodePDA(readerPubkey);
  const [attestPDA] = deriveAttestationPDA(job.jobId, readerPubkey);
  const jobPDA = job.address;

  const ix = buildAttestIx(jobPDA, configPDA, attestPDA, nodePDA, readerPubkey, isValid);

  const blockhash = (await rpcCall('getLatestBlockhash', [{ commitment: 'confirmed' }])).value.blockhash;
  const txBase64 = buildSignedTx(ix, blockhash, keypair);
  const sig = await sendAndConfirm(txBase64);

  attestedJobIds.add(job.jobId);
  _attestCount++;

  console.log(`[Attester] Attested job #${job.jobId} — valid: ${isValid}, tx: ${sig}`);
  return sig;
}

// ── Main polling loop ────────────────────────────────────────────────────────

async function pollAndAttest() {
  if (!_running) return;

  try {
    const submittedJobs = await fetchSubmittedJobs();
    if (submittedJobs.length === 0) return;

    for (const job of submittedJobs) {
      // Skip if we already attested
      const already = await hasAttested(job.jobId);
      if (already) continue;

      // Skip if writer is us (self-attestation is rejected on-chain anyway)
      const keypair = getServerKeypair();
      if (job.writer.equals(keypair.publicKey)) {
        console.log(`[Attester] Skipping job #${job.jobId} — we are the writer (self-attestation)`);
        attestedJobIds.add(job.jobId); // don't check again
        continue;
      }

      // Verify blob integrity
      const isValid = await verifyJobBlob(job);
      if (isValid === null) {
        // Indeterminate — can't fetch blob. Skip this cycle, try again later.
        console.log(`[Attester] Job #${job.jobId} — blob unavailable, skipping`);
        continue;
      }

      // Attest
      try {
        await attestJob(job, isValid);
      } catch (err) {
        // AlreadyAttested error means PDA exists — mark and move on
        if (err.message.includes('already in use') || err.message.includes('AlreadyAttested')) {
          attestedJobIds.add(job.jobId);
          continue;
        }
        console.error(`[Attester] Failed to attest job #${job.jobId}: ${err.message}`);
      }

      // Small delay between attestations to avoid RPC rate limits
      await sleep(1000);
    }
  } catch (err) {
    console.error(`[Attester] Poll error: ${err.message}`);
  }
}

// ── Public API ───────────────────────────────────────────────────────────────

export function startAttester() {
  if (_running) return;
  _running = true;

  console.log(`[Attester] Starting marketplace attester (poll every ${POLL_INTERVAL / 1000}s)`);

  // Initial poll after a short delay
  setTimeout(() => {
    pollAndAttest();
    _pollTimer = setInterval(pollAndAttest, POLL_INTERVAL);
  }, 8000); // stagger from claimer's 5s delay
}

export function stopAttester() {
  _running = false;
  if (_pollTimer) {
    clearInterval(_pollTimer);
    _pollTimer = null;
  }
  console.log('[Attester] Stopped');
}

export function getAttesterStatus() {
  return {
    running: _running,
    pollIntervalMs: POLL_INTERVAL,
    totalAttestations: _attestCount,
    sessionAttestedJobs: attestedJobIds.size,
  };
}
