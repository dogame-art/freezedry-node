/**
 * writer/confirm.js — Signature confirmation + surgical retry for dropped chunks.
 * Extracted from worker/src/completion.js (lines 30-144).
 * No Blob/Redis coupling — uses local imports only.
 */

import {
  Transaction, TransactionInstruction, PublicKey, ComputeBudgetProgram,
} from '@solana/web3.js';
import {
  MEMO_PROGRAM_ID, SEND_CONCURRENCY,
  CONFIRM_WAIT_MS, CONFIRM_RETRIES, CONFIRM_RETRY_WAIT,
} from '../config.js';
import { getServerKeypair } from '../wallet.js';
import { rpcCall, sendWithRetry, fetchPriorityFee } from './rpc.js';
import { buildV3ChunkData } from './chunks.js';

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Confirm all signatures on-chain ───────────────────────────────────────────

/**
 * Batch-confirm signatures via getSignatureStatuses.
 * Splits into pages of 256 (Solana RPC limit). Retries up to maxAttempts x 2s.
 * Returns array of indices that never confirmed (empty = all good).
 */
export async function confirmAllSigs(sigs, maxAttempts = 10) {
  const PAGE = 256;
  const confirmed = new Set();

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    for (let i = 0; i < sigs.length; i += PAGE) {
      const page = sigs.slice(i, i + PAGE);
      try {
        const result = await rpcCall('getSignatureStatuses', [page]);
        (result.value || []).forEach((s, j) => {
          if (s && !s.err && (s.confirmationStatus === 'confirmed' || s.confirmationStatus === 'finalized')) {
            confirmed.add(i + j);
          }
        });
      } catch (_) {}
    }
    if (confirmed.size === sigs.length) return [];
    if (attempt < maxAttempts - 1) await sleep(2000);
  }

  return sigs.map((_, i) => i).filter(i => !confirmed.has(i));
}

// ── Surgical retry for failed chunks ──────────────────────────────────────────

/**
 * Re-send specific chunks whose txs were dropped.
 * Returns array of indices that are STILL broken after retry.
 */
export async function surgicalRetry(signatures, allChunks, failedIdxs, manifestHash, microLamports) {
  const serverKeypair = getServerKeypair();
  const payerKey = serverKeypair.publicKey;
  const memoProgramId = new PublicKey(MEMO_PROGRAM_ID);

  const retrySigs = [];
  for (let r = 0; r < failedIdxs.length; r += SEND_CONCURRENCY) {
    const retryBatchIdxs = failedIdxs.slice(r, r + SEND_CONCURRENCY);

    let blockhash = (await rpcCall('getLatestBlockhash', [{ commitment: 'confirmed' }])).value.blockhash;
    let batchSigs = await Promise.all(retryBatchIdxs.map(idx => {
      const chunk = allChunks[idx];
      const tx = new Transaction({ recentBlockhash: blockhash, feePayer: payerKey })
        .add(ComputeBudgetProgram.setComputeUnitLimit({ units: 350_000 }))
        .add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports }))
        .add(new TransactionInstruction({
          keys: [{ pubkey: payerKey, isSigner: true, isWritable: false }],
          programId: memoProgramId,
          data: buildV3ChunkData(chunk, idx, manifestHash),
        }));
      tx.sign(serverKeypair);
      return sendWithRetry(tx.serialize().toString('base64'));
    }));

    // Confirm-per-batch
    await sleep(CONFIRM_WAIT_MS);
    for (let attempt = 0; attempt < CONFIRM_RETRIES; attempt++) {
      const statuses = await rpcCall('getSignatureStatuses', [batchSigs]);
      const needResend = [];
      (statuses.value || []).forEach((s, j) => {
        const ok = s && !s.err && (s.confirmationStatus === 'confirmed' || s.confirmationStatus === 'finalized');
        if (!ok) needResend.push(j);
      });
      if (needResend.length === 0) break;
      if (attempt < CONFIRM_RETRIES - 1) {
        console.log(`[Retry] batch ${r}: ${needResend.length} unconfirmed — re-sending (attempt ${attempt + 1})`);
        blockhash = (await rpcCall('getLatestBlockhash', [{ commitment: 'confirmed' }])).value.blockhash;
        for (const j of needResend) {
          try {
            const idx = retryBatchIdxs[j];
            const chunk = allChunks[idx];
            const tx = new Transaction({ recentBlockhash: blockhash, feePayer: payerKey })
              .add(ComputeBudgetProgram.setComputeUnitLimit({ units: 350_000 }))
              .add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports }))
              .add(new TransactionInstruction({
                keys: [{ pubkey: payerKey, isSigner: true, isWritable: false }],
                programId: memoProgramId,
                data: buildV3ChunkData(chunk, idx, manifestHash),
              }));
            tx.sign(serverKeypair);
            batchSigs[j] = await sendWithRetry(tx.serialize().toString('base64'));
          } catch (e) { console.log(`[Retry] re-send chunk ${retryBatchIdxs[j]} failed: ${e.message}`); }
        }
        await sleep(CONFIRM_RETRY_WAIT);
      }
    }

    retrySigs.push(...batchSigs);
    if (r + SEND_CONCURRENCY < failedIdxs.length) await sleep(400);
  }

  // Patch signatures array
  failedIdxs.forEach((origIdx, i) => { signatures[origIdx] = retrySigs[i]; });

  // Final check
  const stillFailed = await confirmAllSigs(retrySigs, 3);
  return stillFailed.map(i => failedIdxs[i]);
}

// ── Final verification ────────────────────────────────────────────────────────

/**
 * Read back chunk 0 from chain to confirm data is actually there.
 */
export async function verifyChunkZero(sig0) {
  const verifyResult = await rpcCall('getTransaction', [sig0, {
    encoding: 'jsonParsed',
    commitment: 'confirmed',
    maxSupportedTransactionVersion: 0,
  }]);
  if (!verifyResult) throw new Error('chunk 0 tx not found on final verify');
  const memoIxs = verifyResult.transaction?.message?.instructions?.filter(
    ix => (ix.programId || ix.program) === MEMO_PROGRAM_ID
  );
  if (!memoIxs?.length || !memoIxs[0].parsed) throw new Error('chunk 0 memo data missing on final verify');
}
