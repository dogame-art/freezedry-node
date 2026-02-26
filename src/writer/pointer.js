/**
 * writer/pointer.js — Send FREEZEDRY pointer memo to finalize inscription.
 * Extracted from worker/src/completion.js (lines 271-320).
 * No Blob patching — just sends the memo and returns the signature.
 */

import {
  Transaction, TransactionInstruction, PublicKey, ComputeBudgetProgram,
} from '@solana/web3.js';
import { MEMO_PROGRAM_ID, MEMO_CHUNK_SIZE } from '../config.js';
import { getServerKeypair } from '../wallet.js';
import { rpcCall, sendWithRetry, fetchPriorityFee } from './rpc.js';

/**
 * Send FREEZEDRY:3: pointer memo.
 * Returns the pointer transaction signature.
 */
export async function sendPointerMemo(job) {
  const serverKeypair = getServerKeypair();
  const payerKey = serverKeypair.publicKey;
  const memoProgramId = new PublicKey(MEMO_PROGRAM_ID);
  const microLamports = await fetchPriorityFee();

  const encFlag = 'o';
  const contentFlag = 'I';
  const manifestFlag = 'c';
  const flags = `${encFlag}${contentFlag}${manifestFlag}`;
  const inscriber = serverKeypair.publicKey.toBase58().substring(0, 8);
  const lastChunkSig = job.signatures[job.signatures.length - 1] || '';
  const pointerData = `FREEZEDRY:3:${job.manifestHash}:${job.chunkCount}:${job.blobSize}:${MEMO_CHUNK_SIZE}:${flags}:${inscriber}:${lastChunkSig}`;

  const blockhash = (await rpcCall('getLatestBlockhash', [{ commitment: 'confirmed' }])).value.blockhash;
  const tx = new Transaction({ recentBlockhash: blockhash, feePayer: payerKey })
    .add(ComputeBudgetProgram.setComputeUnitLimit({ units: 50_000 }))
    .add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports }))
    .add(new TransactionInstruction({
      keys: [{ pubkey: payerKey, isSigner: true, isWritable: false }],
      programId: memoProgramId,
      data: Buffer.from(pointerData),
    }));
  tx.sign(serverKeypair);
  const pointerSig = await sendWithRetry(tx.serialize().toString('base64'));

  console.log(`[Pointer] ${pointerData} -> ${pointerSig}`);
  return pointerSig;
}
