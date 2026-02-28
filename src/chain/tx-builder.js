/**
 * chain/tx-builder.js — Build freezedry_jobs program instructions without Anchor.
 *
 * Handles PDA derivation, Borsh serialization, and instruction construction
 * for claim_job, submit_receipt, and attest instructions.
 *
 * Also includes account parsers (ported from packages/jobs/src/client.ts)
 * so nodes can read job/config state directly from chain.
 */

import {
  PublicKey, TransactionInstruction, Transaction,
  ComputeBudgetProgram, SystemProgram,
} from '@solana/web3.js';
import { env } from '../config.js';

// ── Program IDs ──────────────────────────────────────────────────────────────

export const JOBS_PROGRAM_ID = new PublicKey(
  env('JOBS_PROGRAM_ID') || 'HNUy6zDap79hGxn9NCjqtHsLmmNPR5RJf8P4zxoPd5M2'
);

export const REGISTRY_PROGRAM_ID = new PublicKey(
  env('REGISTRY_PROGRAM_ID') || '6UGJUc28AuCj8a8sjhsVEKbvYHfQECCuJC7i54vk2to'
);

// ── Instruction discriminators (from freezedry_jobs IDL) ─────────────────────

export const IX_CLAIM_JOB        = Buffer.from([9, 160, 5, 231, 116, 123, 198, 14]);
export const IX_SUBMIT_RECEIPT   = Buffer.from([172, 84, 119, 35, 195, 154, 214, 176]);
export const IX_ATTEST           = Buffer.from([83, 148, 120, 119, 144, 139, 117, 160]);
export const IX_CREATE_JOB       = Buffer.from([178, 130, 217, 110, 100, 27, 82, 119]);
export const IX_RELEASE_PAYMENT  = Buffer.from([24, 34, 191, 86, 145, 160, 183, 233]);

// ── Account discriminators ───────────────────────────────────────────────────

export const CONFIG_DISC = Buffer.from([155, 12, 170, 224, 30, 250, 204, 130]);
export const JOB_DISC    = Buffer.from([91, 16, 162, 5, 45, 210, 125, 65]);
export const ATTEST_DISC = Buffer.from([231, 126, 92, 51, 84, 178, 81, 242]);
export const NODE_DISC   = Buffer.from([125, 166, 18, 146, 195, 127, 86, 220]);

// ── Registry account discriminators ──────────────────────────────────────────

export const REGISTRY_CONFIG_DISC = Buffer.from([23, 118, 10, 246, 173, 231, 243, 156]);

// ── PDA seeds ────────────────────────────────────────────────────────────────

const SEED_CONFIG       = Buffer.from('fd-config');
const SEED_JOB          = Buffer.from('fd-job');
const SEED_ATTEST       = Buffer.from('fd-attest');
const SEED_NODE         = Buffer.from('freeze-node');
const SEED_REG_CONFIG   = Buffer.from('fd-registry-config');

export function deriveConfigPDA(programId = JOBS_PROGRAM_ID) {
  return PublicKey.findProgramAddressSync([SEED_CONFIG], programId);
}

export function deriveJobPDA(jobId, programId = JOBS_PROGRAM_ID) {
  const buf = Buffer.alloc(8);
  buf.writeBigUInt64LE(BigInt(jobId));
  return PublicKey.findProgramAddressSync([SEED_JOB, buf], programId);
}

export function deriveAttestationPDA(jobId, reader, programId = JOBS_PROGRAM_ID) {
  const buf = Buffer.alloc(8);
  buf.writeBigUInt64LE(BigInt(jobId));
  return PublicKey.findProgramAddressSync([SEED_ATTEST, buf, reader.toBuffer()], programId);
}

export function deriveNodePDA(owner, programId = REGISTRY_PROGRAM_ID) {
  return PublicKey.findProgramAddressSync([SEED_NODE, owner.toBuffer()], programId);
}

export function deriveRegistryConfigPDA(programId = REGISTRY_PROGRAM_ID) {
  return PublicKey.findProgramAddressSync([SEED_REG_CONFIG], programId);
}

// ── Status enum ──────────────────────────────────────────────────────────────

export const JOB_STATUSES = ['open', 'claimed', 'submitted', 'completed', 'cancelled', 'expired', 'disputed'];

// ── Account parsers ──────────────────────────────────────────────────────────

export function parseJobAccount(pubkey, data) {
  if (data.length < 8 + 8 + 32) return null;
  if (!data.subarray(0, 8).equals(JOB_DISC)) return null;

  let o = 8;
  const jobId = Number(data.readBigUInt64LE(o)); o += 8;
  const creator = new PublicKey(data.subarray(o, o + 32)); o += 32;
  const writer = new PublicKey(data.subarray(o, o + 32)); o += 32;

  const hashLen = data.readUInt32LE(o); o += 4;
  if (hashLen > 256 || o + hashLen > data.length) return null;
  const contentHash = data.subarray(o, o + hashLen).toString('utf8'); o += hashLen;

  const chunkCount = data.readUInt32LE(o); o += 4;
  const escrowLamports = Number(data.readBigUInt64LE(o)); o += 8;
  const statusVal = data[o]; o += 1;
  const status = JOB_STATUSES[statusVal] || 'open';

  const createdAt = Number(data.readBigInt64LE(o)); o += 8;
  const claimedAt = Number(data.readBigInt64LE(o)); o += 8;
  const submittedAt = Number(data.readBigInt64LE(o)); o += 8;
  const completedAt = Number(data.readBigInt64LE(o)); o += 8;

  const attestationCount = data[o]; o += 1;

  const sigLen = data.readUInt32LE(o); o += 4;
  if (sigLen > 256 || o + sigLen > data.length) return null;
  const pointerSig = data.subarray(o, o + sigLen).toString('utf8'); o += sigLen;

  const bump = data[o]; o += 1;

  const referrer = new PublicKey(data.subarray(o, o + 32));

  return {
    address: pubkey, jobId, creator, writer,
    contentHash, chunkCount, escrowLamports, status,
    createdAt, claimedAt, submittedAt, completedAt,
    attestationCount, pointerSig, bump, referrer,
  };
}

export function parseConfigAccount(pubkey, data) {
  if (data.length < 8 + 32) return null;
  if (!data.subarray(0, 8).equals(CONFIG_DISC)) return null;

  let o = 8;
  const authority = new PublicKey(data.subarray(o, o + 32)); o += 32;
  const treasury = new PublicKey(data.subarray(o, o + 32)); o += 32;
  const registryProgram = new PublicKey(data.subarray(o, o + 32)); o += 32;
  const inscriberFeeBps = data.readUInt16LE(o); o += 2;
  const indexerFeeBps = data.readUInt16LE(o); o += 2;
  const treasuryFeeBps = data.readUInt16LE(o); o += 2;
  const referralFeeBps = data.readUInt16LE(o); o += 2;
  const minAttestations = data[o]; o += 1;
  const jobExpirySeconds = Number(data.readBigInt64LE(o)); o += 8;
  const totalJobsCreated = Number(data.readBigUInt64LE(o)); o += 8;
  const totalJobsCompleted = Number(data.readBigUInt64LE(o)); o += 8;
  const bump = data[o];

  return {
    address: pubkey, authority, treasury, registryProgram,
    inscriberFeeBps, indexerFeeBps, treasuryFeeBps, referralFeeBps,
    minAttestations, jobExpirySeconds,
    totalJobsCreated, totalJobsCompleted, bump,
  };
}

// ── Registry account parsers ─────────────────────────────────────────────────

/**
 * Parse a NodeAccount PDA from the registry program.
 * Layout: 8 disc + 32 wallet + (4+nodeId) + (4+url) + 1 role
 *         + 8 registeredAt + 8 lastHeartbeat + 1 isActive
 *         + 8 artworksIndexed + 8 artworksComplete + 1 bump
 *         + 8 verifiedStake + 32 stakeVoter + 8 stakeVerifiedAt + 16 reserved2
 */
export function parseNodeAccount(pubkey, data) {
  if (data.length < 8 + 32) return null;
  if (!data.subarray(0, 8).equals(NODE_DISC)) return null;

  let o = 8;
  const wallet = new PublicKey(data.subarray(o, o + 32)); o += 32;

  const nodeIdLen = data.readUInt32LE(o); o += 4;
  if (nodeIdLen > 64 || o + nodeIdLen > data.length) return null;
  const nodeId = data.subarray(o, o + nodeIdLen).toString('utf8'); o += nodeIdLen;

  const urlLen = data.readUInt32LE(o); o += 4;
  if (urlLen > 256 || o + urlLen > data.length) return null;
  const url = data.subarray(o, o + urlLen).toString('utf8'); o += urlLen;

  const ROLE_MAP = ['reader', 'writer', 'both'];
  const role = ROLE_MAP[data[o]] || 'reader'; o += 1;

  const registeredAt = Number(data.readBigInt64LE(o)); o += 8;
  const lastHeartbeat = Number(data.readBigInt64LE(o)); o += 8;
  const isActive = data[o] === 1; o += 1;
  const artworksIndexed = Number(data.readBigUInt64LE(o)); o += 8;
  const artworksComplete = Number(data.readBigUInt64LE(o)); o += 8;
  const bump = data[o]; o += 1;

  // Stake verification fields (v2) — zeros = unverified
  let verifiedStake = 0;
  let stakeVoter = PublicKey.default;
  let stakeVerifiedAt = 0;

  if (o + 48 <= data.length) {
    verifiedStake = Number(data.readBigUInt64LE(o)); o += 8;
    stakeVoter = new PublicKey(data.subarray(o, o + 32)); o += 32;
    stakeVerifiedAt = Number(data.readBigInt64LE(o)); o += 8;
  }

  return {
    address: pubkey, wallet, nodeId, url, role,
    registeredAt, lastHeartbeat, isActive,
    artworksIndexed, artworksComplete, bump,
    verifiedStake, stakeVoter, stakeVerifiedAt,
  };
}

/**
 * Parse a RegistryConfig PDA from the registry program.
 * Layout: 8 disc + 32 authority + 32 preferredValidator + 1 bump + 64 reserved
 */
export function parseRegistryConfig(pubkey, data) {
  if (data.length < 8 + 32 + 32 + 1) return null;
  if (!data.subarray(0, 8).equals(REGISTRY_CONFIG_DISC)) return null;
  let o = 8;
  const authority = new PublicKey(data.subarray(o, o + 32)); o += 32;
  const preferredValidator = new PublicKey(data.subarray(o, o + 32)); o += 32;
  const bump = data[o];

  return { address: pubkey, authority, preferredValidator, bump };
}

// ── Registry instruction discriminators ─────────────────────────────────────
// These will be filled from IDL after the first build.
// verify_stake discriminator: sha256("global:verify_stake")[0..8]
export const IX_VERIFY_STAKE = Buffer.from([53, 180, 26, 222, 13, 252, 231, 35]);

/**
 * Build verify_stake instruction for the registry program.
 * Accounts: node (writable), stake_account, owner (signer)
 */
export function buildVerifyStakeIx(nodePDA, stakeAccountPubkey, ownerPubkey) {
  return new TransactionInstruction({
    programId: REGISTRY_PROGRAM_ID,
    keys: [
      { pubkey: nodePDA,             isSigner: false, isWritable: true },
      { pubkey: stakeAccountPubkey,  isSigner: false, isWritable: false },
      { pubkey: ownerPubkey,         isSigner: true,  isWritable: false },
    ],
    data: IX_VERIFY_STAKE,
  });
}

// ── Instruction builders ─────────────────────────────────────────────────────

/**
 * Build claim_job instruction.
 * Accounts: job (writable), config, node_account, writer (signer, writable)
 * No args — just the discriminator.
 */
export function buildClaimJobIx(jobPDA, configPDA, nodePDA, writerPubkey) {
  return new TransactionInstruction({
    programId: JOBS_PROGRAM_ID,
    keys: [
      { pubkey: jobPDA,       isSigner: false, isWritable: true },
      { pubkey: configPDA,    isSigner: false, isWritable: false },
      { pubkey: nodePDA,      isSigner: false, isWritable: false },
      { pubkey: writerPubkey, isSigner: true,  isWritable: true },
    ],
    data: IX_CLAIM_JOB,
  });
}

/**
 * Build submit_receipt instruction.
 * Accounts: job (writable), writer (signer)
 * Args: pointer_sig (string: 4-byte LE len + UTF-8)
 */
export function buildSubmitReceiptIx(jobPDA, writerPubkey, pointerSig) {
  const sigBytes = Buffer.from(pointerSig, 'utf8');
  const data = Buffer.alloc(8 + 4 + sigBytes.length);
  let off = 0;
  IX_SUBMIT_RECEIPT.copy(data, off); off += 8;
  data.writeUInt32LE(sigBytes.length, off); off += 4;
  sigBytes.copy(data, off);

  return new TransactionInstruction({
    programId: JOBS_PROGRAM_ID,
    keys: [
      { pubkey: jobPDA,       isSigner: false, isWritable: true },
      { pubkey: writerPubkey, isSigner: true,  isWritable: false },
    ],
    data,
  });
}

/**
 * Build attest instruction.
 * Accounts: job (writable), config, attestation (writable, init), node_account, reader (signer, writable), system_program
 * Args: is_valid (bool, 1 byte)
 */
export function buildAttestIx(jobPDA, configPDA, attestPDA, nodePDA, readerPubkey, isValid) {
  const data = Buffer.alloc(8 + 1);
  IX_ATTEST.copy(data, 0);
  data[8] = isValid ? 1 : 0;

  return new TransactionInstruction({
    programId: JOBS_PROGRAM_ID,
    keys: [
      { pubkey: jobPDA,       isSigner: false, isWritable: true },
      { pubkey: configPDA,    isSigner: false, isWritable: false },
      { pubkey: attestPDA,    isSigner: false, isWritable: true },
      { pubkey: nodePDA,      isSigner: false, isWritable: false },
      { pubkey: readerPubkey, isSigner: true,  isWritable: true },
      { pubkey: SystemProgram.programId, isSigner: false, isWritable: false },
    ],
    data,
  });
}

/**
 * Build create_job instruction.
 * Accounts: config (writable), job (writable), creator (signer, writable), system_program
 * Args: content_hash (string), chunk_count (u32), escrow_amount (u64), referrer (Pubkey)
 */
export function buildCreateJobIx(configPDA, jobPDA, creatorPubkey, contentHash, chunkCount, escrowAmount, referrerPubkey) {
  const hashBytes = Buffer.from(contentHash, 'utf8');
  const data = Buffer.alloc(8 + 4 + hashBytes.length + 4 + 8 + 32);
  let off = 0;
  IX_CREATE_JOB.copy(data, off); off += 8;
  // String: 4-byte LE length + UTF-8
  data.writeUInt32LE(hashBytes.length, off); off += 4;
  hashBytes.copy(data, off); off += hashBytes.length;
  // u32 chunk_count
  data.writeUInt32LE(chunkCount, off); off += 4;
  // u64 escrow_amount
  data.writeBigUInt64LE(BigInt(escrowAmount), off); off += 8;
  // Pubkey referrer (32 bytes)
  referrerPubkey.toBuffer().copy(data, off);

  return new TransactionInstruction({
    programId: JOBS_PROGRAM_ID,
    keys: [
      { pubkey: configPDA,     isSigner: false, isWritable: true },
      { pubkey: jobPDA,        isSigner: false, isWritable: true },
      { pubkey: creatorPubkey, isSigner: true,  isWritable: true },
      { pubkey: SystemProgram.programId, isSigner: false, isWritable: false },
    ],
    data,
  });
}

/**
 * Build release_payment instruction (permissionless).
 * Accounts: job (writable), config (writable), inscriber (writable), treasury (writable), referrer (writable), signer
 * No args — just the discriminator.
 */
export function buildReleasePaymentIx(jobPDA, configPDA, inscriberPubkey, treasuryPubkey, referrerPubkey, signerPubkey) {
  return new TransactionInstruction({
    programId: JOBS_PROGRAM_ID,
    keys: [
      { pubkey: jobPDA,           isSigner: false, isWritable: true },
      { pubkey: configPDA,        isSigner: false, isWritable: true },
      { pubkey: inscriberPubkey,  isSigner: false, isWritable: true },
      { pubkey: treasuryPubkey,   isSigner: false, isWritable: true },
      { pubkey: referrerPubkey,   isSigner: false, isWritable: true },
      { pubkey: signerPubkey,     isSigner: true,  isWritable: false },
    ],
    data: IX_RELEASE_PAYMENT,
  });
}

/**
 * Wrap an instruction into a signed transaction with priority fees.
 * Returns the serialized base64-encoded transaction.
 */
export function buildSignedTx(instruction, blockhash, keypair, microLamports = 10_000) {
  const tx = new Transaction({ recentBlockhash: blockhash, feePayer: keypair.publicKey })
    .add(ComputeBudgetProgram.setComputeUnitLimit({ units: 200_000 }))
    .add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports }))
    .add(instruction);
  tx.sign(keypair);
  return tx.serialize().toString('base64');
}
