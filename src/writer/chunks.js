/**
 * writer/chunks.js — Chunk building and splitting for v3 protocol.
 * Copied from worker/src/chunks.js — import path fixed to ../config.js.
 */

import { createHash } from 'crypto';
import { MEMO_PAYLOAD_SIZE } from '../config.js';

/**
 * Build v3 chunk data with self-identifying header: FD:{hash8}:{index}:{base64data}
 * hash8 = first 8 hex chars of manifest hash (after 'sha256:' prefix)
 */
export function buildV3ChunkData(chunk, index, manifestHash) {
  const hash8 = manifestHash.replace('sha256:', '').slice(0, 8);
  const idxStr = String(index).padStart(2, '0');
  const header = `FD:${hash8}:${idxStr}:`;
  return Buffer.from(header + chunk.toString('base64'));
}

/**
 * Split a blob buffer into payload-sized chunks.
 */
export function splitIntoChunks(blobBuffer) {
  const chunks = [];
  for (let off = 0; off < blobBuffer.length; off += MEMO_PAYLOAD_SIZE) {
    chunks.push(blobBuffer.slice(off, Math.min(off + MEMO_PAYLOAD_SIZE, blobBuffer.length)));
  }
  return chunks;
}

/**
 * Extract manifest hash from blob header (bytes 17-48 = 32 raw SHA-256 bytes).
 * Only valid for open mode (byte 4 % 3 === 0); falls back to SHA-256 of full blob.
 */
export function extractManifestHash(blobBuffer) {
  if (blobBuffer.length >= 49 && blobBuffer[4] % 3 === 0) {
    return 'sha256:' + blobBuffer.slice(17, 49).toString('hex');
  }
  return 'sha256:' + createHash('sha256').update(blobBuffer).digest('hex');
}
