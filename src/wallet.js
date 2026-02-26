/**
 * wallet.js — Server keypair loader for writer operations.
 * Loads from WALLET_KEYPAIR env var (JSON array of 64 bytes).
 */

import { Keypair } from '@solana/web3.js';
import { env } from './config.js';

let _keypair = null;

export function getServerKeypair() {
  if (_keypair) return _keypair;
  const raw = env('WALLET_KEYPAIR');
  if (!raw) throw new Error('WALLET_KEYPAIR env var not set — required for writer role');
  _keypair = Keypair.fromSecretKey(new Uint8Array(JSON.parse(raw)));
  return _keypair;
}
