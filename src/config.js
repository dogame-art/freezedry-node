/**
 * config.js — Unified constants for Freeze Dry Node (reader + writer).
 * Extracted from worker/src/config.js, stripped of GCP/Telegram/upload specifics.
 */

// Trim env vars — trailing \n common with copy-paste
export const env = (key) => (process.env[key] || '').trim();

// Solana memo constants
export const MEMO_PROGRAM_ID = 'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr';
export const MEMO_CHUNK_SIZE = 600;
export const V3_HEADER_SIZE = 15;
export const MEMO_PAYLOAD_SIZE = MEMO_CHUNK_SIZE - V3_HEADER_SIZE; // 585B

// Transaction settings
export const TX_BASE_FEE = 5_000;
export const SEND_CONCURRENCY = 25;

// Confirm-per-batch settings
export const CONFIRM_WAIT_MS = 1500;
export const CONFIRM_RETRIES = 3;
export const CONFIRM_RETRY_WAIT = 1500;

// Writer settings
export const PROGRESS_SAVE_INTERVAL = 150; // save progress every N chunks
export const MAX_JOB_RUNTIME_MS = 30 * 60 * 1000; // 30 min hard kill per job
