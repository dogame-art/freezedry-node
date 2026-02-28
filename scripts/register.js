#!/usr/bin/env node
/**
 * register.js — Register this Freeze Dry node with the coordinator (freezedry.art).
 *
 * Uses ed25519 wallet signature for auth (same as all other Freeze Dry APIs).
 * Requires WALLET_KEYPAIR + NODE_URL in .env.
 *
 * Usage:
 *   node scripts/register.js              # register (reads .env)
 *   node scripts/register.js --status     # check registration status
 *   node scripts/register.js --deregister # remove from coordinator
 */

import { readFileSync, existsSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { sign, createPrivateKey } from 'crypto';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, '..');

// ─── Load .env ───
function loadEnv() {
  const envPath = join(ROOT, '.env');
  if (!existsSync(envPath)) {
    console.error('No .env file found. Run scripts/setup.sh first or copy .env.example to .env');
    process.exit(1);
  }
  const lines = readFileSync(envPath, 'utf8').split('\n');
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eq = trimmed.indexOf('=');
    if (eq === -1) continue;
    const key = trimmed.slice(0, eq).trim();
    const val = trimmed.slice(eq + 1).trim();
    if (!process.env[key]) process.env[key] = val;
  }
}
loadEnv();

// ─── Config ───
const COORDINATOR_URL = process.env.COORDINATOR_URL || 'https://freezedry.art';
const NODE_URL = (process.env.NODE_URL || '').trim();
const NODE_ID = (process.env.NODE_ID || 'freezedry-node').trim();
const ROLE = (process.env.ROLE || 'both').toLowerCase();

// ─── Base58 encoding (inline — no dependency) ───
const B58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';

function encodeBase58(bytes) {
  if (bytes.length === 0) return '';
  const digits = [0];
  for (let i = 0; i < bytes.length; i++) {
    let carry = bytes[i];
    for (let j = 0; j < digits.length; j++) {
      carry += digits[j] << 8;
      digits[j] = carry % 58;
      carry = (carry / 58) | 0;
    }
    while (carry > 0) {
      digits.push(carry % 58);
      carry = (carry / 58) | 0;
    }
  }
  let output = '';
  for (let i = 0; i < bytes.length && bytes[i] === 0; i++) output += B58_ALPHABET[0];
  for (let i = digits.length - 1; i >= 0; i--) output += B58_ALPHABET[digits[i]];
  return output;
}

// DER prefix for Ed25519 PKCS8 private key (RFC 8410)
const ED25519_PKCS8_PREFIX = Buffer.from('302e020100300506032b657004220420', 'hex');

// ─── Wallet helpers ───
function getKeypair() {
  const raw = (process.env.WALLET_KEYPAIR || '').trim();
  if (!raw) return null;
  try {
    const bytes = new Uint8Array(JSON.parse(raw));
    if (bytes.length !== 64) return null;
    return {
      seed: bytes.slice(0, 32),
      pubkey: bytes.slice(32, 64),
      pubkeyBase58: encodeBase58(bytes.slice(32, 64)),
    };
  } catch {
    return null;
  }
}

function signMessage(message, seedBytes) {
  const privateKeyObj = createPrivateKey({
    key: Buffer.concat([ED25519_PKCS8_PREFIX, Buffer.from(seedBytes)]),
    format: 'der',
    type: 'pkcs8',
  });
  return sign(null, Buffer.from(message, 'utf-8'), privateKeyObj).toString('base64');
}

// ─── Validation ───
function validate() {
  const issues = [];
  if (!NODE_URL) issues.push('NODE_URL not set in .env — coordinator needs your public URL');
  if (!getKeypair()) issues.push('WALLET_KEYPAIR not set or invalid (need 64-byte JSON array)');
  return issues;
}

// ─── Register ───
async function register() {
  console.log('\n  Freeze Dry Node — Registration\n');
  console.log(`  Coordinator:  ${COORDINATOR_URL}`);
  console.log(`  Node ID:      ${NODE_ID}`);
  console.log(`  Node URL:     ${NODE_URL || '(not set)'}`);
  console.log(`  Role:         ${ROLE}`);

  const kp = getKeypair();
  if (kp) console.log(`  Wallet:       ${kp.pubkeyBase58}`);
  console.log('');

  const issues = validate();
  if (issues.length) {
    console.error('  Issues found:');
    issues.forEach(i => console.error(`    - ${i}`));
    console.error('\n  Fix the above in .env and try again.\n');
    process.exit(1);
  }

  // Sign the registration message
  const timestamp = Math.floor(Date.now() / 1000);
  const message = `FreezeDry:node-register:${NODE_URL}:${timestamp}`;
  const signature = signMessage(message, kp.seed);

  console.log('  Registering with coordinator...');

  try {
    const resp = await fetch(`${COORDINATOR_URL}/api/nodes?action=register`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        nodeId: NODE_ID,
        nodeUrl: NODE_URL,
        role: ROLE,
        walletPubkey: kp.pubkeyBase58,
        message,
        signature,
      }),
      signal: AbortSignal.timeout(15000),
    });

    if (!resp.ok) {
      const text = await resp.text().catch(() => '');
      console.error(`  Registration failed: ${resp.status} ${resp.statusText}`);
      if (text) console.error(`  Response: ${text}`);
      if (resp.status === 502) {
        console.error('\n  The coordinator could not reach your node.');
        console.error('  Make sure your node is running and NODE_URL is publicly accessible.');
      }
      process.exit(1);
    }

    const data = await resp.json();
    console.log('  Registration successful!\n');
    console.log(`  Status:    ${data.status || 'active'}`);
    console.log(`  Node URL:  ${data.nodeUrl}`);
    console.log('');
  } catch (err) {
    if (err.name === 'TimeoutError') {
      console.error('  Registration timed out — coordinator unreachable.');
    } else {
      console.error(`  Registration failed: ${err.message}`);
    }
    process.exit(1);
  }
}

// ─── Status ───
async function checkStatus() {
  console.log('\n  Checking registration status...\n');

  const kp = getKeypair();
  if (!NODE_URL || !kp) {
    console.error('  NODE_URL and WALLET_KEYPAIR required for status check.');
    process.exit(1);
  }

  // Check local health
  const PORT = process.env.PORT || '3100';
  try {
    const localResp = await fetch(`http://localhost:${PORT}/health`, {
      signal: AbortSignal.timeout(5000),
    });
    if (localResp.ok) {
      const health = await localResp.json();
      console.log('  Local node:');
      console.log(`    Status:    ${health.status}`);
      console.log(`    Role:      ${health.role}`);
      console.log(`    Uptime:    ${health.uptime}s`);
      console.log(`    Indexed:   ${health.indexed?.artworks || 0} artworks`);
      console.log(`    Peers:     ${health.peers || 0}`);
      console.log('');
    }
  } catch {
    console.log('  Local node: not running (start with: node src/server.js)\n');
  }

  // Check coordinator status
  try {
    const resp = await fetch(
      `${COORDINATOR_URL}/api/nodes?action=status&nodeUrl=${encodeURIComponent(NODE_URL)}&wallet=${kp.pubkeyBase58}`,
      { signal: AbortSignal.timeout(10000) },
    );
    if (resp.ok) {
      const data = await resp.json();
      console.log('  Coordinator status:');
      console.log(`    Status:      ${data.status}`);
      console.log(`    Role:        ${data.role}`);
      console.log(`    Last seen:   ${data.lastSeen ? new Date(data.lastSeen).toISOString() : 'never'}`);
      console.log(`    Last healthy: ${data.lastHealthy ? new Date(data.lastHealthy).toISOString() : 'never'}`);
      console.log(`    Fail count:  ${data.failCount}`);
      console.log(`    Bad data:    ${data.badDataCount}`);
    } else if (resp.status === 404) {
      console.log('  Coordinator: node not registered');
    } else {
      const text = await resp.text().catch(() => '');
      console.log(`  Coordinator: ${resp.status} — ${text}`);
    }
  } catch {
    console.log('  Coordinator: unreachable');
  }
  console.log('');
}

// ─── Deregister ───
async function deregister() {
  console.log('\n  Deregistering from coordinator...\n');

  const kp = getKeypair();
  if (!NODE_URL || !kp) {
    console.error('  NODE_URL and WALLET_KEYPAIR required for deregistration.');
    process.exit(1);
  }

  const timestamp = Math.floor(Date.now() / 1000);
  const message = `FreezeDry:node-deregister:${NODE_URL}:${timestamp}`;
  const signature = signMessage(message, kp.seed);

  try {
    const resp = await fetch(`${COORDINATOR_URL}/api/nodes?action=deregister`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        nodeUrl: NODE_URL,
        walletPubkey: kp.pubkeyBase58,
        message,
        signature,
      }),
      signal: AbortSignal.timeout(10000),
    });
    if (resp.ok) {
      console.log('  Deregistered successfully.\n');
    } else {
      const text = await resp.text().catch(() => '');
      console.error(`  Deregistration failed: ${resp.status} ${text}\n`);
    }
  } catch (err) {
    console.error(`  Deregistration failed: ${err.message}\n`);
  }
}

// ─── CLI ───
const args = process.argv.slice(2);
if (args.includes('--status') || args.includes('-s')) {
  await checkStatus();
} else if (args.includes('--deregister') || args.includes('-d')) {
  await deregister();
} else {
  await register();
}
