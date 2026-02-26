#!/usr/bin/env node
/**
 * register.js — Register this Freeze Dry node with the coordinator (freezedry.art).
 *
 * Phase 1: HTTP registration via coordinator API (shared API key).
 * Phase 3: On-chain PDA registration (trustless, drop-in replacement).
 *
 * Usage:
 *   node scripts/register.js              # register (reads .env)
 *   node scripts/register.js --status     # check registration status
 *   node scripts/register.js --deregister # remove from coordinator
 */

import { readFileSync, existsSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { Keypair } from '@solana/web3.js';

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
const API_KEY = (process.env.API_KEY || '').trim();
const NODE_URL = (process.env.NODE_URL || '').trim();
const NODE_ID = (process.env.NODE_ID || 'freezedry-node').trim();
const ROLE = (process.env.ROLE || 'both').toLowerCase();
const PORT = process.env.PORT || '3100';

// ─── Derive wallet pubkey (if writer) ───
function getWalletPubkey() {
  const raw = (process.env.WALLET_KEYPAIR || '').trim();
  if (!raw) return null;
  try {
    const kp = Keypair.fromSecretKey(new Uint8Array(JSON.parse(raw)));
    return kp.publicKey.toBase58();
  } catch {
    return null;
  }
}

// ─── Validation ───
function validate() {
  const issues = [];
  if (!API_KEY) issues.push('API_KEY not set in .env — required for coordinator auth');
  if (!NODE_URL) issues.push('NODE_URL not set in .env — coordinator needs your public URL');
  if (ROLE === 'writer' || ROLE === 'both') {
    const pubkey = getWalletPubkey();
    if (!pubkey) issues.push('WALLET_KEYPAIR not set or invalid — required for writer role');
  }
  return issues;
}

// ─── Register ───
async function register() {
  console.log('\n  Freeze Dry Node — Registration\n');
  console.log(`  Coordinator:  ${COORDINATOR_URL}`);
  console.log(`  Node ID:      ${NODE_ID}`);
  console.log(`  Node URL:     ${NODE_URL || '(not set)'}`);
  console.log(`  Role:         ${ROLE}`);

  const pubkey = getWalletPubkey();
  if (pubkey) console.log(`  Writer Wallet: ${pubkey}`);
  console.log('');

  const issues = validate();
  if (issues.length) {
    console.error('  Issues found:');
    issues.forEach(i => console.error(`    - ${i}`));
    console.error('\n  Fix the above in .env and try again.\n');
    process.exit(1);
  }

  // Build registration payload
  const payload = {
    nodeId: NODE_ID,
    nodeUrl: NODE_URL,
    role: ROLE,
    port: parseInt(PORT, 10),
  };
  if (pubkey) payload.walletPubkey = pubkey;

  console.log('  Registering with coordinator...');

  try {
    const resp = await fetch(`${COORDINATOR_URL}/api/nodes/register`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': API_KEY,
      },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(15000),
    });

    if (!resp.ok) {
      const text = await resp.text().catch(() => '');
      console.error(`  Registration failed: ${resp.status} ${resp.statusText}`);
      if (text) console.error(`  Response: ${text}`);

      // Helpful hints
      if (resp.status === 401 || resp.status === 403) {
        console.error('\n  Check that API_KEY in .env matches the key from the coordinator.');
      }
      if (resp.status === 404) {
        console.error('\n  The coordinator may not have the /api/nodes/register endpoint yet.');
        console.error('  For now, register manually via the Freeze Dry admin panel.');
      }
      process.exit(1);
    }

    const data = await resp.json();
    console.log('  Registration successful!\n');
    console.log(`  Status:    ${data.status || 'active'}`);
    if (data.apiKey) console.log(`  API Key:   ${data.apiKey} (save this if different from yours)`);
    if (data.message) console.log(`  Message:   ${data.message}`);
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

  if (!NODE_URL) {
    console.error('  NODE_URL not set — cannot check status.');
    process.exit(1);
  }

  // First, check local health
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

  // Check if coordinator can reach us
  try {
    const resp = await fetch(`${COORDINATOR_URL}/api/nodes/status?url=${encodeURIComponent(NODE_URL)}`, {
      headers: { 'X-API-Key': API_KEY },
      signal: AbortSignal.timeout(10000),
    });
    if (resp.ok) {
      const data = await resp.json();
      console.log('  Coordinator status:');
      console.log(`    Registered: ${data.registered ? 'yes' : 'no'}`);
      if (data.lastSeen) console.log(`    Last seen:  ${new Date(data.lastSeen).toISOString()}`);
      if (data.role) console.log(`    Role:       ${data.role}`);
    } else if (resp.status === 404) {
      console.log('  Coordinator: status endpoint not available yet (Phase 1)');
    }
  } catch {
    console.log('  Coordinator: unreachable');
  }
  console.log('');
}

// ─── Deregister ───
async function deregister() {
  console.log('\n  Deregistering from coordinator...\n');

  if (!API_KEY || !NODE_URL) {
    console.error('  API_KEY and NODE_URL required for deregistration.');
    process.exit(1);
  }

  try {
    const resp = await fetch(`${COORDINATOR_URL}/api/nodes/deregister`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': API_KEY,
      },
      body: JSON.stringify({ nodeUrl: NODE_URL }),
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
