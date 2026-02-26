#!/usr/bin/env bash
#
# setup.sh — Guided setup for a Freeze Dry node.
# Generates keypair, writes .env, starts the node, and registers.
#
# Usage:
#   bash scripts/setup.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$ROOT/.env"

echo ""
echo "  ============================================"
echo "    Freeze Dry Node — Setup Wizard"
echo "  ============================================"
echo ""

# ─── Step 1: Check prerequisites ───

echo "  [1/6] Checking prerequisites..."

# Check Node.js
if ! command -v node &>/dev/null; then
  echo "  ERROR: Node.js not found. Install from https://nodejs.org (v18+)"
  exit 1
fi
NODE_VER=$(node -e "console.log(process.versions.node.split('.')[0])")
if [ "$NODE_VER" -lt 18 ]; then
  echo "  ERROR: Node.js v18+ required (found v$NODE_VER)"
  exit 1
fi
echo "    Node.js v$(node -v | tr -d 'v') ... OK"

# Check if deps installed
if [ ! -d "$ROOT/node_modules" ]; then
  echo "    Installing dependencies..."
  cd "$ROOT" && npm install --production
fi
echo "    Dependencies ... OK"
echo ""

# ─── Step 2: Choose role ───

echo "  [2/6] Choose your node role:"
echo ""
echo "    1) reader  — Index the chain + serve artwork (no wallet needed)"
echo "    2) writer  — Accept inscription jobs from the coordinator"
echo "    3) both    — Reader + writer (recommended)"
echo ""
read -rp "  Enter 1, 2, or 3 [3]: " ROLE_CHOICE
case "${ROLE_CHOICE:-3}" in
  1) ROLE="reader" ;;
  2) ROLE="writer" ;;
  *) ROLE="both" ;;
esac
echo "    Selected: $ROLE"
echo ""

# ─── Step 3: Helius API key ───

echo "  [3/6] Helius RPC key (get one free at https://helius.dev)"
echo ""
EXISTING_KEY=""
if [ -f "$ENV_FILE" ]; then
  EXISTING_KEY=$(grep -oP '(?<=^HELIUS_API_KEY=).+' "$ENV_FILE" 2>/dev/null || true)
fi
if [ -n "$EXISTING_KEY" ] && [ "$EXISTING_KEY" != "your-helius-api-key-here" ]; then
  echo "    Found existing key: ${EXISTING_KEY:0:8}..."
  read -rp "  Keep existing key? [Y/n]: " KEEP_KEY
  if [[ "${KEEP_KEY:-Y}" =~ ^[Yy] ]]; then
    HELIUS_KEY="$EXISTING_KEY"
  else
    read -rp "  Helius API key: " HELIUS_KEY
  fi
else
  read -rp "  Helius API key: " HELIUS_KEY
fi
if [ -z "$HELIUS_KEY" ]; then
  echo "  ERROR: Helius API key is required."
  exit 1
fi
echo ""

# ─── Step 4: Wallet keypair (writer/both only) ───

WALLET_KEYPAIR=""
WALLET_PUBKEY=""
if [ "$ROLE" = "writer" ] || [ "$ROLE" = "both" ]; then
  echo "  [4/6] Solana wallet keypair"
  echo ""
  echo "    The writer needs a funded Solana wallet to send memo transactions."
  echo "    The keypair is stored ONLY in your local .env file."
  echo ""

  # Check for existing keypair in .env
  EXISTING_KP=""
  if [ -f "$ENV_FILE" ]; then
    EXISTING_KP=$(grep -oP '(?<=^WALLET_KEYPAIR=).+' "$ENV_FILE" 2>/dev/null || true)
  fi

  if [ -n "$EXISTING_KP" ]; then
    # Derive pubkey from existing
    WALLET_PUBKEY=$(node -e "
      const { Keypair } = require('@solana/web3.js');
      try {
        const kp = Keypair.fromSecretKey(new Uint8Array(JSON.parse('$EXISTING_KP')));
        console.log(kp.publicKey.toBase58());
      } catch { console.log('invalid'); }
    " 2>/dev/null || echo "invalid")
    if [ "$WALLET_PUBKEY" = "invalid" ]; then
      echo "    Existing keypair is invalid. Generating a new one..."
      EXISTING_KP=""
    else
      echo "    Found existing wallet: $WALLET_PUBKEY"
      read -rp "  Keep existing wallet? [Y/n]: " KEEP_WALLET
      if [[ "${KEEP_WALLET:-Y}" =~ ^[Yy] ]]; then
        WALLET_KEYPAIR="$EXISTING_KP"
      else
        EXISTING_KP=""
      fi
    fi
  fi

  if [ -z "$EXISTING_KP" ] || [ -z "$WALLET_KEYPAIR" ]; then
    echo ""
    echo "    Options:"
    echo "      1) Generate a new keypair (recommended for fresh setup)"
    echo "      2) Paste an existing keypair (JSON array)"
    echo ""
    read -rp "  Enter 1 or 2 [1]: " KP_CHOICE

    if [ "${KP_CHOICE:-1}" = "2" ]; then
      echo "    Paste your keypair JSON (e.g. [1,2,3,...,64]):"
      read -rp "  > " WALLET_KEYPAIR
      WALLET_PUBKEY=$(node -e "
        const { Keypair } = require('@solana/web3.js');
        try {
          const kp = Keypair.fromSecretKey(new Uint8Array(JSON.parse('$WALLET_KEYPAIR')));
          console.log(kp.publicKey.toBase58());
        } catch { console.log('invalid'); }
      " 2>/dev/null || echo "invalid")
      if [ "$WALLET_PUBKEY" = "invalid" ]; then
        echo "  ERROR: Invalid keypair format."
        exit 1
      fi
    else
      echo "    Generating new Solana keypair..."
      KEYPAIR_JSON=$(node -e "
        const { Keypair } = require('@solana/web3.js');
        const kp = Keypair.generate();
        console.log(JSON.stringify(Array.from(kp.secretKey)));
      " 2>/dev/null)
      WALLET_KEYPAIR="$KEYPAIR_JSON"
      WALLET_PUBKEY=$(node -e "
        const { Keypair } = require('@solana/web3.js');
        const kp = Keypair.fromSecretKey(new Uint8Array($KEYPAIR_JSON));
        console.log(kp.publicKey.toBase58());
      " 2>/dev/null)
      echo ""
      echo "    NEW WALLET: $WALLET_PUBKEY"
      echo ""
      echo "    IMPORTANT: Fund this wallet with SOL before running inscription jobs."
      echo "    Transfer ~0.1 SOL for testing, ~1 SOL for production workloads."
      echo "    Send SOL to: $WALLET_PUBKEY"
    fi
  fi
  echo ""
else
  echo "  [4/6] Wallet keypair ... skipped (reader-only)"
  echo ""
fi

# ─── Step 5: Node identity ───

echo "  [5/6] Node identity"
echo ""
read -rp "  Node ID (friendly name) [freezedry-node]: " NODE_ID
NODE_ID="${NODE_ID:-freezedry-node}"

read -rp "  Public URL (e.g. https://node.yourdomain.com) []: " NODE_URL
NODE_URL="${NODE_URL:-}"

read -rp "  Port [3100]: " PORT
PORT="${PORT:-3100}"

# API key for coordinator auth
API_KEY=""
if [ "$ROLE" = "writer" ] || [ "$ROLE" = "both" ]; then
  echo ""
  echo "    Writer nodes need an API key shared with the coordinator."
  echo "    Get yours at: https://freezedry.art/validator"
  read -rp "  API key (from coordinator) []: " API_KEY
fi
echo ""

# ─── Step 6: Write .env ───

echo "  [6/6] Writing configuration..."

cat > "$ENV_FILE" << ENVEOF
# Freeze Dry Node — Configuration
# Generated by setup.sh on $(date -u +"%Y-%m-%dT%H:%M:%SZ")

# ─── Role ───
ROLE=$ROLE

# ─── Helius RPC ───
HELIUS_API_KEY=$HELIUS_KEY

# ─── Node Identity ───
NODE_ID=$NODE_ID
PORT=$PORT
ENVEOF

# Conditionally add optional fields
if [ -n "$NODE_URL" ]; then
  echo "NODE_URL=$NODE_URL" >> "$ENV_FILE"
fi

if [ -n "$WALLET_KEYPAIR" ]; then
  echo "" >> "$ENV_FILE"
  echo "# ─── Writer ───" >> "$ENV_FILE"
  echo "WALLET_KEYPAIR=$WALLET_KEYPAIR" >> "$ENV_FILE"
  echo "CAPACITY=3" >> "$ENV_FILE"
fi

if [ -n "$API_KEY" ]; then
  echo "API_KEY=$API_KEY" >> "$ENV_FILE"
fi

# Add discovery defaults
cat >> "$ENV_FILE" << ENVEOF

# ─── Discovery ───
SERVER_WALLET=6ao3hnvKQJfmQ94xTMV34uLUP6azVNHzCfip1ic5Nafj
REGISTRY_URL=https://freezedry.art/api/registry
ENVEOF

echo "    .env written to: $ENV_FILE"
echo ""

# ─── Summary ───

echo "  ============================================"
echo "    Setup Complete!"
echo "  ============================================"
echo ""
echo "    Role:       $ROLE"
echo "    Node ID:    $NODE_ID"
echo "    Port:       $PORT"
if [ -n "$NODE_URL" ]; then
  echo "    Public URL: $NODE_URL"
fi
if [ -n "$WALLET_PUBKEY" ]; then
  echo "    Wallet:     $WALLET_PUBKEY"
fi
echo ""
echo "  ─── Next Steps ───"
echo ""
echo "    1. Start the node:"
echo "       npm start"
echo ""
if [ -n "$WALLET_PUBKEY" ]; then
  echo "    2. Fund the writer wallet with SOL:"
  echo "       solana transfer $WALLET_PUBKEY 0.1 --url mainnet-beta"
  echo ""
  echo "    3. Register with the coordinator:"
  echo "       node scripts/register.js"
  echo ""
  echo "    4. Check status:"
  echo "       node scripts/register.js --status"
else
  echo "    2. Check health:"
  echo "       curl http://localhost:$PORT/health"
fi
echo ""
echo "  ─── Docker (alternative) ───"
echo ""
echo "    docker compose up -d"
echo "    docker compose logs -f"
echo ""
