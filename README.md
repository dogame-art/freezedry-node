# Freeze Dry Node

A lightweight indexer and cache node for the [Freeze Dry Protocol](https://github.com/dogame-art/freezedry-protocol) — on-chain art storage on Solana.

Nodes scan the Solana blockchain for `FREEZEDRY:` pointer memos, fetch the associated chunk data, and serve reconstructed artwork blobs over HTTP. The chain is the source of truth; nodes are a discovery and caching layer.

**Full app**: [freezedry.art](https://freezedry.art) — managed inscriptions, NFT minting, and fast hydration.

## 5-Minute Setup

### Option A: Guided setup (recommended)

```bash
git clone https://github.com/dogame-art/freezedry-node.git
cd freezedry-node
bash scripts/setup.sh
npm start
```

The setup wizard walks you through role selection, Helius key, wallet, and node identity. It generates `.env` with a secure `WEBHOOK_SECRET` automatically.

### Option B: Manual setup

```bash
git clone https://github.com/dogame-art/freezedry-node.git
cd freezedry-node
npm install
cp .env.example .env
# Edit .env — at minimum set HELIUS_API_KEY and generate WEBHOOK_SECRET:
#   openssl rand -hex 32
npm start
```

### Required configuration

| Variable | Required | What it is | Where to get it |
|----------|----------|------------|-----------------|
| `HELIUS_API_KEY` | Yes | Solana RPC access | [helius.dev](https://helius.dev) (free tier works) |
| `WEBHOOK_SECRET` | Yes | Auth for write endpoints | `openssl rand -hex 32` (setup.sh generates this) |
| `NODE_URL` | For peers | Your node's public https URL | Your domain + reverse proxy |
| `WALLET_KEYPAIR` | For registry | Solana keypair JSON array | `solana-keygen new` or setup.sh generates one |

You should see:

```
Freeze Dry Node (my-freezedry-node) listening on :3100
Indexer: starting (poll every 120s, wallet: 6ao3hnvK...)
```

The node immediately begins scanning the chain and caching artwork.

## How It Works

```
Solana Chain                    Your Node                    Peers / CDN
    |                              |                           |
    |--- FREEZEDRY: pointer ------>| discover artwork           |
    |--- chunk memos ------------->| fetch & cache chunks       |
    |                              |                           |
    |                              |<-- GET /artwork/:hash ----| metadata
    |                              |<-- GET /blob/:hash -------| cached blob (peers only)
    |                              |<-- GET /verify/:hash -----| SHA-256 proof
```

**Discovery**: The indexer polls for the configured `SERVER_WALLET`'s memo transactions, looking for `FREEZEDRY:` pointers. Each pointer contains a hash, chunk count, and blob size. Paginated — handles artworks with thousands of chunks.

**Caching**: Once a pointer is found, the node fetches all chunk transactions (paginated beyond API limits), strips memo headers, and stores the raw data in SQLite.

**Peer Sync**: Before reading from chain, the node tries peers first. Peer blob downloads are instant HTTP — no RPC credits needed. Blobs from registered, liveness-verified peers are accepted with size sanity checks.

**Serving**: Peers request blobs via HTTP. Only **complete** blobs are served — partial data is never sent.

## API Endpoints

### Public (no auth)

| Endpoint | Method | Returns |
|----------|--------|---------|
| `/health` | GET | Node status, indexed artwork count, peer count |
| `/artwork/:hash` | GET | Artwork metadata (dimensions, mode, chunk count, complete status) |
| `/artworks?limit=50&offset=0` | GET | List indexed artworks |
| `/verify/:hash` | GET | SHA-256 verification of stored blob |

### Peer-gated (registered peers only)

| Endpoint | Method | How to access |
|----------|--------|---------------|
| `/blob/:hash` | GET | `X-Node-URL` header matching a registered peer |
| `/sync/list` | GET | Same — lists available artworks for sync |
| `/sync/chunks/:hash` | GET | Same — base64 blob for peer sync |
| `/nodes` | GET | Same — list known peers (gossip discovery) |

### Protected (require `WEBHOOK_SECRET`)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/ingest` | POST | Push artwork metadata (coordinator → node) |
| `/webhook/helius` | POST | Receive real-time Helius webhook pushes |

### Peer discovery (public, rate-limited + liveness-verified)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/sync/announce` | POST | Register a peer node URL (must be https, public IP, reachable) |

## Peer Network

Nodes discover each other and sync blobs without using RPC credits.

### Setup

```bash
# In .env — your node's public URL
NODE_URL=https://node.yourdomain.com

# Optional: bootstrap peers (otherwise discovered via coordinator)
PEER_NODES=https://dehydrate.dogame.art,https://node.dogame.art
```

### How Peer Sync Works

```
Your Node                          Peer Node
    |                                  |
    |--- POST /sync/announce --------->| "I exist at this URL"
    |         (liveness check: peer pings your /health)
    |<-- POST /sync/announce ----------| "I exist too" (bidirectional)
    |                                  |
    |--- GET /blob/:hash ------------->| complete blob (fast!)
    |    (peer sync — no RPC needed)   |
```

1. **Announce** — Peer sends its URL. Your node verifies it's a live Freeze Dry node (pings `/health`), checks URL is https + public IP, then registers it
2. **Bidirectional** — Your node announces back automatically
3. **Parallel fill** — When filling incomplete artworks, tries peers first in batches of 4 (instant HTTP). Falls back to chain reads only if no peer has the data
4. **Gossip** — Every ~20 minutes, nodes exchange peer lists to discover new nodes

### Security

- **SSRF protection**: Only `https://` URLs with public IPs accepted. Private ranges (`10.x`, `192.168.x`, `169.254.x`, `.internal`, `.local`) blocked
- **Liveness verification**: Announcing node must respond 200 on `/health` (no redirects)
- **Rate limiting**: 10 announce requests/min per IP
- **Peer-gated data**: Blob data requires active peer registration — no unauthenticated scraping
- **Complete blobs only**: Partial/incomplete data is never served to peers
- **Minimal exposure**: `/health` returns only status + counts. No memory, uptime, or internal details. API responses are explicitly shaped — no raw database rows

## Helius Plan Auto-Detection

The node auto-detects your Helius plan on startup:

- **Free key**: Uses standard RPC (`getSignaturesForAddress` + `getTransaction`). Works fine, slightly slower.
- **Paid key (Developer+)**: Uses Enhanced API. ~50x cheaper in credits, faster indexing.

Override with `USE_ENHANCED_API=true|false` in `.env` if needed.

## Architecture

```
freezedry-node/
  src/
    server.js    — Fastify HTTP server + endpoints
    indexer.js   — Chain scanner + peer sync + gossip
    db.js        — SQLite storage (better-sqlite3, WAL mode)
    config.js    — Protocol constants
    wallet.js    — Keypair loader (writer only, optional)
  scripts/
    setup.sh     — Interactive setup wizard
    register.js  — Manual PDA registration
  .env.example   — Configuration template
```

**Database**: SQLite via `better-sqlite3` with WAL mode for concurrent reads. Created automatically on first run. This is a cache — delete it to re-index from chain.

**Dependencies**: 3 runtime deps: `fastify`, `better-sqlite3`, `@solana/web3.js` (optional — reader-only nodes work without it).

## Production Deployment

### Reverse proxy (nginx)

```nginx
server {
    listen 443 ssl;
    server_name node.yourdomain.com;

    location / {
        proxy_pass http://127.0.0.1:3100;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### systemd service

```ini
[Unit]
Description=Freeze Dry Node
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/freezedry-node
ExecStart=/usr/bin/node src/server.js
Restart=on-failure
MemoryMax=512M
MemoryHigh=400M

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable freezedry-node
sudo systemctl start freezedry-node
```

### Docker

```bash
docker compose up -d
docker compose logs -f
```

### Helius Webhook (real-time indexing)

Instead of polling every 2 minutes, configure a Helius webhook for instant indexing:

1. Go to [Helius Dashboard](https://dashboard.helius.dev) > Webhooks
2. Create webhook watching the `SERVER_WALLET` address
3. Set URL to `https://node.yourdomain.com/webhook/helius`
4. Set auth header to your `WEBHOOK_SECRET`
5. Select "Enhanced" format

## Related

- [freezedry-protocol](https://github.com/dogame-art/freezedry-protocol) — SDK packages + Anchor programs
- [freezedry.art](https://freezedry.art) — Full app with managed infrastructure

## License

MIT
