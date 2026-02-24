# Freeze Dry Node

A lightweight indexer and cache node for the [Freeze Dry Protocol](https://freezedry.dogame.art) — on-chain art storage on Solana.

Nodes scan the Solana blockchain for `FREEZEDRY:` pointer memos, fetch the associated chunk data, and serve reconstructed artwork blobs over HTTP. The chain is the source of truth; nodes are a discovery and caching layer.

## 5-Minute Setup

### 1. Clone and install

```bash
git clone https://github.com/dogame-art/freezedry-node.git
cd freezedry-node
npm install
```

### 2. Configure environment

```bash
cp .env.example .env
```

Open `.env` and fill in:

| Variable | Required | What it is | Where to get it |
|----------|----------|------------|-----------------|
| `HELIUS_API_KEY` | Yes | Solana RPC access | [helius.dev](https://helius.dev) (free tier works) |
| `WEBHOOK_SECRET` | Yes | Auth token for write endpoints | Generate any random string (e.g. `openssl rand -hex 32`) |
| `SERVER_WALLET` | No | Wallet to index (default: official inscriber) | Change to index a different artist |
| `PORT` | No | HTTP port (default: 3100) | |
| `NODE_ID` | No | Name shown in /health | |

### 3. Start

```bash
npm start
```

You should see:

```
Freeze Dry Node (my-freezedry-node) listening on :3100
Indexer: starting (poll every 120s, wallet: 6ao3hnvK...)
```

The node will immediately begin scanning the chain and caching artwork.

## How It Works

```
Solana Chain                    Your Node                    Users
    |                              |                           |
    |--- FREEZEDRY: pointer ------>| discover artwork           |
    |--- chunk memos ------------->| fetch & cache chunks       |
    |                              |                           |
    |                              |<-- GET /artwork/:hash ----| metadata
    |                              |<-- GET /blob/:hash -------| cached blob
    |                              |<-- GET /verify/:hash -----| SHA-256 proof
```

**Discovery**: The indexer polls `getSignaturesForAddress` for the configured `SERVER_WALLET`, looking for `FREEZEDRY:` pointer memos. Each pointer contains a manifest hash, chunk count, and blob size.

**Caching**: Once a pointer is found, the node fetches all chunk transactions, strips memo headers, and reassembles the `.hyd` blob into SQLite storage.

**Serving**: Clients request artwork via HTTP. Blobs are served with immutable cache headers.

## API Endpoints

### Public (read)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Node status, uptime, indexed count |
| `/artwork/:hash` | GET | Artwork metadata (dimensions, mode, chunk count) |
| `/artworks?limit=50&offset=0` | GET | List all indexed artworks |
| `/blob/:hash` | GET | Raw `.hyd` blob (binary, cached 1 year) |
| `/verify/:hash` | GET | SHA-256 verification against stored blob |
| `/sync/list` | GET | List artworks for peer sync |
| `/sync/chunks/:hash` | GET | Base64 blob for peer sync |

### Protected (require `Authorization` header = `WEBHOOK_SECRET`)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/ingest` | POST | Push artwork metadata from Vercel or peers |
| `/webhook/helius` | POST | Receive real-time Helius webhook pushes |
| `/sync/announce` | POST | Register a peer node URL |

## Helius Plan Auto-Detection

The node auto-detects your Helius plan on startup:

- **Free key**: Uses standard RPC (`getSignaturesForAddress` + `getTransaction`). Works fine, slightly slower.
- **Paid key (Developer+)**: Uses Enhanced API (`/v0/addresses/.../transactions`). ~50x cheaper in credits, faster indexing.

Override with `USE_ENHANCED_API=true|false` in `.env` if needed.

## Architecture

```
freezedry-node/
  src/
    server.js    — Fastify HTTP server + route handlers
    indexer.js   — Chain scanner (polling + webhook modes)
    db.js        — SQLite storage (better-sqlite3)
  .env.example   — Configuration template
  package.json   — 2 dependencies: fastify + better-sqlite3
```

**Database**: SQLite via `better-sqlite3`. The database file (`freezedry.db`) is created automatically on first run. This is a cache — if you delete it, the node re-indexes from the chain.

**Dependencies**: Intentionally minimal. Only 2 runtime deps.

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

### Process manager (pm2)

```bash
npm install -g pm2
pm2 start src/server.js --name freezedry-node
pm2 save
pm2 startup
```

### Helius Webhook (real-time indexing)

Instead of polling every 2 minutes, configure a Helius webhook to push new transactions instantly:

1. Go to [Helius Dashboard](https://dashboard.helius.dev) > Webhooks
2. Create webhook watching the `SERVER_WALLET` address
3. Set URL to `https://node.yourdomain.com/webhook/helius`
4. Set auth header to your `WEBHOOK_SECRET`
5. Select "Enhanced" format

## Security

- All write endpoints (`/ingest`, `/webhook/helius`, `/sync/announce`) require the `Authorization` header to match `WEBHOOK_SECRET`
- If `WEBHOOK_SECRET` is not set, write endpoints return `403` (read-only mode)
- Read endpoints are rate-limited (120 req/min per IP)
- Write endpoints are rate-limited (10 req/min per IP)
- CORS is open (`*`) — nodes are public read APIs by design

## License

MIT
