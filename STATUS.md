# Freeze Dry Node — Project Status

**Last updated**: 2026-02-26

## Goal

A self-contained, open-source community node template that anyone can run to:
1. **Index** all Freeze Dry inscriptions from the Solana blockchain
2. **Serve** cached artwork blobs to other nodes and the CDN
3. **Write** inscription jobs (writer role) for the marketplace
4. Strengthen the network through **peer-to-peer replication** and **trustless verification**

## Architecture

```
Chain (Solana memos)
  ↓ poll / webhook
Indexer → SQLite (artworks + chunks)
  ↓
Fastify HTTP server
  ├─ /blob/:hash     → serve cached blobs to peers + CDN
  ├─ /artwork/:hash  → artwork metadata
  ├─ /verify/:hash   → SHA-256 verification
  ├─ /sync/announce  → peer discovery
  └─ /health         → liveness
```

## Current State (v74)

### Working
- Chain indexer: discovers pointers, fetches chunk data, handles both v2 (raw base64) and v3 (`FD:` header) memo formats
- Peer sync: nodes discover each other, exchange blobs with 3-layer trustless verification (HYD header → SHA-256 → chain spot-check)
- Setup wizard (`scripts/setup.sh`): guided env config, keypair generation, auto-generates webhook secret
- Health endpoint, peer liveness checks, rate limiting on announce
- SQLite WAL mode for concurrent reads

### Recently Fixed (2026-02-26/27)
1. **Blob endpoint completeness check** — `/blob/:hash` now only serves complete artworks (was serving partial data causing peer "unverifiable" errors)
2. **Indexer pagination** — `fillChunksEnhanced()` and `fillChunksRPC()` now paginate beyond the 100/1000 API limits (was capping all artworks at exactly 100 chunks)
3. **Peer blob verification** — `fillFromPeers()` now accepts both HYD-format and legacy blobs. For non-HYD blobs (chain-reconstructed), trusts registered liveness-verified peers with size sanity check — no Helius dependency for peer-to-peer sync
4. **Blob storage** — New `storeBlob()` function properly replaces partial chain-sourced chunks with complete peer-synced blobs
5. **Security** — `.gitignore` hardened, `WEBHOOK_SECRET` auto-generated in setup, `/sync/announce` rate-limited
6. **Peer discovery** — `/sync/announce` now uses liveness check instead of webhook auth (each node has unique secret, webhook auth blocked all peer registration)
7. **Results**: GCP 13/14 complete, Oracle 14/15 complete — peer sync working end-to-end

### Known Issues
- **v2 blob reconstruction**: Pre-v3 inscriptions (raw base64, 600B chunks) may produce SHA-256 mismatches when reconstructed from chain — inherent to v2 format (no embedded length field)
- **GCP port 3100 not publicly exposed**: nginx only proxies `:3000` (compress service); node-to-node sync works via registered peers but direct blob access requires peer auth

## Live Nodes

| Node | Host | Role | Helius Plan | Status |
|------|------|------|-------------|--------|
| GCP | `dehydrate.dogame.art` | reader+writer | Paid (Developer) | Active |
| Oracle | `node.dogame.art` | reader | Free | Active |

## What's Next

### Short-term
- [ ] Verify pagination fix: GCP should now index all 7340 chunks for `d1afb245...` (was stuck at 100)
- [ ] Verify peer sync end-to-end: Oracle should fill complete blobs from GCP after GCP finishes indexing
- [ ] Test a new inscription job → confirm it appears on both nodes within one poll cycle

### Medium-term
- [ ] On-chain PDA registry integration (Anchor program deployed to devnet at `6UGJUc28AuCj8a8sjhsVEKbvYHfQECCuJC7i54vk2to`)
- [ ] Jobs marketplace: nodes claim/complete inscription jobs from the on-chain jobs program
- [ ] Stake-based priority: staked nodes get priority in job assignment (0-7s delay vs 15s for unstaked)
- [ ] Writer role implementation: accept delegation from coordinator, inscribe memos, report receipts

### Long-term
- [ ] Mainnet deploy of registry + jobs programs
- [ ] Community node incentive rollout (fee splits: 70% writer / 15% treasury / 15% pool)
- [ ] CDN edge integration with node discovery (already working via `cdn.freezedry.art` CF Worker)
- [ ] Automated testing / CI for the template repo

## Files

| File | Purpose |
|------|---------|
| `src/server.js` | Fastify HTTP server, all endpoints |
| `src/indexer.js` | Chain scanner, peer sync, chunk fetching |
| `src/db.js` | SQLite schema, prepared statements, blob storage |
| `scripts/setup.sh` | Interactive setup wizard |
| `scripts/register.js` | Manual PDA registration (future) |
| `docker-compose.yml` | Docker deployment option |
| `.env.example` | Environment variable template |

## Protocol Versions

| Version | Memo Format | Chunk Size | Header |
|---------|-------------|------------|--------|
| v2 | Raw base64 (800 chars) | 600B decoded | None |
| v3 | `FD:{hash8}:{idx}:{base64}` | 585B payload | `FREEZEDRY:3:` pointer |

The indexer handles both formats transparently via `stripV3Header()`.
