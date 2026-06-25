# Fly.io: Public Traffic + Private Deploy

Fly runs one `dd_server` app process with two listeners:

- public traffic on `BIND_PUBLIC_ADDR` (`0.0.0.0:8080`)
- private control plane on `BIND_PRIVATE_ADDR` (`[::]:8081`)

Workers are deployed into that running app. They are not separate Fly apps.

## Canonical flow

1. deploy platform container with `flyctl deploy`
2. open WireGuard tunnel to private port with `just fly-proxy <app>`
3. mint a scoped token through the private control plane
4. deploy workers through the public endpoint with that token

Use direct worker-store writes only as recovery/maintenance escape hatch, not normal workflow.

## 1) Create app and volume

```bash
flyctl apps create your-dd-app
flyctl volumes create dd_store --region ams --size 1 --app your-dd-app
```

## 2) Deploy platform

```bash
flyctl deploy --app your-dd-app --config deploy/fly/fly.toml --remote-only --no-cache
```

Helper:

```bash
just fly-deploy your-dd-app
```

Persistent data lives under `/app/store`:

- deployed worker source/config
- KV and memory SQLite files
- cache blobs and indexes

## 3) Configure private auth

Set shared private token for both server and CLI/helpers:

```bash
export DD_PRIVATE_TOKEN=replace-me-with-long-random-secret
```

This value in docs is placeholder only. Replace it with fresh random secret.

## Runtime isolate tuning

`dd_server` defaults the process-wide isolate budget to the host logical CPU
count. Override these values for production capacity planning:

```bash
flyctl secrets set \
  DD_RUNTIME_MAX_GLOBAL_ISOLATES=1 \
  DD_RUNTIME_MAX_ISOLATES_PER_WORKER=8 \
  DD_RUNTIME_MAX_INFLIGHT_PER_ISOLATE=4 \
  DD_RUNTIME_MIN_ISOLATES_PER_WORKER=0 \
  DD_MEMORY_OUTBOX_MAX_CONCURRENT_SHARDS=1 \
  DD_MEMORY_DB_CACHE_MAX_OPEN=256 \
  DD_MEMORY_DB_READ_CONNECTIONS_PER_DATABASE=2 \
  DD_MEMORY_DB_MAX_TOTAL_CONNECTIONS=256 \
  --app your-dd-app
```

The global value is shared by all deployed workers in one process. The
per-worker value is a ceiling; it may be higher than the global value, but one
worker can only reach it when global slots are available. The memory outbox
parallelism value bounds how many physical memory shards can claim, deliver, and
ack durable effects at once. The memory DB connection values bound reusable
per-database reader connections plus the single writer connection per active
database slot.

## 4) Open private tunnel

```bash
just fly-proxy your-dd-app
```

Equivalent direct helper:

```bash
./deploy/fly/proxy-private-deploy.sh your-dd-app 18081 8081
```

## 5) Deploy workers through tunnel

The private control plane can always deploy directly, and remains useful for
local admin work:

```bash
just fly-worker-deploy hello examples/hello.js --public
just fly-worker-deploy preview examples/hello.js --public --temporary
just fly-worker-deploy chat examples/chat-worker/src/worker.js --memory-binding CHAT_ROOM --public --assets-dir examples/chat-worker/assets
```

Temporary workers expire one hour after deploy. Redeploying with `--temporary`
refreshes the hour; redeploying without `--temporary` makes that worker
permanent. Deploying `--temporary` over an existing permanent worker is
rejected.

Equivalent raw CLI:

```bash
cargo run -p cli -- --server http://127.0.0.1:18081 deploy hello examples/hello.js --public
cargo run -p cli -- --server http://127.0.0.1:18081 deploy static-assets-site examples/static-assets-site/worker.js --public --assets-dir examples/static-assets-site/assets
```

## 6) Mint a public token

For CI and GitHub Actions, mint a narrow token once, then store only that
token in the repository secret store. Tokens are hashed at rest by `dd_server`.

Example token for one public worker that needs one memory binding:

```bash
just fly-worker-mint-token \
  --name github-actions-chat \
  --worker chat \
  --public \
  --memory-binding CHAT_ROOM \
  --max-source-bytes 1048576 \
  --max-assets 256 \
  --max-asset-bytes 16777216
```

`--name` is the token id used by `list-tokens`, `get-token`, and
`delete-token`. It must be a unique lowercase, dash-delimited slug such as
`github-actions-chat`; uppercase input is normalized to lowercase. The response
includes `token`. Put that value in `DD_TOKEN` in CI. Omit expiry for a
long-lived token, or add `--expires-in-seconds` and/or `--max-uses` for
short-lived release tokens.

Token admin stays on the private control plane:

```bash
cargo run -p cli -- --server http://127.0.0.1:18081 list-tokens
cargo run -p cli -- --server http://127.0.0.1:18081 get-token github-actions-chat
cargo run -p cli -- --server http://127.0.0.1:18081 delete-token github-actions-chat
```

Deploy a generated Vite config through the public endpoint:

```bash
export DD_TOKEN=dddt_...
cargo run -p cli -- --server https://your-dd-app.fly.dev deploy-config dist/dd.deploy.json
```

For local machines, put the public app URL in `dd.json` or the generated
`dist/dd.deploy.json`, then store the token in the OS credential store:

```json
{
  "base_url": "https://your-dd-app.fly.dev"
}
```

```bash
cargo run -p cli -- auth login
cargo run -p cli -- deploy-config dist/dd.deploy.json
```

Helper:

```bash
DD_TOKEN=dddt_... just fly-worker-public-deploy-config your-dd-app dist/dd.deploy.json
```

## 7) Public routing

Once deployed with `--public`, host routing maps subdomain to worker name:

- `echo.example.com/* -> worker "echo"`

For built-in Fly hostname:

- set `PUBLIC_BASE_DOMAIN=your-dd-app.fly.dev`
- `https://echo.your-dd-app.fly.dev/` maps to worker `echo`

Fly app apex hostname itself is not mapped to worker and returns `404`.

## 8) Custom domains

```bash
flyctl certs add example.com --app your-dd-app
flyctl certs add "*.example.com" --app your-dd-app
```

Then set:

```toml
[env]
PUBLIC_BASE_DOMAIN = "example.com"
```

## Operations

- redeploy platform: `just fly-deploy your-dd-app`
- open tunnel: `just fly-proxy your-dd-app`
- deploy worker through tunnel: `just fly-worker-deploy <name> <file> [flags...]`
- mint token through tunnel: `just fly-worker-mint-token --name <token-name> --worker <worker> --public ...`
- list tokens through tunnel: `just fly-worker-list-tokens`
- delete token through tunnel: `just fly-worker-delete-token <token-name>`
- deploy through public endpoint: `DD_TOKEN=... just fly-worker-public-deploy-config <app> <config>`
- invoke private worker: `cargo run -p cli -- --server http://127.0.0.1:18081 invoke <name> --method GET --path /`

Internal escape hatch:

- `just fly-worker-store-deploy ...` writes directly into persisted worker store and restarts machine
- use only when normal private control plane path is unavailable
