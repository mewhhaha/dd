# Fly.io: Public Traffic + Private Deploy

Fly runs one `dd_server` app process with two listeners:

- public traffic on `BIND_PUBLIC_ADDR` (`0.0.0.0:8080`)
- private control plane on `BIND_PRIVATE_ADDR` (`[::]:8081`)

Workers are deployed into that running app. They are not separate Fly apps.

## Canonical flow

1. provision the Fly app, volume, runtime secrets, and CI deploy token once
2. merge to `main`; successful CI deploys the platform container
3. open a WireGuard tunnel to the private port with `just fly-proxy <app>`
4. mint a scoped token through the private control plane
5. deploy workers through the public endpoint with that token

## 1) Create app and volume

```bash
flyctl apps create your-dd-app
flyctl volumes create dd_store --region ams --size 1 --app your-dd-app
```

## 2) Configure platform CI

Create an app-scoped token with a practical expiry:

```bash
flyctl tokens create deploy \
  --app your-dd-app \
  --name github-actions \
  --expiry 720h
```

Store the complete token as the `FLY_API_TOKEN` GitHub Actions secret. The
`Deploy Platform` workflow uses the `production` GitHub environment, waits for
the `CI` workflow to pass on `main`, and skips a successful commit if a newer
commit has already reached `main`. Images are labeled with the deployed Git
commit. Protect the `production` environment with required reviewers if
deployments should require manual approval.

The workflow uses the app name and production settings from
`deploy/fly/fly.toml`. Fly deploy tokens are scoped to one app; do not use a
personal authentication token in CI.

To deploy the current `main` commit again without another push, run the
`Deploy Platform` workflow manually from GitHub Actions.

## 3) Configure private auth

Generate a shared private token, store it in Fly, and keep it in the current
shell for the CLI and operational helpers:

```bash
read -rsp 'DD private token: ' DD_PRIVATE_TOKEN
export DD_PRIVATE_TOKEN
printf '\n'
printf 'DD_PRIVATE_TOKEN=%s\n' "$DD_PRIVATE_TOKEN" \
  | flyctl secrets import --stage --app your-dd-app
```

Use a newly generated, long random value. The server refuses to start without
this secret because its private listener is reachable inside the Fly network.

## Runtime isolate tuning

`dd_server` defaults the process-wide isolate budget to the host logical CPU
count. Override these values for production capacity planning:

```bash
flyctl secrets set \
  DD_RUNTIME_MAX_GLOBAL_ISOLATES=2 \
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

The Fly production image includes WebSockets and OTLP HTTP trace propagation.
Direct HTTP/3 and WebTransport support remain an experimental opt-in Cargo
feature and are not linked into this image.

## 4) Deploy platform manually

CI is the normal platform deployment path. For recovery or initial setup, run:

```bash
flyctl deploy --app your-dd-app --config deploy/fly/fly.toml --remote-only
```

Helper:

```bash
just fly-deploy your-dd-app
```

Persistent data lives under `/app/store`:

- deployed worker source/config
- KV and memory SQLite files
- cache blobs and indexes

The container starts through `dd_init`. On the first boot of a volume it repairs
the store tree to UID/GID 65532, records `.dd-volume-ownership-v1`, drops all
root and supplementary-group privileges, and then replaces itself with
`dd_server`. Later boots validate the marker and skip the recursive repair.
Fly checks `/readyz`, which fails during startup restoration and maintenance
drains; `/healthz` remains the process liveness endpoint.

## 5) Open private tunnel

```bash
just fly-proxy your-dd-app
```

Equivalent direct helper:

```bash
./deploy/fly/proxy-private-deploy.sh your-dd-app 18081 8081
```

`18081` is only the local end of the tunnel. The server continues to listen on
private port `8081` inside the Fly network.

## 6) Deploy workers through tunnel

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

## 7) Mint a public token

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
includes `token`. Generic CI jobs expose that value as `DD_TOKEN`; the checked-in
`Deploy Workers` workflow stores the chat-specific value as the
`DD_CHAT_DEPLOY_TOKEN` GitHub secret and maps it to `DD_TOKEN` only for the
deploy step. Omit expiry for a long-lived token, or add `--expires-in-seconds`
and/or `--max-uses` for short-lived release tokens.

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

## 8) Public routing

Once deployed with `--public`, host routing maps subdomain to worker name:

- `echo.example.com/* -> worker "echo"`

For built-in Fly hostname:

- set `PUBLIC_BASE_DOMAIN=your-dd-app.fly.dev`
- `https://echo.your-dd-app.fly.dev/` maps to worker `echo`

Fly app apex hostname itself is not mapped to worker and returns `404`.

## 9) Custom domains

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

- redeploy platform through CI: rerun the `Deploy Platform` workflow
- deploy platform manually: `just fly-deploy your-dd-app`
- deploy workers through CI: rerun the `Deploy Workers` workflow
- open tunnel: `just fly-proxy your-dd-app`
- deploy worker through tunnel: `just fly-worker-deploy <name> <file> [flags...]`
- mint token through tunnel: `just fly-worker-mint-token --name <token-name> --worker <worker> --public ...`
- list tokens through tunnel: `just fly-worker-list-tokens`
- delete token through tunnel: `just fly-worker-delete-token <token-name>`
- deploy through public endpoint: `DD_TOKEN=... just fly-worker-public-deploy-config <app> <config>`
- invoke private worker: `cargo run -p cli -- --server http://127.0.0.1:18081 invoke <name> --method GET --path /`
- consistent volume snapshot: `DD_PRIVATE_TOKEN=... just fly-snapshot <app> <volume-id>`

To roll back the platform, find the previous image with
`flyctl releases --app <app> --image`, then run
`flyctl deploy --app <app> --config deploy/fly/fly.toml --image <image>`. This
restores only the container image. It does not restore or reverse changes to the
attached volume.

The snapshot helper opens a private Fly proxy, drains writes, checkpoints every
database, schedules the volume snapshot, and resumes the service even when an
intermediate command fails.
