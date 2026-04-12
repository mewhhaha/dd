# Fly.io: Public Traffic + Private Deploy

Fly runs one `dd_server` app process with two listeners:

- public traffic on `BIND_PUBLIC_ADDR` (`0.0.0.0:8080`)
- private control plane on `BIND_PRIVATE_ADDR` (`[::]:8081`)

Workers are deployed into that running app. They are not separate Fly apps.

## Canonical flow

1. deploy platform container with `flyctl deploy`
2. open WireGuard tunnel to private port with `just fly-proxy <app>`
3. deploy workers through that tunnel with `just fly-worker-deploy ...`

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

## 4) Open private tunnel

```bash
just fly-proxy your-dd-app
```

Equivalent direct helper:

```bash
./deploy/fly/proxy-private-deploy.sh your-dd-app 18081 8081
```

## 5) Deploy workers through tunnel

Preferred helper:

```bash
just fly-worker-deploy hello examples/hello.js --public
just fly-worker-deploy chat examples/chat-worker/src/worker.js --memory-binding CHAT_ROOM --public --assets-dir examples/chat-worker/assets
```

Equivalent raw CLI:

```bash
cargo run -p cli -- --server http://127.0.0.1:18081 deploy hello examples/hello.js --public
cargo run -p cli -- --server http://127.0.0.1:18081 deploy static-assets-site examples/static-assets-site/worker.js --public --assets-dir examples/static-assets-site/assets
```

## 6) Public routing

Once deployed with `--public`, host routing maps subdomain to worker name:

- `echo.example.com/* -> worker "echo"`

For built-in Fly hostname:

- set `PUBLIC_BASE_DOMAIN=your-dd-app.fly.dev`
- `https://echo.your-dd-app.fly.dev/` maps to worker `echo`

Fly app apex hostname itself is not mapped to worker and returns `404`.

## 7) Custom domains

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
- invoke private worker: `cargo run -p cli -- --server http://127.0.0.1:18081 invoke <name> --method GET --path /`

Internal escape hatch:

- `just fly-worker-store-deploy ...` writes directly into persisted worker store and restarts machine
- use only when normal private control plane path is unavailable
