# Fly.io: Public Traffic + Private Deploy

Fly runs one `dd_server` app process with two listeners:

- public traffic on `BIND_PUBLIC_ADDR` (`0.0.0.0:8080`)
- private control plane on `BIND_PRIVATE_ADDR` (`[::]:8081`)

Workers are deployed into that running app. They are not separate Fly apps.

## Canonical flow

1. provision the Fly app, volume, and secrets once
2. merge to `main`; successful CI deploys the platform container
3. open a WireGuard tunnel to the private port with `just fly-proxy <app>`
4. deploy workers through the tunnel with the `dd` CLI

## 1) Create app and volume

```bash
flyctl apps create your-dd-app
flyctl volumes create dd_store --region ams --size 1 --app your-dd-app
```

The volume mounts at `/app/store` (see `fly.toml`); deployed workers and
all KV/memory/cache state persist there.

## 2) Configure platform CI

Create an app-scoped token and store it as the `FLY_API_TOKEN` GitHub
Actions secret:

```bash
flyctl tokens create deploy --app your-dd-app --name github-actions --expiry 720h
```

The `Deploy Platform` workflow waits for CI on `main` and ships the
container image built from `deploy/fly/Dockerfile`.

## 3) Require auth on the private API

```bash
flyctl secrets set DD_PRIVATE_TOKEN="$(openssl rand -hex 32)" --app your-dd-app
```

Without it, `dd_server` logs a warning and accepts unauthenticated deploys —
fine locally, not on Fly.

## 4) Deploy workers

```bash
just fly-proxy your-dd-app                # tunnel 127.0.0.1:18081 -> private port
export DD_PRIVATE_TOKEN=...               # the secret from step 3
just fly-worker-deploy hello examples/hello.ts --public
curl -H 'host: hello.your-domain.example' https://your-dd-app.fly.dev/
```

`dd deploy` compiles TypeScript with the Perry compiler before upload, so
Perry must be installed wherever you run it (`npm install -g
@perryts/perry`). Prebuilt `.wasm` modules deploy without Perry.

## Snapshots

`just fly-snapshot <app> <volume-id>` drains writes, checkpoints every
database, schedules a Fly volume snapshot, and resumes traffic.
