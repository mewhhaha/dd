# Fly.io: Public Traffic + Private Deploy

This package runs one `dd_server` process with two listeners:

- public listener on `BIND_PUBLIC_ADDR` (`0.0.0.0:8080`)
- private deploy listener on `BIND_PRIVATE_ADDR` (`[::]:8081`)

Public internet traffic reaches port `8080` through Fly service routing.
Deploy traffic stays private and is sent to port `8081` over WireGuard (`flyctl proxy`).

If you want to try the built-in Fly domain, set `PUBLIC_BASE_DOMAIN` to your app's
`<app-name>.fly.dev` host.

## Important model

On Fly there is one app process:

- app/container: `dd_server`
- workers: deployed into that running app through the private deploy endpoint

Workers are **not** separate Fly apps.

That means:

- `fly deploy` updates the `dd_server` binary/container
- `dd deploy` uploads worker source/config into the running `dd_server`

## 1) Create the app + volume

```bash
flyctl apps create your-dd-app
flyctl volumes create dd_store --region ams --size 1 --app your-dd-app
```

This profile defaults to `ams` because it is EU and widely available.

## 2) Deploy the platform

From the repo root:

```bash
flyctl deploy --app your-dd-app --config deploy/fly/fly.toml --remote-only --no-cache
```

Or use the helper:

```bash
just fly-deploy your-dd-app
```

Use the Fly app default domain while iterating (`https://<app-name>.fly.dev`) once public ingress exists.

The app keeps persistent data in `/app/store`:

- deployed worker source/config
- KV/memory/sqlite files
- cache blobs/indexes

## 3) Private deploy tunnel (no auth, private network only)

Use the helper to resolve the active machine IPv6 and proxy local deploy traffic:

```bash
./deploy/fly/proxy-private-deploy.sh your-dd-app 18081 8081
```

Or from the repo root:

```bash
just fly-proxy your-dd-app
```

Then deploy workers to the private endpoint:

```bash
cargo run -p cli -- --server http://127.0.0.1:18081 deploy hello examples/hello.js
cargo run -p cli -- --server http://127.0.0.1:18081 deploy static-assets-site examples/static-assets-site/worker.js --public --assets-dir examples/static-assets-site/assets
```

## 4) Public worker traffic

Once a worker is deployed with `--public`, public internet traffic can reach it via host routing:

- `echo.example.com/* -> worker "echo"`

For the built-in Fly domain:

- if app name is `your-dd-app`
- set `PUBLIC_BASE_DOMAIN=your-dd-app.fly.dev`
- then `https://echo.your-dd-app.fly.dev/` maps to worker `echo`

Fly’s default app hostname itself (the apex) is not mapped to a worker and returns `404`.

## 5) Custom domain + wildcard subdomains

If you want `worker.yourdomain.com -> worker`, add certs + DNS:

```bash
flyctl certs add example.com --app your-dd-app
flyctl certs add "*.example.com" --app your-dd-app
```

Add DNS records at your DNS provider as instructed by `flyctl certs show`.

Then set:

```toml
[env]
PUBLIC_BASE_DOMAIN = "example.com"
```

Now:

- `echo.example.com` -> worker `echo`
- `chat.example.com` -> worker `chat`

## 6) Operations

- redeploy platform:
  - `just fly-deploy your-dd-app`
- open private deploy proxy:
  - `just fly-proxy your-dd-app`
- deploy worker:
  - `cargo run -p cli -- --server http://127.0.0.1:18081 deploy name file.js [--public]`
- invoke private worker:
  - `cargo run -p cli -- --server http://127.0.0.1:18081 invoke name --method GET --path /`
