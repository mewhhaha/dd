# Fly.io: Public Traffic + Private Deploy

This package runs one `dd_server` process with two listeners:

- public listener on `BIND_PUBLIC_ADDR` (`0.0.0.0:8080`)
- private deploy listener on `BIND_PRIVATE_ADDR` (`[::]:8081`)

Public internet traffic reaches port `8080` through Fly service routing.
Deploy traffic stays private and is sent to port `8081` over WireGuard (`flyctl proxy`).

If you want to try the built-in Fly domain, set `PUBLIC_BASE_DOMAIN` to your app's
`<app-name>.fly.dev` host.

## Important model

Workers are deployed into the running `dd_server` app via its private deploy endpoint.
They are not separate Fly apps.

- `flyctl deploy` updates the server app image.
- `cargo run -p cli -- deploy ...` (against private port `8081`) deploys worker code.

## 1) App and volume

`fly.toml` currently points to `dd-private-8956e096` in `ams`.

```bash
flyctl apps create your-dd-app
flyctl volumes create dd_store --app your-dd-app --region ams --size 1
```

## 2) Deploy

```bash
flyctl deploy --app your-dd-app --config deploy/fly/fly.toml --flycast
```

From the repo root you can use the built-in helper instead:

```bash
just fly-deploy your-dd-app
```

Use the Fly app default domain while iterating (`https://<app-name>.fly.dev`) once public ingress exists.

The app keeps persistent data in `/app/store`:

- worker deployment files
- KV/actor/sqlite files
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
```

Or from the repo root:

```bash
just fly-worker-deploy hello examples/hello.js
just fly-worker-deploy echo examples/echo.js --public
```

This command deploys worker `hello` inside your existing Fly app. It does not create a new Fly app.

## 4) Public worker traffic

Set `PUBLIC_BASE_DOMAIN` in `fly.toml` to your domain (for example `example.com`).
Public requests route by subdomain:

- `foo.example.com/*` invokes worker `foo`
- apex `example.com` returns `404`

Public deploy route is not exposed.

## 5) Custom domain + wildcard

Allocate public ingress for internet traffic:

```bash
flyctl ips allocate-v4 --shared --app your-dd-app
flyctl ips allocate-v6 --app your-dd-app
```

Attach your domain and wildcard to the app:

```bash
flyctl certs add example.com --app your-dd-app
flyctl certs add "*.example.com" --app your-dd-app
```

Add DNS records at your DNS provider as instructed by `flyctl certs show`.

## 6) Operations

```bash
flyctl logs --app your-dd-app
flyctl machines list --app your-dd-app
flyctl ips list --app your-dd-app
```
