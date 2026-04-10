# Static Assets Site Example

This example shows the deploy-time asset flow:

- `/index.html`, `/styles.css`, and `/app.js` come from `--assets-dir`
- the worker only handles `/`, `/api/time`, and 404s
- root [examples/static-assets-site/assets/_headers](/home/mewhhaha/src/grugd/examples/static-assets-site/assets/_headers) sets asset-only caching policy

## Deploy

```bash
cargo run -p cli -- deploy static-assets-site examples/static-assets-site/worker.js --public --assets-dir examples/static-assets-site/assets
```

If you are deploying into the Fly app through the private proxy:

```bash
just fly-proxy
just fly-worker-deploy static-assets-site examples/static-assets-site/worker.js --public --assets-dir examples/static-assets-site/assets
```

## Use

- open `/` and the worker redirects to `/index.html`
- the browser loads `/styles.css` and `/app.js` directly from the asset bundle
- `app.js` fetches `/api/time`, which falls through to the worker
