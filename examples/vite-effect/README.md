# Vite Effect Example

Small TypeScript worker for `@mewhhaha/vite-plugin-dd` using `effect`, `itty-router`, and SimpleWebAuthn.

The worker is a passkey auth app. Effect owns the worker architecture:

- `RequestContext` and `WorkerEnv` are request-scoped Context services.
- `StorageLive` is a Layer-backed service over KV, memory challenges, rate limits, sessions, and audit logs.
- `PasskeysLive` is a Layer-backed service over SimpleWebAuthn.
- Auth routes are Effect programs over those services.
- `Schema` validates incoming JSON and persisted KV records.
- Tagged errors model expected HTTP failures.
- `Schedule` retries transient KV operations.

It is also a multi-worker deployment. The public frontend worker declares an
`AUTH` service binding, and the auth worker is deployed separately as
`vite-effect-auth` with `public: false`. Build output includes
`dist/vite-effect/dd.deploy.json` for the frontend and
`dist/auth/dd.deploy.json` for the private auth worker. The private auth worker
is called through the `AUTH` service binding during dev and production.

```bash
pnpm install
pnpm --filter dd-vite-effect-example dev
pnpm --filter dd-vite-effect-example build
pnpm --filter dd-vite-effect-example smoke
pnpm --filter dd-vite-effect-example typecheck
```

The first registered passkey becomes the admin user. Later users are viewers. The app uses `dd()`
with the default root mount, so page requests hit the worker while Vite module and HMR requests keep
working normally. The smoke test checks server-side passkey challenge behavior and Vite module bypass.
