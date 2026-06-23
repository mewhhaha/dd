# Vite Hono Example

Minimal TypeScript example for `@mewhhaha/vite-plugin-dd` using Hono.

It uses `dd()` with the default root mount, so normal app requests to the Vite
dev server hit the worker while Vite's own module/HMR requests bypass it.
There is no `index.html`; the worker is the app entrypoint, and the Vite build
input is a small TypeScript client module only to exercise fingerprinted asset
output.

The worker also binds `EXAMPLE_MEMORY` and increments an STM-backed request
counter with `memory.atomic(...)`. App responses expose the current value in
`x-dd-stm-count`, and the smoke test requires that value to advance.

Storefront mutations use `fixi-js` attributes on normal HTML forms. Without
JavaScript they still submit and redirect as plain forms; with fixi loaded from
`src/client.ts`, Hono sees `FX-Request: true` and returns only the cart panel
fragment for an in-place swap.

Tailwind CSS is enabled through `@tailwindcss/vite`; `src/client.ts` imports
`src/tailwind.css`, and the rendered worker HTML uses a Tailwind component
class.

```bash
pnpm install
pnpm --filter dd-vite-hono-example dev
pnpm --filter dd-vite-hono-example build
pnpm --filter dd-vite-hono-example smoke
pnpm --filter dd-vite-hono-example typecheck
```

The build emits `dist/vite-hono/worker.js`, `dist/vite-hono/dd.deploy.json`,
`dist/client/` assets, and `dist/dd.workers.json`.
