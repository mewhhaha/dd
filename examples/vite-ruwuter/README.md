# Vite Ruwuter Example

Minimal TypeScript example for `@dd/vite` using `@mewhhaha/ruwuter` from JSR.

It uses `dd()` with the default root mount, so normal app requests to the Vite
dev server hit the worker while Vite's own module/HMR requests bypass it.
There is no `index.html`; the worker is the app entrypoint, and the Vite build
input is a small TypeScript client module only to exercise fingerprinted asset
output.

The worker also binds `EXAMPLE_MEMORY` and increments an STM-backed request
counter with `memory.atomic(...)`. App responses expose the current value in
`x-dd-stm-count`, and the smoke test requires that value to advance.

Tailwind CSS is enabled through `@tailwindcss/vite`; `src/client.ts` imports
`src/tailwind.css`, and the rendered worker HTML uses a Tailwind component
class.

```bash
pnpm install
pnpm --filter dd-vite-ruwuter-example dev
pnpm --filter dd-vite-ruwuter-example build
pnpm --filter dd-vite-ruwuter-example smoke
pnpm --filter dd-vite-ruwuter-example typecheck
```

The build emits a rewritten deployment config in `dist/dd.deploy.json` and a cache policy for fingerprinted assets in `dist/_headers`.
