# Vite Worker Example

Minimal TypeScript example for the `@dd/vite` workspace package.

It uses `dd()` with the default root mount, so normal app requests to the Vite
dev server hit the worker while Vite's own module/HMR requests bypass it.
There is no `index.html`; the worker is the app entrypoint, and the Vite build
input is a small TypeScript client module only to exercise fingerprinted asset
output.

```bash
pnpm install
pnpm --filter dd-vite-worker-example dev
pnpm --filter dd-vite-worker-example smoke
pnpm --filter dd-vite-worker-example typecheck
```
