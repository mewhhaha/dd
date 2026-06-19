# Vite Worker Example

Minimal TypeScript example for the `@dd/vite` workspace package.

It uses `dd()` with the default root mount, so normal app requests to the Vite
dev server hit the worker while Vite's own module/HMR requests bypass it.

```bash
pnpm install
pnpm --filter dd-vite-worker-example dev
pnpm --filter dd-vite-worker-example smoke
pnpm --filter dd-vite-worker-example typecheck
```
