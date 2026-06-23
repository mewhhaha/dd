# Vite React Router RSC Example

This workspace example uses React Router's experimental RSC framework mode through `unstable_reactRouterRSC()`, `@vitejs/plugin-rsc`, and `@mewhhaha/vite-plugin-dd/react-router-rsc`.

The dd framework preset owns the dd-specific Vite wiring: the outer `rsc`
environment is backed by dd, the `ssr` child environment remains a runnable Vite
environment for the RSC dev proxy, and the worker imports
`virtual:dd-react-router-rsc-server` instead of a generated local build helper.

The worker binds `EXAMPLE_MEMORY` and passes it through React Router's request
context. The project route loader increments an STM-backed request counter with
`memory.atomic(...)`, the RSC route server component renders the count from
`loaderData`, and app responses mirror it in `x-dd-stm-count`. The smoke test
requires both values to advance together.

Tailwind CSS is enabled through `@tailwindcss/vite`; the root route imports
`app/tailwind.css` and uses a Tailwind component class in the navigation.

```bash
pnpm --filter dd-vite-react-router-rsc-example dev
pnpm --filter dd-vite-react-router-rsc-example build
pnpm --filter dd-vite-react-router-rsc-example smoke
```

During development, app requests enter the dd runtime first. The example worker
executes Vite's transformed React Router RSC server modules inside dd, while
Vite still serves its own client, module, RSC proxy, and HMR endpoints directly.

The generated production deployment config points at the bundled dd worker entry and the React Router RSC client asset directory:

```text
dist/vite-react-router-rsc/worker.js
dist/vite-react-router-rsc/dd.deploy.json
dist/react-router-rsc/client/
dist/dd.workers.json
```
