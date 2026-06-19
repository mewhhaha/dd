# Vite React Router RSC Example

This workspace example uses React Router's experimental RSC framework mode through `unstable_reactRouterRSC()`, `@vitejs/plugin-rsc`, and the dd Vite environment.

The worker binds `EXAMPLE_MEMORY` and passes it through React Router's request
context. The project route loader increments an STM-backed request counter with
`memory.atomic(...)`, the RSC route server component renders the count from
`loaderData`, and app responses mirror it in `x-dd-stm-count`. The smoke test
requires both values to advance together.

Tailwind CSS is enabled through `@tailwindcss/vite`; the root route exposes
`app/tailwind.css` through a React Router `links` export and uses a Tailwind
component class in the navigation.

```bash
pnpm --filter dd-vite-react-router-rsc-example dev
pnpm --filter dd-vite-react-router-rsc-example build
pnpm --filter dd-vite-react-router-rsc-example smoke
```

During development, app requests enter the dd runtime first. The example worker executes the built React Router RSC server handler inside dd, while Vite still serves its own client, module, and HMR endpoints directly.

The generated production deployment config points at the bundled dd worker entry and the React Router RSC client asset directory:

```text
dist/worker.js
dist/dd.deploy.json
dist/react-router-rsc/client/
```
