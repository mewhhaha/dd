# Vite React Router Example

This workspace example uses React Router framework mode through `@react-router/dev/vite`, then runs the generated server build inside the local `dd` runtime during Vite development. Vite still serves its own module graph, client runtime, and React Router development endpoints.

The worker binds `EXAMPLE_MEMORY` and passes it through React Router's request
context. The project route loader increments an STM-backed request counter with
`memory.atomic(...)`, renders the count, and app responses mirror it in
`x-dd-stm-count`. The smoke test requires both values to advance together.

Tailwind CSS is enabled through `@tailwindcss/vite`; the root route imports
`app/tailwind.css` and uses a Tailwind component class in the navigation.

```bash
pnpm --filter dd-vite-react-router-example dev
pnpm --filter dd-vite-react-router-example build
pnpm --filter dd-vite-react-router-example smoke
```

The build emits a rewritten deployment config in `dist/dd.deploy.json` and a cache policy for fingerprinted assets in `dist/_headers`.
