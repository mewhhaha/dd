# Bundled Router Example

This example shows a TypeScript worker bundled with `pnpm` + `tsdown`.

## Build

```bash
cd examples/bundled-router
pnpm install
pnpm run build
```

## Deploy

```bash
cargo run -p cli -- deploy bundled-router examples/bundled-router/dist/worker.js
```

## Invoke

```bash
cargo run -p cli -- invoke bundled-router --method GET --path /
cargo run -p cli -- invoke bundled-router --method GET --path /health
printf "hello" | cargo run -p cli -- invoke bundled-router --method POST --path /echo --header "content-type: text/plain" --body-file -
```
