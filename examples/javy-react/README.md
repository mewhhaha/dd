# Javy React experiment

This worker is bundled to one ES2023 module by Vite, compiled to QuickJS
bytecode and Wasm by Javy, and executed by `dd_javy_server`.

Install Javy 9, build the custom host-call plugin, then build and serve the
worker:

```bash
scripts/install-javy.sh
scripts/build-javy-plugin.sh
JAVY_BIN=target/tools/javy JAVY_PLUGIN=target/tools/dd-javy-plugin.wasm \
  pnpm --filter dd-javy-react-example build
cargo run -p javy_host --bin dd_javy_server -- \
  --worker examples/javy-react/dist/worker.wasm \
  --env GREETING=welcome \
  --kv TEST_KV \
  --memory TEST_MEMORY
```

Try both the React SSR and JSON paths:

```bash
curl 'http://127.0.0.1:8091/?name=Ada'
curl 'http://127.0.0.1:8091/stream?name=Ada'
curl -H 'user-agent: javy-smoke' \
  'http://127.0.0.1:8091/json?name=Ada'
curl 'http://127.0.0.1:8091/bindings?name=first'
```

Omitting `JAVY_PLUGIN` produces a worker without the `dd_host` imports; Web
APIs and React still work, but host bindings do not.
