# Perry wasm runtime experiment

`crates/wasm-host` is an experimental dd runtime that executes workers
compiled from TypeScript to WebAssembly by
[Perry](https://github.com/PerryTS/perry), instead of running JavaScript in
V8/Deno isolates like `crates/runtime` does. There is no JS engine in the
loop: wasmtime executes the compiled module, and a native Rust host provides
every runtime function the module imports.

## Running it

```bash
npm install -g @perryts/perry
perry compile examples/perry-wasm-worker/worker.ts -o worker.wasm --target wasm
cargo run -p wasm_host --bin dd_wasm_server -- --worker worker.wasm --port 8090
curl http://127.0.0.1:8090/perry
```

## Worker contract

Perry has no module-export story under `--target wasm`, but a bodyless
`declare function` compiles to a wasm import in the `ffi` namespace. dd uses
that as its host API — a worker declares and calls:

```ts
declare function dd_register(
  fetchHandler: (method: string, url: string, body: string) => unknown,
): void;
declare function dd_header(name: string): string | null; // current request header
declare function dd_json(value: unknown): string;        // host-side JSON.stringify

dd_register((method, url, body) => ({
  status: 200,
  headers: { "content-type": "application/json" },
  body: dd_json({ hello: new URL(url).pathname }),
}));
```

The handler returns either a plain string (served as `text/plain`) or an
object with optional `status`, `headers`, and `body` fields. Each request
runs in a fresh instance: module top level re-runs, so workers are stateless
across requests by construction.

## How it works

Perry's wasm target NaN-boxes every JS value into 64 bits (tags for
undefined/null/booleans; high-16-bit tags marking string-table ids, host
handle ids, and int32s; anything else is the f64 itself). Strings, objects,
arrays, closures, and promises live host-side in a handle heap; the guest
only holds ids. At startup the module registers its string literals through
`rt.string_new(ptr, len)` in interning order, and nearly every runtime
operation then flows through one generic bus:

```
rt.mem_call(name_id, arg_count, base_addr)
```

with arguments as raw NaN-box bits in guest memory and the result written
back to `base_addr`. The host implements a name-keyed dispatcher
(`bridge::dispatch`) covering console, strings, arrays, objects, classes,
closures (which call back into the guest through
`__indirect_function_table`), Map/Set, JSON, URL, dates, buffers, crypto,
timers, and promises. The ~200 individually-declared `rt.*` imports exist
mostly to keep import indices stable; they adapt into the same dispatcher.

The reference semantics are `wasm_runtime.js` in the Perry repo — the JS
glue Perry embeds for browser use. Where that glue silently tolerates
codegen gaps (unresolvable method dispatch returns `undefined`), this host
does the same but logs a warning.

## Known limitations

Upstream Perry wasm-target gaps (verified against the browser glue, which
has the same behavior):

- **No `async`/`await`.** Perry compiles top-level async function bodies to
  JavaScript, which this host cannot run — such modules are rejected at load
  with a clear error. `new Promise(executor)` also compiles to a no-op.
  Write synchronous handlers.
- **`JSON.stringify` of typed object literals silently returns `undefined`**
  (the `JsonStringifyFull` lowering has no wasm handler). Use `dd_json`.
- **Passing typed objects host→guest breaks field access** (shape-field ids
  resolve wrongly for shapes never constructed by guest code) — hence the
  string-based `(method, url, body)` handler signature plus `dd_header`.
- **Mutating an array stored in a class field** (`this.items.push(x)`) is a
  silent no-op (frame-layout bug in the generic `class_call_method`
  fallback). Use local arrays.

Host limitations of this experiment:

- No outbound `fetch` (errors loudly), no KV/cache/memory bindings yet, no
  UI bridge. String indices are Unicode scalar positions (exact for BMP
  text). Regex support is literal-plus-anchors only.

## Benchmarks

```bash
cargo run -p wasm_host --bin bench_wasm_worker --release
DD_BENCH_REQUESTS=5000 DD_BENCH_CONCURRENCY=64 \
  cargo run -p wasm_host --bin bench_wasm_worker --release
```

Reports module compile time plus invoke throughput and mean/p50/p95/p99
latency, the same metrics as `cargo run -p runtime --bin bench --release`
(steady-state section) for the V8 runtime. When comparing, remember the
methodology difference: the wasm engine pays a full cold start (fresh
instance + `_start`) on every request, while the V8 bench measures warm
reused isolates; the wasm bench also drives the engine directly where the
V8 bench goes through `RuntimeService` dispatch, and the workers differ
(the wasm fixture parses the URL and builds JSON via `dd_json`).

## Fixtures

Integration tests run against real Perry-compiled modules vendored in
`crates/wasm-host/fixtures/`. Rebuild them with
`scripts/build-perry-wasm-fixtures.sh` after Perry upgrades (fixture sources
are `examples/perry-wasm-worker/worker.ts` and
`crates/wasm-host/fixtures/features_worker.ts`).
