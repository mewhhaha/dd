# The dd wasm runtime

`crates/wasm-host` is dd's runtime: workers compiled from TypeScript to
WebAssembly by [Perry](https://github.com/PerryTS/perry) execute in
wasmtime, and a native Rust host provides every runtime function the module
imports. There is no JS engine in the loop.

## Running it

```bash
npm install -g @perryts/perry
cargo run -p wasm_host --bin dd_server
cargo run -p cli --bin dd -- deploy hello examples/hello.ts --public
curl -H 'host: hello.example.com' http://127.0.0.1:8080/perry
```

`dd_server` persists deployed workers under its store directory and backs
KV, memory namespaces, and the response cache with the `storage` crate.
Service bindings are configured at deploy time (`dd deploy --service
BINDING=WORKER`); `--assets-dir` serves static files before worker code
runs.

## Worker contract

Perry has no module-export story under `--target wasm`, but a bodyless
`declare function` compiles to a wasm import in the `ffi` namespace. dd uses
that as its host API. Declare only what you use — but never with optional
parameters (Perry emits an invalid call for omitted optional args on `ffi`
imports; pass every argument explicitly):

```ts
// Registration and request access
declare function dd_register(
  fetchHandler: (method: string, url: string, body: string) => unknown,
): void;
declare function dd_header(name: string): string | null;
declare function dd_json(value: unknown): string; // host-side JSON.stringify

// KV (write-last, monotonic versions — same store as the V8 runtime)
declare function dd_kv_get(binding: string, key: string): string | null;
declare function dd_kv_set(binding: string, key: string, value: string): void;
declare function dd_kv_delete(binding: string, key: string): void;
declare function dd_kv_list(binding: string, prefix: string): string[];

// Keyed memory namespaces: `command` runs under the key's lock; tvar writes
// commit atomically with the command's completion
declare function dd_memory_atomic(
  binding: string, key: string, command: () => unknown,
): any;
declare function dd_tvar_read(name: string): any;   // inside command only
declare function dd_tvar_write(name: string, value: unknown): void;

// Worker-scoped response cache (honors cache-control on the stored response)
declare function dd_cache_match(url: string): any;  // {status, headers, body} | null
declare function dd_cache_put(url: string, response: unknown): void;
declare function dd_cache_delete(url: string): boolean;

// Outbound HTTP (synchronous; 16 MiB response cap, 10 s timeout)
declare function dd_fetch(url: string, options: unknown): any;

// Co-deployed workers
declare function dd_service_fetch(
  binding: string, method: string, url: string, body: string,
): any;

// Websockets: handlers run on one dedicated instance per worker, so
// module-level state spans connections; sends work from any handler
declare function dd_ws_register(handlers: unknown): void; // {open?, message?, close?}
declare function dd_ws_send(connection: number, data: string): boolean;
declare function dd_ws_close(connection: number): void;
```

The handler returns either a plain string (served as `text/plain`) or an
object with optional `status`, `headers`, and `body` fields. Values coming
back from the host (`dd_fetch`, `dd_cache_match`, `dd_service_fetch`,
`dd_tvar_read`) must be typed `any` and accessed dynamically — Perry's
typed-shape field lowering breaks on host-created objects.

Instances are pooled and reused across requests, so module-level state
persists across requests on the same instance (and is lost on recycle —
use KV or memory namespaces for anything durable).

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
- **Optional parameters on `declare function`** produce invalid wasm when an
  argument is omitted at a call site — declare exact arities.

Host limitations of this experiment:

- No dynamic workers (`env.SANDBOX`) — they would need the Perry compiler
  itself at runtime, since worker code arrives as TypeScript source. No UI
  bridge. Websocket messages are text-only (binary frames are dropped), and
  all websocket handlers of a worker share one serialized instance. String indices are Unicode scalar
  positions (exact for BMP text). Regex support is literal-plus-anchors
  only. Memory namespaces commit tvar state but not the outbox-effect
  machinery of the V8 runtime, and tvars written by the V8 runtime (v8sc
  encoding) are rejected rather than decoded.

## Benchmarks

```bash
cargo run -p wasm_host --bin bench_wasm_worker --release
DD_BENCH_REQUESTS=5000 DD_BENCH_CONCURRENCY=64 \
  cargo run -p wasm_host --bin bench_wasm_worker --release
```

Reports module compile time plus invoke throughput and mean/p50/p95/p99
latency. Instances are pooled, so this measures the warm request path of
the engine directly, without HTTP or dispatch overhead.

## Fixtures

Integration tests run against real Perry-compiled modules vendored in
`crates/wasm-host/fixtures/`. Rebuild them with
`scripts/build-perry-wasm-fixtures.sh` after Perry upgrades (each fixture's
TypeScript source sits next to its `.wasm`).
