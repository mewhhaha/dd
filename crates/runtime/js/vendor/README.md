# Vendored web platform scripts

The scripts here implement dd's web platform classes: URL and URLPattern,
events and AbortSignal, streams, encoding, Blob and File, Headers, Request,
Response and FormData, `performance`, and WebCrypto. They come from Deno
(<https://github.com/denoland/deno>, MIT license, copyright the Deno authors),
whose copyright line each file keeps. `webidl/00_webidl.js` also adapts
jsdom's webidl-conversions (BSD-2-Clause), as its header says.

| Directory | Deno crate        | Version |
| --------- | ----------------- | ------- |
| `webidl`  | `deno_webidl`     | 0.259.0 |
| `web`     | `deno_web`        | 0.290.0 |
| `fetch`   | `deno_fetch`      | 0.283.0 |
| `crypto`  | `deno_crypto`     | 0.253   |

Only some of each crate's scripts came over. dd has since made them its own
code, edited in place rather than tracked against upstream:

- Each file is a script, not a module. It runs once, as the body of a
  function whose `__bootstrap` parameter holds `core`, `primordials` and
  dd's private state, and returns its exports. `core.loadExtScript` loads it
  on first use under the specifier `ext:` plus its path under
  `crates/runtime/js`, such as `ext:vendor/web/06_streams.js`; the list lives
  in `crates/runtime/src/web/mod.rs`.
- They run on dd's own `core` (`js/core/core.js`) and ops
  (`crates/runtime/src/web`), not on `deno_core`. There is no resource table,
  so nothing is backed by a resource id.
- What dd does not expose is gone: the `Deno` namespace and `Deno.serve`'s
  fast paths, `fetch` itself (dd's is in `js/execute_worker/fetch.js`),
  `Deno.HttpClient`, `location`, `MessageChannel` and `BroadcastChannel`,
  `PerformanceObserver`, resource-backed streams, and the hooks Deno's
  `node:` polyfills read.
- `js/bootstrap.js` decides which classes become globals, and
  `js/web/console.js`, dd's console, takes the place of Deno's.
