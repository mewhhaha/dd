# Binary Size Report

`just size-report` builds the dist-profile `dd_server` and writes a
commit-addressed report below `target/size-report/<git-sha>/dist/`. The
dist profile is `release` with `codegen-units=1`, `lto="thin"`,
`opt-level=3`, and stripped symbols.

For scale: the wasm runtime's `dd_server` builds to ~31 MB stripped, versus
~82 MB for the previous V8/Deno-based server it replaced.
