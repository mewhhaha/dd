# Binary Size Report

`just size-report` builds the dist-profile `dd_server` and writes a
commit-addressed report below `target/size-report/<git-sha>/dist/`. The
dist profile is `release` with `codegen-units=1`, `lto="thin"`,
`opt-level=3`, and stripped symbols.

For scale: `dd_server` builds to ~31 MB stripped (fat LTO, opt-level 3),
versus ~82 MB for the V8/Deno-based server it replaced. Building with
`opt-level = "z"` shrinks it to ~21 MB but costs ~40% of bridge throughput —
measured and rejected as the default.
