# @dd/runtime

Small platform selector for the `dd_dev_runtime` binary used by `@dd/vite`.

This package follows the same shape as `workerd`: it is a tiny wrapper with
platform-specific optional dependencies. Package managers install only the
runtime package matching the current `os` and `cpu`.

The packaged binary is for local test/dev only. It is built with the
`dev-runtime` Cargo profile, optimized for distribution size rather than peak
runtime performance.
