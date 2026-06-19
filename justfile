set shell := ["bash", "-euo", "pipefail", "-c"]

default_app := "your-dd-app"
default_fly_config := "deploy/fly/fly.toml"
default_private_server := "http://127.0.0.1:18081"

# Materialize patched-crates/<crate> from crates.io source and apply patches/<crate>.patch if it exists.
patch crate version='':
  ./scripts/patch-crate.sh {{crate}} {{version}}

# Replace an existing patched crate from crates.io source, then apply patches/<crate>.patch.
patch-refresh crate version='':
  PATCH_REFRESH=1 ./scripts/patch-crate.sh {{crate}} {{version}}

# Regenerate patches/<crate>.patch from patched-crates/<crate> versus the locked crates.io source.
patch-save crate version='':
  ./scripts/patch-save-crate.sh {{crate}} {{version}}

# Deploy the dd_server app to Fly.
fly-deploy app=default_app config=default_fly_config:
  FLYCTL_BIN="${FLYCTL_BIN:-$(if command -v flyctl >/dev/null 2>&1; then command -v flyctl; elif [ -x /home/mewhhaha/.fly/bin/flyctl ]; then printf %s /home/mewhhaha/.fly/bin/flyctl; elif command -v fly >/dev/null 2>&1; then command -v fly; else echo "flyctl not found (set FLYCTL_BIN or install flyctl)" >&2; exit 1; fi)}"; \
  "$FLYCTL_BIN" deploy --app {{app}} --config {{config}} --remote-only --no-cache

# Open a local proxy to the private deploy port on Fly.
fly-proxy app=default_app local_port='18081' remote_port='8081':
  ./deploy/fly/proxy-private-deploy.sh {{app}} {{local_port}} {{remote_port}}

# Deploy a worker into the running Fly app through the private proxy.
fly-worker-deploy name file +flags:
  ./deploy/fly/post-worker-deploy.sh {{default_private_server}} {{name}} {{file}} {{flags}}

# Deploy a generated worker config into the running Fly app through the private proxy.
fly-worker-deploy-config config:
  cargo run -p cli -- --server {{default_private_server}} deploy-config {{config}}

# Mint a scoped token through the private proxy.
fly-worker-mint-token +flags:
  cargo run -p cli -- --server {{default_private_server}} mint-token {{flags}}

# List token metadata through the private proxy.
fly-worker-list-tokens:
  cargo run -p cli -- --server {{default_private_server}} list-tokens

# Delete a token through the private proxy.
fly-worker-delete-token id:
  cargo run -p cli -- --server {{default_private_server}} delete-token {{id}}

# Deploy a generated worker config through the public Fly endpoint with DD_TOKEN.
fly-worker-public-deploy-config app=default_app config='dist/dd.deploy.json':
  cargo run -p cli -- --server https://{{app}}.fly.dev deploy-config {{config}}

# Deploy a worker into the running Fly app through an explicitly chosen private proxy endpoint.
fly-worker-deploy-at server name file +flags:
  ./deploy/fly/post-worker-deploy.sh {{server}} {{name}} {{file}} {{flags}}

# Deploy a generated worker config through an explicitly chosen private proxy endpoint.
fly-worker-deploy-config-at server config:
  cargo run -p cli -- --server {{server}} deploy-config {{config}}

# Internal escape hatch: write directly into persisted Fly worker store, then restart machine.
fly-worker-store-deploy name file +flags:
  ./deploy/fly/store-worker-deploy.sh {{default_app}} {{name}} {{file}} {{flags}}

# Contributor check path.
check:
  bash scripts/check_public_memory_naming.sh
  just check-js
  cargo fmt --all -- --check
  cargo check --workspace --all-targets --all-features
  cargo test --workspace

# Build dd_server with the selected distribution profile and write a size report.
size-report profile="dist":
  ./scripts/measure-binary-size.sh {{profile}}

# Syntax-check source-only JS integration package.
check-js:
  node --check packages/dd-vite/src/index.js
  node --check packages/dd-runtime/index.cjs
  node --check packages/dd-vite/src/runtime.js
  node --check packages/dd-vite/src/vite.js
  node --check packages/dd-vite/src/vitest.js
  node --check packages/dd-vite/src/vitest-environment.js

# Build the size-optimized dd dev runtime binary into the current host package.
build-dd-runtime-package package='':
  ./scripts/build-dd-runtime-package.sh {{package}}
