set shell := ["bash", "-euo", "pipefail", "-c"]

default_app := "your-dd-app"
default_fly_config := "deploy/fly/fly.toml"
default_private_server := "http://127.0.0.1:18081"

# Contributor check path.
check:
  bash scripts/check_public_memory_naming.sh
  cargo fmt --all -- --check
  cargo check --workspace --all-targets --all-features
  cargo clippy --workspace --all-targets --all-features -- -D warnings
  cargo test --workspace

# Deploy every example through the CLI against a local dd_server and
# exercise it (requires the Perry compiler: PERRY env or on PATH).
smoke-examples:
  ./scripts/smoke_examples.sh

# Rebuild the Perry-compiled wasm fixtures used by crates/wasm-host tests.
fixtures:
  ./scripts/build-perry-wasm-fixtures.sh

# Worker invoke benchmark.
bench:
  cargo run -p wasm_host --bin bench_wasm_worker --release

# Build the dist-profile dd_server and report its size.
size-report:
  ./scripts/measure-binary-size.sh

# Deploy the dd_server app to Fly.
fly-deploy app=default_app config=default_fly_config:
  FLYCTL_BIN="${FLYCTL_BIN:-$(if command -v flyctl >/dev/null 2>&1; then command -v flyctl; elif [ -x /home/mewhhaha/.fly/bin/flyctl ]; then printf %s /home/mewhhaha/.fly/bin/flyctl; elif command -v fly >/dev/null 2>&1; then command -v fly; else echo "flyctl not found (set FLYCTL_BIN or install flyctl)" >&2; exit 1; fi)}"; \
  "$FLYCTL_BIN" deploy --app {{app}} --config {{config}} --remote-only

# Drain writes, checkpoint every database, schedule a Fly volume snapshot, and always resume.
fly-snapshot app volume_id port='18081':
  ./deploy/fly/snapshot.sh {{app}} {{volume_id}} {{port}}

# Open a local proxy to the private deploy port on Fly.
fly-proxy app=default_app local_port='18081' remote_port='8081':
  ./deploy/fly/proxy-private-deploy.sh {{app}} {{local_port}} {{remote_port}}

# Deploy a worker (TypeScript or .wasm; compiled with Perry) through the private proxy.
fly-worker-deploy name file *flags='':
  cargo run -p cli --bin dd -- --server {{default_private_server}} deploy {{name}} {{file}} {{flags}}
