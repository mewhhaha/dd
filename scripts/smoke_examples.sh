#!/usr/bin/env bash
# End-to-end smoke: builds dd_server and the dd CLI, deploys every example
# through the CLI (which compiles TypeScript with Perry), and exercises each
# worker over HTTP. Requires the Perry compiler (PERRY env or on PATH).
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

public_port="${PUBLIC_PORT:-18180}"
private_port="${PRIVATE_PORT:-18181}"
store_dir="$(mktemp -d)"
upstream_port="${UPSTREAM_PORT:-18182}"

cargo build -q -p wasm_host --bin dd_server -p cli --bin dd

./target/debug/dd_server \
  --public-addr "127.0.0.1:$public_port" \
  --private-addr "127.0.0.1:$private_port" \
  --store-dir "$store_dir" &
server_pid=$!
trap 'kill $server_pid 2>/dev/null || true; rm -rf "$store_dir"' EXIT
sleep 1

dd() { ./target/debug/dd --server "http://127.0.0.1:$private_port" "$@"; }

pass() { echo "PASS $1"; }
check_contains() {
  local name="$1" body="$2" needle="$3"
  if [[ "$body" == *"$needle"* ]]; then pass "$name"; else
    echo "FAIL $name: expected $needle in: $body"; exit 1; fi
}

for worker in hello kv-counter memory-counter cache router auth chat proxy; do
  dd deploy "$worker" "examples/$worker.ts" --public
done
dd deploy router examples/router.ts --public --service AUTH=auth
dd list

url() { printf 'http://127.0.0.1:%s%s' "$public_port" "$2"; }
curl_worker() { curl -sf --max-time 10 -H "host: $1.example.com" "$(url "$1" "$2")" "${@:3}"; }

check_contains hello "$(curl_worker hello /perry)" '"greeting":"hello perry"'
check_contains kv-counter-1 "$(curl_worker kv-counter /)" "hits=1"
check_contains kv-counter-2 "$(curl_worker kv-counter /)" "hits=2"
curl_worker memory-counter /alice -X POST >/dev/null
check_contains memory-counter "$(curl_worker memory-counter /alice -X POST)" '"count":2'
check_contains memory-counter-get "$(curl_worker memory-counter /alice)" '"count":2'
check_contains cache-miss "$(curl_worker cache / -i)" "x-cache: miss"
check_contains cache-hit "$(curl_worker cache / -i)" "x-cache: hit"
check_contains router-echo "$(curl_worker router /echo -X POST -d ping)" '"echo":"ping"'
check_contains router-auth "$(curl_worker router /auth)" "session:ok"
check_contains invoke "$(dd invoke hello --path /cli)" '"greeting":"hello cli"'

python3 - "$upstream_port" <<'PY' &
import http.server, sys
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"upstream-ok"
        self.send_response(200)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass
http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), H).handle_request()
PY
upstream_pid=$!
sleep 0.3
check_contains proxy "$(curl_worker proxy / -X POST -d "http://127.0.0.1:$upstream_port/data")" "upstream-ok"
wait $upstream_pid 2>/dev/null || true

# Handshake-level websocket check; the full frame round trip lives in
# crates/wasm-host/tests/server_e2e.rs (node's WebSocket cannot set Host).
ws_status=$(curl -si --max-time 2 \
  -H "host: chat.example.com" \
  -H "connection: Upgrade" -H "upgrade: websocket" \
  -H "sec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==" -H "sec-websocket-version: 13" \
  "$(url chat /)" | head -1 || true)
check_contains chat-upgrade "$ws_status" "101"

dd delete auth >/dev/null
check_contains delete-list "$(dd list)" "router"

echo "all example checks passed"
