#!/usr/bin/env bash
set -euo pipefail

image="${1:-dd-server-smoke}"
suffix="${GITHUB_RUN_ID:-$$}-${RANDOM}"
volume="dd-container-smoke-${suffix}"
container=""
private_token="dd-container-smoke-token"
tmp_dir="$(mktemp -d)"
background_pids=()

cleanup() {
  for pid in "${background_pids[@]:-}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
  if [ -n "$container" ]; then
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  docker volume rm -f "$volume" >/dev/null 2>&1 || true
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

docker volume create "$volume" >/dev/null

start_container() {
  local name="$1"
  container="$name"
  docker run --detach \
    --name "$container" \
    --env "DD_PRIVATE_TOKEN=$private_token" \
    --publish 127.0.0.1::8080 \
    --publish 127.0.0.1::8081 \
    --volume "$volume:/app/store" \
    "$image" >/dev/null
}

mapped_port() {
  docker port "$container" "$1/tcp" | awk -F: 'NR == 1 { print $NF }'
}

private_url() {
  printf 'http://127.0.0.1:%s%s' "$(mapped_port 8081)" "$1"
}

public_url() {
  printf 'http://127.0.0.1:%s%s' "$(mapped_port 8080)" "$1"
}

json_get() {
  local path="$1"
  node -e '
    const fs = require("node:fs");
    const path = process.argv[1].split(".");
    let value = JSON.parse(fs.readFileSync(0, "utf8"));
    for (const part of path) value = value?.[part];
    if (value === undefined || value === null) process.exit(2);
    process.stdout.write(typeof value === "string" ? value : JSON.stringify(value));
  ' "$path"
}

wait_for_readiness() {
  local private_port=""
  for _ in $(seq 1 90); do
    if [ "$(docker inspect --format '{{.State.Running}}' "$container")" != "true" ]; then
      docker logs "$container" >&2 || true
      echo "container stopped before becoming ready" >&2
      return 1
    fi
    private_port="$(mapped_port 8081)"
    if [ -n "$private_port" ] \
      && curl --fail --silent --show-error "http://127.0.0.1:${private_port}/readyz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  docker logs "$container" >&2 || true
  echo "container did not become ready" >&2
  return 1
}

wait_for_active_requests() {
  local minimum="$1"
  local body active
  for _ in $(seq 1 100); do
    body="$(curl --fail --silent --show-error \
      --header "authorization: Bearer ${private_token}" \
      "$(private_url /v1/admin/status)")"
    active="$(json_get active_requests <<<"$body")"
    if [ "$active" -ge "$minimum" ]; then
      return 0
    fi
    sleep 0.05
  done
  echo "requests did not become active before shutdown" >&2
  return 1
}

wait_for_container_exit() {
  local expected_exit_code="$1"
  local exit_code
  for _ in $(seq 1 300); do
    if [ "$(docker inspect --format '{{.State.Running}}' "$container")" != "true" ]; then
      exit_code="$(docker inspect --format '{{.State.ExitCode}}' "$container")"
      if [ "$exit_code" != "$expected_exit_code" ]; then
        docker logs "$container" >&2 || true
        echo "container exited with ${exit_code}, expected ${expected_exit_code}" >&2
        return 1
      fi
      return 0
    fi
    sleep 0.1
  done
  docker logs "$container" >&2 || true
  echo "container did not exit within the shutdown deadline" >&2
  return 1
}

assert_server_identity() {
  local pid uid gid group_count
  pid="$(docker inspect --format '{{.State.Pid}}' "$container")"
  uid="$(awk '/^Uid:/ { print $2 }' "/proc/${pid}/status")"
  gid="$(awk '/^Gid:/ { print $2 }' "/proc/${pid}/status")"
  group_count="$(awk '/^Groups:/ { print NF - 1 }' "/proc/${pid}/status")"
  if [ "$uid" != "65532" ] || [ "$gid" != "65532" ]; then
    echo "dd_server is running as ${uid}:${gid}, expected 65532:65532" >&2
    return 1
  fi
  if [ "$group_count" != "0" ]; then
    echo "dd_server retained ${group_count} supplementary groups" >&2
    return 1
  fi
}

worker_source='const encoder = new TextEncoder();
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function memory(env) {
  return env.SMOKE_MEMORY.get(env.SMOKE_MEMORY.idFromName("state"));
}

async function cache() {
  return caches.open("container-smoke");
}

function cacheKey(name) {
  return new Request(`http://container-smoke/${name}`, { method: "GET" });
}

async function writeAll(env, name, value) {
  const opened = await cache();
  await Promise.all([
    env.SMOKE_KV.put(name, value, { durability: "committed" }),
    memory(env).write(name, value),
    opened.put(cacheKey(name), new Response(value, {
      headers: [["cache-control", "public, max-age=3600"]],
    })),
  ]);
}

async function readAll(env, name) {
  const opened = await cache();
  const [kv, durable, cached] = await Promise.all([
    env.SMOKE_KV.get(name),
    memory(env).read(name),
    opened.match(cacheKey(name)),
  ]);
  return { kv, memory: durable, cache: cached ? await cached.text() : null };
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/seed") {
      await writeAll(env, "seed", "persisted");
      return new Response("seeded");
    }
    if (url.pathname === "/write") {
      const id = String(url.searchParams.get("i") ?? "missing");
      if (id === "crash") {
        await writeAll(env, `write-${id}`, "ok");
        await new Promise(() => {});
      }
      await delay(500);
      await writeAll(env, `write-${id}`, "ok");
      return new Response(`write-${id}:ok`);
    }
    if (url.pathname === "/verify") {
      const seed = await readAll(env, "seed");
      const writes = [];
      for (let index = 0; index < 6; index += 1) {
        const state = await readAll(env, `write-${index}`);
        writes.push(state.kv === "ok" && state.memory === "ok" && state.cache === "ok");
      }
      return Response.json({ ...seed, writes });
    }
    if (url.pathname === "/verify-write") {
      const id = String(url.searchParams.get("i") ?? "missing");
      return Response.json(await readAll(env, `write-${id}`));
    }
    if (url.pathname === "/stream") {
      return new Response(new ReadableStream({
        start(controller) {
          controller.enqueue(encoder.encode("stream-start\n"));
          setTimeout(() => {
            controller.enqueue(encoder.encode("stream-end\n"));
            controller.close();
          }, 1500);
        },
      }));
    }
    return new Response("persisted");
  },
};'

deploy_persisted_worker() {
  local payload response
  payload="$(SMOKE_WORKER_SOURCE="$worker_source" node -e '
    process.stdout.write(JSON.stringify({
      name: "persisted",
      source: process.env.SMOKE_WORKER_SOURCE,
      config: {
        public: true,
        cache: { enabled: true },
        bindings: [
          { type: "kv", binding: "SMOKE_KV" },
          { type: "memory", binding: "SMOKE_MEMORY" },
        ],
      },
    }));
  ')"
  response="$(curl --fail --silent --show-error \
    --header "authorization: Bearer ${private_token}" \
    --header "content-type: application/json" \
    --data-binary "$payload" \
    "$(private_url /v1/deploy)")"
  json_get deployment_id <<<"$response"
}

mint_deploy_token() {
  local response
  response="$(curl --fail --silent --show-error \
    --header "authorization: Bearer ${private_token}" \
    --header "content-type: application/json" \
    --data-binary '{"id":"container-smoke","capabilities":{"allow_any_worker":true,"allow_public":true,"allow_private":true,"allow_any_bindings":true}}' \
    "$(private_url /v1/admin/tokens)")"
  json_get token <<<"$response"
}

assert_public_worker() {
  local worker="$1"
  local expected="$2"
  local path="${3:-/}"
  local body
  body="$(curl --fail --silent --show-error \
    --header "host: ${worker}.example.com" \
    "$(public_url "$path")")"
  if [ "$body" != "$expected" ]; then
    echo "unexpected ${worker} response: $body" >&2
    return 1
  fi
}

assert_persisted_state() {
  local expected='{"kv":"persisted","memory":"persisted","cache":"persisted","writes":[true,true,true,true,true,true]}'
  assert_public_worker persisted "$expected" /verify
}

assert_deployment_and_token_metadata() {
  local deployment_id="$1"
  local response restored_id token_id
  response="$(curl --fail --silent --show-error \
    --header "authorization: Bearer ${private_token}" \
    "$(private_url "/v1/admin/deployment?id=${deployment_id}")")"
  restored_id="$(json_get deployment.deployment_id <<<"$response")"
  if [ "$restored_id" != "$deployment_id" ]; then
    echo "restored deployment id ${restored_id} does not match ${deployment_id}" >&2
    return 1
  fi

  response="$(curl --fail --silent --show-error \
    --header "authorization: Bearer ${private_token}" \
    "$(private_url /v1/admin/tokens/container-smoke)")"
  token_id="$(json_get token.id <<<"$response")"
  if [ "$token_id" != "container-smoke" ]; then
    echo "restored token metadata is missing" >&2
    return 1
  fi
}

deploy_with_restored_token() {
  local token="$1"
  curl --fail --silent --show-error \
    --header "authorization: Bearer ${token}" \
    --header "content-type: application/json" \
    --data-binary '{"name":"token-check","source":"export default { fetch() { return new Response(\"token-restored\"); } };","config":{"public":true}}' \
    "$(public_url /v1/deploy)" >/dev/null
}

first="dd-container-smoke-first-${suffix}"
start_container "$first"
wait_for_readiness
assert_server_identity
deployment_id="$(deploy_persisted_worker)"
deploy_token="$(mint_deploy_token)"
assert_public_worker persisted seeded /seed

curl --fail --no-buffer --silent --show-error \
  --header 'host: persisted.example.com' \
  "$(public_url /stream)" >"${tmp_dir}/stream" &
stream_pid=$!
background_pids+=("$stream_pid")

write_pids=()
for index in $(seq 0 5); do
  curl --fail --silent --show-error \
    --header 'host: persisted.example.com' \
    "$(public_url "/write?i=${index}")" >"${tmp_dir}/write-${index}" &
  write_pids+=("$!")
  background_pids+=("$!")
done

wait_for_active_requests 7
docker kill --signal=SIGTERM "$container" >/dev/null
wait_for_container_exit 0
wait "$stream_pid"
for pid in "${write_pids[@]}"; do
  wait "$pid"
done
if [ "$(tr -d '\r' <"${tmp_dir}/stream")" != $'stream-start\nstream-end' ]; then
  echo "stream did not finish during graceful shutdown" >&2
  exit 1
fi
for index in $(seq 0 5); do
  if [ "$(<"${tmp_dir}/write-${index}")" != "write-${index}:ok" ]; then
    echo "concurrent write ${index} did not finish during graceful shutdown" >&2
    exit 1
  fi
done
first_logs="$(docker logs "$container" 2>&1)"
if ! grep -Fq "dd_init: repaired /app/store ownership for 65532:65532" <<<"$first_logs"; then
  echo "first boot did not report a volume ownership repair" >&2
  printf '%s\n' "$first_logs" >&2
  exit 1
fi
docker rm "$container" >/dev/null
container=""
background_pids=()

second="dd-container-smoke-second-${suffix}"
start_container "$second"
wait_for_readiness
assert_server_identity
assert_persisted_state
assert_deployment_and_token_metadata "$deployment_id"
deploy_with_restored_token "$deploy_token"
assert_public_worker token-check token-restored

crash_output="${tmp_dir}/write-crash"
curl --fail --silent --show-error \
  --header 'host: persisted.example.com' \
  "$(public_url '/write?i=crash')" >"$crash_output" 2>"${crash_output}.err" &
crash_pid=$!
background_pids+=("$crash_pid")

crash_state=''
for _ in $(seq 1 100); do
  crash_state="$(curl --fail --silent --show-error \
    --header 'host: persisted.example.com' \
    "$(public_url '/verify-write?i=crash')" 2>/dev/null || true)"
  if [ "$crash_state" = '{"kv":"ok","memory":"ok","cache":"ok"}' ]; then
    break
  fi
  sleep 0.05
done
if [ "$crash_state" != '{"kv":"ok","memory":"ok","cache":"ok"}' ]; then
  echo "crash write did not become durably observable" >&2
  exit 1
fi
if ! kill -0 "$crash_pid" 2>/dev/null; then
  echo "crash write request completed before SIGKILL" >&2
  exit 1
fi
docker kill --signal=SIGKILL "$container" >/dev/null
wait_for_container_exit 137
if wait "$crash_pid"; then
  echo "crash write request completed successfully after SIGKILL" >&2
  exit 1
fi
second_logs="$(docker logs "$container" 2>&1)"
if ! grep -Fq "dd_init: ownership marker is current; skipping repair" <<<"$second_logs"; then
  echo "second boot did not reuse the ownership marker" >&2
  printf '%s\n' "$second_logs" >&2
  exit 1
fi
docker rm "$container" >/dev/null
container=""

third="dd-container-smoke-third-${suffix}"
start_container "$third"
wait_for_readiness
assert_server_identity
assert_persisted_state
assert_public_worker persisted '{"kv":"ok","memory":"ok","cache":"ok"}' '/verify-write?i=crash'
assert_public_worker token-check token-restored
assert_deployment_and_token_metadata "$deployment_id"

echo "container shutdown, crash recovery, storage, tokens, deployments, and privilege-drop smoke passed"
