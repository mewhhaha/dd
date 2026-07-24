# Chat Worker Example

This example is a small browser chat built with:

- plain HTML forms/buttons (no custom styling)
- deploy-time static assets for `fixi.js` plus a tiny websocket plugin for UI updates
- a keyed room memory namespace for per-room websocket fanout and persisted message history

## Assets

- browser files live under [examples/chat-worker/assets](assets)
- the root [examples/chat-worker/assets/_headers](assets/_headers) file applies asset-only response headers

## Deploy

```bash
export DD_PRIVATE_TOKEN=dev-token
cargo run -p cli -- --server http://127.0.0.1:8081 deploy chat examples/chat-worker/src/worker.js --memory-binding CHAT_ROOM --public --assets-dir examples/chat-worker/assets
```

If you are deploying into the Fly app through the private proxy:

```bash
just fly-proxy your-dd-app
just fly-worker-deploy chat examples/chat-worker/src/worker.js --memory-binding CHAT_ROOM --public --assets-dir examples/chat-worker/assets
```

The equivalent config-driven deploy uses the same source, binding, assets, and
production endpoint as CI:

```bash
DD_TOKEN=dddt_... cargo run -p cli -- deploy-config examples/chat-worker/dd.json
```

## CI deploy

The `Deploy Workers` GitHub Actions workflow deploys `chat` after `CI` succeeds
on `main`, then checks `https://chat.wdyt.chat/`. Mint its scoped token once
through the private proxy:

```bash
just fly-worker-mint-token \
  --name github-actions-chat \
  --worker chat \
  --public \
  --memory-binding CHAT_ROOM \
  --max-source-bytes 1048576 \
  --max-assets 256 \
  --max-asset-bytes 16777216
```

Store the returned `token` as the `DD_CHAT_DEPLOY_TOKEN` secret in the
`production` GitHub environment. The workflow carries no private control-plane
credential and the token cannot deploy another worker or add another binding.
Run the workflow manually to redeploy the current `main` commit.

## Use

- `https://chat.wdyt.chat/` shows the join page
- submit the form to enter a room at `https://chat.wdyt.chat/rooms/<room-id>?username=...&participant=...`
- websocket updates are streamed into the page as fixi-style JSON swap commands

## Notes

- the room uses smaller transactional vars (`room_id`, `next_seq`, `messages`, `participants`, `connections`) instead of one monolithic room blob
- `room.atomic(...)` is the retryable STM region; `room.tvar("key", default)` gives lazy defaults without persisting on read
- the room accepts sockets transactionally with `room.accept(request)` and then uses handle-backed `new WebSocket(handle)` objects for send/close behavior
- `room.defer(...)` is still available for arbitrary post-commit work, but ordinary websocket sends are staged automatically inside `atomic(...)`
- `/assets/fixi.js` and `/assets/ext-fixi-ws.js` are served by the deploy-time asset bundle, not hand-routed in worker code
- this simplified version keeps the last 200 messages per room
