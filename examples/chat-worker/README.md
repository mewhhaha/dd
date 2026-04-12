# Chat Worker Example

This example is a small browser chat built with:

- plain HTML forms/buttons (no custom styling)
- deploy-time static assets for `fixi.js` plus a tiny websocket plugin for UI updates
- a keyed room memory namespace for per-room websocket fanout and persisted message history

## Assets

- browser files live under [examples/chat-worker/assets](/home/mewhhaha/src/grugd/examples/chat-worker/assets)
- the root [examples/chat-worker/assets/_headers](/home/mewhhaha/src/grugd/examples/chat-worker/assets/_headers) file applies asset-only response headers

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
