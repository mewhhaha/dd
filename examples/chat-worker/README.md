# Chat Worker Example

This example is a small browser chat built with:

- plain HTML forms/buttons (no custom styling)
- vendored `fixi.js` plus a tiny websocket plugin for UI updates
- a keyed room memory for per-room websocket fanout and persisted message history

## Build (optional)

```bash
cp examples/chat-worker/src/worker.js examples/chat-worker/dist/worker.js
```

## Deploy

```bash
cargo run -p cli -- deploy chat examples/chat-worker/dist/worker.js --actor-binding CHAT_ROOM --public
```

If you are deploying into the Fly app through the private proxy:

```bash
just fly-proxy
just fly-worker-deploy chat examples/chat-worker/dist/worker.js --actor-binding CHAT_ROOM --public
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
- this simplified version keeps the last 200 messages per room
