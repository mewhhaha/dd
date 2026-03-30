# Chat Worker Example

This example is a small browser chat built with:

- plain HTML forms/buttons (no custom styling)
- vendored `fixi.js` plus a tiny websocket plugin for UI updates
- a room actor for per-room websocket fanout and persisted message history

## Build (optional)

```bash
cp examples/chat-worker/src/worker.js examples/chat-worker/dist/worker.js
```

## Deploy

```bash
cargo run -p cli -- deploy chat examples/chat-worker/dist/worker.js --actor-binding CHAT_ROOM=ChatRoomActor --public
```

If you are deploying into the Fly app through the private proxy:

```bash
just fly-proxy
just fly-worker-deploy chat examples/chat-worker/dist/worker.js --actor-binding CHAT_ROOM=ChatRoomActor --public
```

## Use

- `https://chat.wdyt.chat/` shows the join page
- submit the form to enter a room at `https://chat.wdyt.chat/rooms/<room-id>?username=...&participant=...`
- websocket updates are streamed into the page as fixi-style JSON swap commands

## Notes

- the actor stores room state in actor storage as structured JS values
- actor storage reads and writes are synchronous inside the actor
- this simplified version keeps the last 200 messages per room
