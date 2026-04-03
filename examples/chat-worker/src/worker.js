const MAX_ROOM_ID_LENGTH = 64;
const MAX_PARTICIPANT_ID_LENGTH = 80;
const MAX_USERNAME_LENGTH = 40;
const MAX_MESSAGE_LENGTH = 2000;
const MAX_HISTORY = 200;
const FIXI_VERSION = "0.9.3";
const FIXI_WS_PLUGIN_VERSION = "1";

// Vendored from https://github.com/bigskysoftware/fixi v0.9.3 (Zero-Clause BSD).
const FIXI_JS = String.raw`(()=>{ if(document.__fixi_mo) return; document.__fixi_mo = new MutationObserver((recs)=>recs.forEach((r)=>r.type === "childList" && r.addedNodes.forEach((n)=>process(n)))) let send = (elt, type, detail, bub)=>elt.dispatchEvent(new CustomEvent("fx:" + type, {detail, cancelable:true, bubbles:bub !== false, composed:true})) let attr = (elt, name, defaultVal)=>elt.getAttribute(name) || defaultVal let ignore = (elt)=>elt.closest("[fx-ignore]") != null let init = (elt)=>{ let options = {} if (elt.__fixi || ignore(elt) || !send(elt, "init", {options})) return elt.__fixi = async(evt)=>{ let reqs = elt.__fixi.requests ||= new Set() let form = elt.form || elt.closest("form") let body = new FormData(form ?? undefined, evt.submitter) if (elt.name && !evt.submitter && (!form || (elt.form === form && elt.type === 'submit'))) body.append(elt.name, elt.value) let ac = new AbortController() let cfg = { trigger:evt, action:attr(elt, "fx-action"), method:attr(elt, "fx-method", "GET").toUpperCase(), target:document.querySelector(attr(elt, "fx-target")) ?? elt, swap:attr(elt, "fx-swap", "outerHTML"), body, drop:reqs.size, headers:{"FX-Request":"true"}, abort:ac.abort.bind(ac), signal:ac.signal, preventTrigger:true, transition:document.startViewTransition?.bind(document), fetch:fetch.bind(window) } let go = send(elt, "config", {cfg, requests:reqs}) if (cfg.preventTrigger) evt.preventDefault() if (!go || cfg.drop) return if (/GET|DELETE/.test(cfg.method)){ let params = new URLSearchParams(cfg.body) if (params.size) cfg.action += (/\?/.test(cfg.action) ? "&" : "?") + params cfg.body = null } reqs.add(cfg) try { if (cfg.confirm){ let result = await cfg.confirm() if (!result) return } if (!send(elt, "before", {cfg, requests:reqs})) return cfg.response = await cfg.fetch(cfg.action, cfg) cfg.text = await cfg.response.text() if (!send(elt, "after", {cfg})) return } catch(error) { send(elt, "error", {cfg, error}) return } finally { reqs.delete(cfg) send(elt, "finally", {cfg}) } let doSwap = ()=>{ if (cfg.swap instanceof Function) return cfg.swap(cfg) else if (/(before|after)(begin|end)/.test(cfg.swap)) cfg.target.insertAdjacentHTML(cfg.swap, cfg.text) else if(cfg.swap in cfg.target) cfg.target[cfg.swap] = cfg.text else if(cfg.swap !== 'none') throw cfg.swap } if (cfg.transition) await cfg.transition(doSwap).finished else await doSwap() send(elt, "swapped", {cfg}) if (!document.contains(elt)) send(document, "swapped", {cfg}) } elt.__fixi.evt = attr(elt, "fx-trigger", elt.matches("form") ? "submit" : elt.matches("input:not([type=button]),select,textarea") ? "change" : "click") elt.addEventListener(elt.__fixi.evt, elt.__fixi, options) send(elt, "inited", {}, false) } let process = (n)=>{ if (n.matches){ if (ignore(n)) return if (n.matches("[fx-action]")) init(n) } if(n.querySelectorAll) n.querySelectorAll("[fx-action]").forEach(init) } document.addEventListener("fx:process", (evt)=>process(evt.target)) document.addEventListener("DOMContentLoaded", ()=>{ document.__fixi_mo.observe(document.documentElement, {childList:true, subtree:true}) process(document.body) }) })()`;

const FIXI_WS_JS = String.raw`(()=>{
  if (document.__fixi_ws_plugin) return;
  document.__fixi_ws_plugin = true;

  const roots = new WeakSet();
  const senders = new WeakSet();
  const transition = document.startViewTransition?.bind(document);

  function setStatus(root, status) {
    root.dataset.wsState = status;
    const selector = root.getAttribute("ext-fx-ws-status");
    if (!selector) return;
    const target = document.querySelector(selector);
    if (target) target.textContent = status;
  }

  function resolveSocketUrl(value) {
    const url = new URL(value, window.location.href);
    if (url.protocol === "http:") url.protocol = "ws:";
    if (url.protocol === "https:") url.protocol = "wss:";
    return url.toString();
  }

  function applySwap(target, swap, text) {
    if (/(before|after)(begin|end)/.test(swap)) target.insertAdjacentHTML(swap, text);
    else if (swap in target) target[swap] = text;
    else if (swap !== "none") throw new Error("unsupported swap " + swap);
  }

  async function applyPayload(payload) {
    const swaps = Array.isArray(payload) ? payload : [payload];
    for (const swap of swaps) {
      if (!swap || typeof swap !== "object" || typeof swap.target !== "string") continue;
      const mode = typeof swap.swap === "string" ? swap.swap : "innerHTML";
      const text = typeof swap.text === "string" ? swap.text : "";
      for (const target of document.querySelectorAll(swap.target)) {
        if (transition) await transition(() => applySwap(target, mode, text)).finished;
        else applySwap(target, mode, text);
      }
    }
  }

  function initRoot(root) {
    if (roots.has(root)) return;
    roots.add(root);
    const connectValue = root.getAttribute("ext-fx-ws-connect");
    if (!connectValue) return;
    const openValue = root.getAttribute("ext-fx-ws-open");
    const shouldReconnect = root.getAttribute("ext-fx-ws-reconnect") !== "false";
    let reconnectDelayMs = 500;

    const connect = () => {
      setStatus(root, "connecting");
      const socket = new WebSocket(resolveSocketUrl(connectValue));
      root.__fixiWs = socket;
      socket.addEventListener("open", () => {
        reconnectDelayMs = 500;
        setStatus(root, "open");
        if (!openValue) return;
        try {
          socket.send(openValue);
        } catch {
          setStatus(root, "error");
        }
      });
      socket.addEventListener("message", async (event) => {
        try {
          await applyPayload(JSON.parse(event.data));
          document.dispatchEvent(new CustomEvent("fx:process", { bubbles: true }));
        } catch {
          setStatus(root, "error");
        }
      });
      socket.addEventListener("error", () => setStatus(root, "error"));
      socket.addEventListener("close", () => {
        setStatus(root, "closed");
        if (!shouldReconnect) return;
        window.setTimeout(connect, reconnectDelayMs);
        reconnectDelayMs = Math.min(reconnectDelayMs * 2, 5000);
      });
    };

    connect();
  }

  function initSender(element) {
    if (!(element instanceof HTMLFormElement) || senders.has(element)) return;
    senders.add(element);
    element.addEventListener("submit", (event) => {
      event.preventDefault();
      const root = element.closest("[ext-fx-ws-connect]");
      const socket = root?.__fixiWs;
      if (!root || !socket || socket.readyState !== WebSocket.OPEN) {
        if (root) setStatus(root, "waiting");
        return;
      }
      const payload = Object.fromEntries(new FormData(element, event.submitter).entries());
      socket.send(JSON.stringify(payload));
      if (element.matches("[ext-fx-ws-reset]")) element.reset();
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll("[ext-fx-ws-connect]").forEach(initRoot);
    document.querySelectorAll("form[ext-fx-ws-send]").forEach(initSender);
  });

  document.addEventListener("fx:process", (event) => {
    const target = event.target;
    if (target instanceof Element && target.matches("[ext-fx-ws-connect]")) initRoot(target);
    if (target?.querySelectorAll) target.querySelectorAll("[ext-fx-ws-connect]").forEach(initRoot);
    if (target instanceof Element && target.matches("form[ext-fx-ws-send]")) initSender(target);
    if (target?.querySelectorAll) target.querySelectorAll("form[ext-fx-ws-send]").forEach(initSender);
  });
})();`;

function html(body, status = 200) {
  return new Response(body, {
    status,
    headers: {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function json(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function text(value, status = 200) {
  return new Response(value, {
    status,
    headers: {
      "content-type": "text/plain; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function script(value, status = 200) {
  return new Response(value, {
    status,
    headers: {
      "content-type": "application/javascript; charset=utf-8",
      "cache-control": "public, max-age=3600",
    },
  });
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (character) => {
    switch (character) {
      case "&":
        return "&amp;";
      case "<":
        return "&lt;";
      case ">":
        return "&gt;";
      case "\"":
        return "&quot;";
      case "'":
        return "&#39;";
      default:
        return character;
    }
  });
}

function normalizeRoomId(input) {
  const value = String(input ?? "").trim().toLowerCase();
  if (!value || value.length > MAX_ROOM_ID_LENGTH) {
    return null;
  }
  if (!/^[a-z0-9][a-z0-9-_]*$/.test(value)) {
    return null;
  }
  return value;
}

function normalizeParticipantId(input) {
  const value = String(input ?? "").trim();
  if (!value || value.length > MAX_PARTICIPANT_ID_LENGTH) {
    return null;
  }
  if (!/^[a-zA-Z0-9:_-]+$/.test(value)) {
    return null;
  }
  return value;
}

function normalizeUsername(input) {
  const value = String(input ?? "").trim().replace(/\s+/g, " ");
  if (!value || value.length > MAX_USERNAME_LENGTH) {
    return null;
  }
  return value;
}

function nowIso() {
  return new Date().toISOString();
}

function externalOrigin(request, fallbackUrl) {
  const headers = request.headers;
  const host = String(headers.get("x-forwarded-host") ?? headers.get("host") ?? "").trim();
  const protoHeader = String(headers.get("x-forwarded-proto") ?? "").trim();
  const proto = protoHeader
    ? protoHeader.replace(/:$/, "")
    : new URL(fallbackUrl).protocol.replace(/:$/, "");
  if (host) {
    return `${proto}://${host}`;
  }
  return new URL(fallbackUrl).origin;
}

function roomActor(env, roomId) {
  const id = env.CHAT_ROOM.idFromName(roomId);
  return env.CHAT_ROOM.get(id);
}

function roomMemory(room) {
  return {
    roomId: room.tvar("room_id", String(room.id)),
    nextSeq: room.tvar("next_seq", 0),
    messages: room.tvar("messages", []),
    participants: room.tvar("participants", {}),
    connections: room.tvar("connections", {}),
  };
}

function roomSnapshot(room) {
  const mem = roomMemory(room);
  return {
    roomId: String(mem.roomId.read()),
    nextSeq: Number(mem.nextSeq.read() ?? 0) || 0,
    messages: mem.messages.read(),
    participants: mem.participants.read(),
    connections: mem.connections.read(),
  };
}

function roomStateResponse(room) {
  const snapshot = roomSnapshot(room);
  return {
    roomId: snapshot.roomId,
    messages: snapshot.messages,
    participants: listConnectedParticipants(snapshot, roomConnectionHandles(snapshot)),
  };
}

function modifyRoom(room, updater) {
  const current = roomSnapshot(room);
  const next = updater({
    roomId: current.roomId,
    nextSeq: current.nextSeq,
    messages: structuredClone(current.messages),
    participants: structuredClone(current.participants),
    connections: structuredClone(current.connections),
  });
  const snapshot = next ?? current;
  const mem = roomMemory(room);
  mem.roomId.write(snapshot.roomId);
  mem.nextSeq.write(snapshot.nextSeq);
  mem.messages.write(snapshot.messages);
  mem.participants.write(snapshot.participants);
  mem.connections.write(snapshot.connections);
  return snapshot;
}

function deferRoomEvents(room, events) {
  for (const event of events) {
    if (!event || typeof event !== "object") {
      continue;
    }
    switch (event.type) {
      case "session_replaced": {
        for (const handle of event.handles ?? []) {
          const socket = new WebSocket(handle);
          socket.close(1001, "replaced");
        }
        break;
      }
      case "participants_changed": {
        const payload = JSON.stringify(
          fixiSwap(
            "#participants",
            "innerHTML",
            renderParticipantsItems(event.room, roomConnectionHandles(event.room)),
          ),
        );
        for (const handle of event.handles ?? []) {
          const socket = new WebSocket(handle);
          socket.send(payload, "text");
        }
        break;
      }
      case "room_snapshot": {
        const payload = JSON.stringify([
          fixiSwap("#messages", "innerHTML", renderMessagesItems(event.room)),
          fixiSwap(
            "#participants",
            "innerHTML",
            renderParticipantsItems(event.room, roomConnectionHandles(event.room)),
          ),
        ]);
        const socket = new WebSocket(event.handle);
        socket.send(payload, "text");
        break;
      }
      case "message_posted": {
        const payload = JSON.stringify(
          fixiSwap("#messages", "beforeend", renderMessageItem(event.message)),
        );
        for (const handle of event.handles ?? []) {
          const socket = new WebSocket(handle);
          socket.send(payload, "text");
        }
        break;
      }
      default:
        break;
    }
  }
}

function roomProcess(room) {
  return {
    openSocket(payload) {
      const { handle, response } = room.accept(payload.request);
      let nextRoom = null;
      let replacedHandles = [];
      modifyRoom(room, (snapshot) => {
        snapshot.roomId = payload.roomId;
        replacedHandles = roomConnectionHandles(snapshot).filter(
          (existingHandle) => (
            existingHandle !== handle
            && snapshot.connections[existingHandle] === payload.participantId
          ),
        );
        snapshot.participants[payload.participantId] = {
          id: payload.participantId,
          username: payload.username,
          joinedAt: snapshot.participants[payload.participantId]?.joinedAt ?? nowIso(),
        };
        for (const existingHandle of replacedHandles) {
          delete snapshot.connections[existingHandle];
        }
        snapshot.connections[handle] = payload.participantId;
        nextRoom = structuredClone(snapshot);
        return snapshot;
      });
      deferRoomEvents(room, [
        { type: "session_replaced", handles: replacedHandles },
        {
          type: "participants_changed",
          handles: roomConnectionHandles(nextRoom).filter((existingHandle) => existingHandle !== handle),
          room: nextRoom,
        },
      ]);
      return response;
    },

    clientReady(handle) {
      const snapshot = roomSnapshot(room);
      deferRoomEvents(room, [{ type: "room_snapshot", handle, room: snapshot }]);
      return true;
    },

    postMessage(handle, text) {
      const snapshot = roomSnapshot(room);
      const participantId = snapshot.connections[handle];
      if (!participantId) {
        return false;
      }
      const participant = snapshot.participants[participantId];
      if (!participant) {
        return false;
      }
      const body = String(text ?? "").trim();
      if (!body || body.length > MAX_MESSAGE_LENGTH) {
        return false;
      }
      let nextMessage = null;
      let handles = [];
      modifyRoom(room, (next) => {
        nextMessage = {
          seq: next.nextSeq + 1,
          participantId,
          username: participant.username,
          text: body,
          timestamp: nowIso(),
        };
        next.nextSeq = nextMessage.seq;
        next.messages.push(nextMessage);
        if (next.messages.length > MAX_HISTORY) {
          next.messages.splice(0, next.messages.length - MAX_HISTORY);
        }
        handles = roomConnectionHandles(next);
        return next;
      });
      deferRoomEvents(room, [{ type: "message_posted", handles, message: nextMessage }]);
      return true;
    },

    disconnect(handle) {
      let nextRoom = null;
      let removed = false;
      modifyRoom(room, (snapshot) => {
        if (!snapshot.connections[handle]) {
          return snapshot;
        }
        removed = true;
        delete snapshot.connections[handle];
        nextRoom = structuredClone(snapshot);
        return snapshot;
      });
      if (!removed) {
        return false;
      }
      deferRoomEvents(room, [{
        type: "participants_changed",
        handles: roomConnectionHandles(nextRoom),
        room: nextRoom,
      }]);
      return true;
    },
  };
}

function joinPage(prefillRoom = "") {
  return `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>dd chat</title>
  </head>
  <body>
    <h1>dd chat</h1>
    <form method="get" action="/enter">
      <p>
        <label>Room<br /><input name="room" value="${escapeHtml(prefillRoom)}" required /></label>
      </p>
      <p>
        <label>Username<br /><input name="username" required /></label>
      </p>
      <p>
        <label>Participant id<br /><input name="participant" required /></label>
      </p>
      <p><button type="submit">Join</button></p>
    </form>
  </body>
</html>`;
}

function roomPage(roomId, username, participantId) {
  const room = escapeHtml(roomId);
  const user = escapeHtml(username);
  const participant = escapeHtml(participantId);
  const wsPath = `/rooms/${encodeURIComponent(roomId)}/ws?username=${encodeURIComponent(username)}&participant=${encodeURIComponent(participantId)}`;
  return `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>dd chat ${room}</title>
    <script src="/assets/fixi.js?v=${FIXI_VERSION}"></script>
    <script src="/assets/ext-fixi-ws.js?v=${FIXI_WS_PLUGIN_VERSION}"></script>
  </head>
  <body>
    <h1>Room ${room}</h1>
    <p>Username: <strong>${user}</strong> | Participant: <code>${participant}</code></p>

    <div ext-fx-ws-connect="${wsPath}" ext-fx-ws-open='{"type":"ready"}' ext-fx-ws-status="#connection-status">
      <p>Socket: <strong id="connection-status">connecting</strong></p>

      <h2>Messages</h2>
      <ul id="messages"></ul>

      <form ext-fx-ws-send ext-fx-ws-reset>
        <input type="hidden" name="type" value="message" />
        <input name="text" maxlength="${MAX_MESSAGE_LENGTH}" required />
        <button type="submit">Send</button>
      </form>

      <h2>Participants</h2>
      <ul id="participants"></ul>
    </div>
  </body>
</html>`;
}

function listConnectedParticipants(room, openHandles) {
  const seen = new Set();
  const participants = [];
  for (const handle of openHandles) {
    const participantId = room.connections[handle];
    if (!participantId || seen.has(participantId)) {
      continue;
    }
    seen.add(participantId);
    const participant = room.participants[participantId];
    if (!participant) {
      continue;
    }
    participants.push({
      id: participant.id,
      username: participant.username,
    });
  }
  participants.sort((a, b) => a.username.localeCompare(b.username));
  return participants;
}

function roomConnectionHandles(room) {
  return Object.keys(room.connections);
}

function parseSocketPayload(data) {
  if (typeof data !== "string") {
    return null;
  }
  try {
    const payload = JSON.parse(data);
    if (!payload || typeof payload !== "object") {
      return null;
    }
    return payload;
  } catch {
    return null;
  }
}

function renderParticipantsItems(room, openHandles) {
  return listConnectedParticipants(room, openHandles)
    .map(
      (participant) =>
        `<li>${escapeHtml(participant.username)} <code>${escapeHtml(participant.id)}</code></li>`,
    )
    .join("");
}

function renderMessagesItems(room) {
  return room.messages.map((message) => renderMessageItem(message)).join("");
}

function renderMessageItem(message) {
  return `<li><strong>${escapeHtml(message.username)}</strong> [${escapeHtml(message.timestamp)}]: ${escapeHtml(message.text)}</li>`;
}

function fixiSwap(target, swap, text) {
  return { target, swap, text };
}

function parseRoomPath(pathname) {
  const match = pathname.match(/^\/rooms\/([^/]+)(?:\/(ws|state))?$/);
  if (!match) {
    return null;
  }
  return {
    roomId: normalizeRoomId(decodeURIComponent(match[1])),
    action: match[2] ?? "",
  };
}

export default {
  async fetch(request, env, ctx) {
    const _ = ctx;
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/") {
      return html(joinPage(url.searchParams.get("room") ?? ""));
    }

    if (request.method === "GET" && url.pathname === "/assets/fixi.js") {
      return script(FIXI_JS);
    }

    if (request.method === "GET" && url.pathname === "/assets/ext-fixi-ws.js") {
      return script(FIXI_WS_JS);
    }

    if (request.method === "GET" && url.pathname === "/favicon.ico") {
      return new Response(null, { status: 204 });
    }

    if (request.method === "GET" && url.pathname === "/enter") {
      const roomId = normalizeRoomId(url.searchParams.get("room"));
      const username = normalizeUsername(url.searchParams.get("username"));
      const participantId = normalizeParticipantId(url.searchParams.get("participant"));
      if (!roomId || !username || !participantId) {
        return json({ ok: false, error: "room, username, and participant are required" }, 400);
      }
      const origin = externalOrigin(request, request.url);
      const target = `${origin}/rooms/${encodeURIComponent(roomId)}?username=${encodeURIComponent(username)}&participant=${encodeURIComponent(participantId)}`;
      return Response.redirect(target, 302);
    }

    if (request.method === "GET") {
      const parsed = parseRoomPath(url.pathname);
      if (!parsed || !parsed.roomId) {
        return text("not found", 404);
      }
      const { roomId, action } = parsed;
      if (action === "ws") {
        const username = normalizeUsername(url.searchParams.get("username"));
        const participantId = normalizeParticipantId(url.searchParams.get("participant"));
        if (!username || !participantId) {
          return json({ ok: false, error: "missing room, username, or participant id" }, 400);
        }
        const room = roomActor(env, roomId);
        return await room.atomic(() => roomProcess(room).openSocket({
          request,
          roomId,
          username,
          participantId,
        }));
      }
      if (action === "state") {
        const room = roomActor(env, roomId);
        return json({ ok: true, ...(await room.atomic(() => roomStateResponse(room))) });
      }
      const username = normalizeUsername(url.searchParams.get("username"));
      const participantId = normalizeParticipantId(url.searchParams.get("participant"));
      if (!username || !participantId) {
        const origin = externalOrigin(request, request.url);
        return Response.redirect(`${origin}/?room=${encodeURIComponent(roomId)}`, 302);
      }
      return html(roomPage(roomId, username, participantId));
    }

    return text("not found", 404);
  },
  async wake(event, env) {
    const _ = env;
    const stub = event?.stub ?? null;
    if (!stub) {
      return;
    }
    if (event.type === "socketmessage") {
      await stub.atomic(() => {
        const payload = parseSocketPayload(event.data);
        if (!payload) {
          return false;
        }
        const process = roomProcess(stub);
        switch (String(payload.type ?? "")) {
          case "ready":
            return process.clientReady(event.handle);
          case "message":
            return process.postMessage(event.handle, payload.text);
          default:
            return false;
        }
      });
      return;
    }
    if (event.type === "socketclose") {
      await stub.atomic(() => roomProcess(stub).disconnect(event.handle));
    }
  },
};
