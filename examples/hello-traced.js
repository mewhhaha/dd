function json(payload, status = 200, headers = {}) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...headers,
    },
  });
}

function html(pageUser) {
  const safeUser = pageUser.replace(/[<>"&]/g, (ch) => (
    ch === "<" ? "&lt;" : ch === ">" ? "&gt;" : ch === "\"" ? "&quot;" : "&amp;"
  ));
  const encodedUser = encodeURIComponent(pageUser);
  return `<!doctype html>
<html><head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>hello-traced</title>
  <style>
    body { font-family: ui-monospace, Menlo, monospace; margin: 24px; background: #0b1324; color: #ecf2ff; }
    a, button { color: #0b1324; background: #93c5fd; border: 0; padding: 8px 10px; border-radius: 8px; text-decoration: none; margin-right: 8px; }
    code { color: #a7f3d0; }
    .row { margin: 12px 0; }
  </style>
</head><body>
  <h1>hello-traced</h1>
  <p>Demo user: <code>${safeUser}</code></p>
  <p>Features: KV, actor namespace fetch, cache API, stream response, websocket status endpoint.</p>
  <div class="row"><a href="/api/state?user=${encodedUser}">GET /api/state</a><a href="/api/cache">GET /api/cache</a><a href="/api/actor/ping?user=${encodedUser}">GET /api/actor/ping</a></div>
  <div class="row"><a href="/api/stream">GET /api/stream</a><a href="/api/ws">GET /api/ws</a></div>
  <form method="post" action="/api/hit?user=${encodedUser}"><button type="submit">POST /api/hit</button></form>
</body></html>`;
}

function userFromRequest(request) {
  const url = new URL(request.url);
  const raw = String(url.search || "").replace(/^\?/, "");
  if (!raw) {
    return "anonymous";
  }
  for (const pair of raw.split("&")) {
    if (!pair) {
      continue;
    }
    const [key, value = ""] = pair.split("=");
    if (decodeURIComponent(key) === "user") {
      const decoded = decodeURIComponent(value);
      return decoded || "anonymous";
    }
  }
  return "anonymous";
}

async function readTotal(kv) {
  const raw = await kv.get("stats:total");
  return Number(raw || 0) || 0;
}

async function writeCacheSnapshot(cache, payload) {
  const key = new Request("http://cache/hello-traced/snapshot", { method: "GET" });
  const response = json(payload, 200, {
    "cache-control": "public, max-age=30",
    "x-dd-cache-created-at": String(payload.createdAtMs),
  });
  await cache.put(key, response.clone());
  return response;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const user = userFromRequest(request);
    const kv = env.APP_KV;

    if (url.pathname === "/" && request.method === "GET") {
      return new Response(html(user), {
        headers: {
          "content-type": "text/html; charset=utf-8",
          "cache-control": "no-store",
        },
      });
    }

    if (url.pathname === "/api/hit" && request.method === "POST") {
      const total = (await readTotal(kv)) + 1;
      await kv.set("stats:total", String(total));
      const event = {
        user,
        total,
        at: new Date().toISOString(),
      };
      await kv.set(`event:last:${user}`, event);
      return json({ ok: true, feature: "kv", event });
    }

    if (url.pathname === "/api/state" && request.method === "GET") {
      const [total, lastEvent] = await Promise.all([
        readTotal(kv),
        kv.get(`event:last:${user}`),
      ]);
      return json({
        ok: true,
        feature: "kv",
        user,
        total,
        lastEvent,
      });
    }

    if (url.pathname === "/api/actor/ping" && request.method === "GET") {
      const actor = env.USER_ACTOR.get(env.USER_ACTOR.idFromName(user));
      const response = await actor.fetch("http://actor/ping");
      if (response instanceof Response) {
        return response;
      }
      return json({ ok: false, error: "actor did not return a Response" }, 500);
    }

    if (url.pathname === "/api/cache" && request.method === "GET") {
      const cache = await caches.open("hello-traced-v1");
      const key = new Request("http://cache/hello-traced/snapshot", { method: "GET" });
      const cached = await cache.match(key);
      if (cached) {
        return json({
          ok: true,
          feature: "cache",
          cacheStatus: "HIT",
          payload: await cached.json(),
        });
      }
      const payload = {
        cacheStatus: "MISS",
        createdAtMs: Date.now(),
        hint: "POST /api/cache/refresh to rotate value",
      };
      await writeCacheSnapshot(cache, payload);
      return json({ ok: true, feature: "cache", cacheStatus: "MISS", payload });
    }

    if (url.pathname === "/api/cache/refresh" && request.method === "POST") {
      const cache = await caches.open("hello-traced-v1");
      const payload = {
        cacheStatus: "REFRESHED",
        createdAtMs: Date.now(),
        token: Math.random().toString(36).slice(2, 10),
      };
      await writeCacheSnapshot(cache, payload);
      return json({ ok: true, feature: "cache", cacheStatus: "REFRESHED", payload });
    }

    if (url.pathname === "/api/stream" && request.method === "GET") {
      return json({
        ok: false,
        feature: "stream",
        supported: false,
        note: "ReadableStream responses are not stable in this runtime build yet.",
      }, 501);
    }

    if (url.pathname === "/api/ws" && request.method === "GET") {
      return json({
        ok: false,
        feature: "websocket",
        supported: false,
        note: "WebSocket upgrade is not implemented in this runtime yet.",
      }, 501);
    }

    return json({ ok: false, error: "not found" }, 404);
  },
};

export class UserActor {
  constructor(state) {
    this.state = state;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/ping") {
      const current = await this.state.storage.get("pings");
      const count = Number(current?.value || 0) + 1;
      await this.state.storage.put("pings", String(count));
      return json({
        ok: true,
        feature: "actor-fetch",
        actorId: String(this.state.id),
        pings: count,
      });
    }
    return json({ ok: false, error: "not found" }, 404);
  }
}
