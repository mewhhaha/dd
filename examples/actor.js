function json(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function userKey(url) {
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

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const key = userKey(url);
    const id = env.USER_ACTOR.idFromName(key);
    const actor = env.USER_ACTOR.get(id);

    if (url.pathname === "/" && request.method === "GET") {
      return json({
        ok: true,
        worker: "actor",
        routes: [
          "GET /ping?user={name}",
          "POST /inc?user={name}",
          "GET /value?user={name}",
          "POST /profile?user={name}",
          "GET /profile?user={name}",
        ],
      });
    }

    if (url.pathname === "/inc" && request.method === "POST") {
      const value = await actor.increment(1);
      return json({ ok: true, user: key, value });
    }

    if (url.pathname === "/value" && request.method === "GET") {
      const value = await actor.value();
      return json({ ok: true, user: key, value });
    }

    if (url.pathname === "/profile" && request.method === "POST") {
      const profile = await actor.updateProfile({
        user: key,
        createdAt: new Date("2026-01-02T03:04:05.000Z"),
        flags: new Set(["paid", "beta"]),
        prefs: new Map([["theme", "light"]]),
      });
      return json({ ok: true, user: key, profile });
    }

    if (url.pathname === "/profile" && request.method === "GET") {
      const profile = await actor.profile();
      return json({ ok: true, user: key, profile });
    }

    if (url.pathname === "/ping" && request.method === "GET") {
      return actor.fetch("/actor/ping");
    }

    return json({ ok: false, error: "route not found" }, 404);
  },
};

export class UserActor {
  constructor(state) {
    this.state = state;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/actor/ping") {
      return new Response("pong");
    }
    return new Response("not found", { status: 404 });
  }

  async increment(by = 1) {
    const current = this.state.storage.get("count");
    const currentValue = current ? Number(current.value) || 0 : 0;
    this.state.storage.put("count", String(currentValue + Number(by || 0)));
    return currentValue + Number(by || 0);
  }

  async value() {
    const current = this.state.storage.get("count");
    return current ? Number(current.value) || 0 : 0;
  }

  async updateProfile(profile) {
    this.state.storage.put("profile", profile);
    const current = this.state.storage.get("profile");
    return current?.value ?? null;
  }

  async profile() {
    const current = this.state.storage.get("profile");
    return current?.value ?? null;
  }
}
