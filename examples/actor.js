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
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (!env.USER_ACTOR) {
      return json({ ok: false, error: "missing actor binding: USER_ACTOR" }, 500);
    }

    if (url.pathname === "/inc" && request.method === "POST") {
      const key = userKey(url);
      const actor = ctx.actor(env.USER_ACTOR, key);
      const response = await actor.fetch(`/actor/inc?user=${encodeURIComponent(key)}`, {
        method: "POST",
      });
      return response;
    }

    if (url.pathname === "/value" && request.method === "GET") {
      const key = userKey(url);
      const actor = ctx.actor(env.USER_ACTOR, key);
      const response = await actor.fetch(`/actor/value?user=${encodeURIComponent(key)}`);
      return response;
    }

    if (url.pathname === "/profile" && request.method === "POST") {
      const key = userKey(url);
      const actor = ctx.actor(env.USER_ACTOR, key);
      const response = await actor.fetch(`/actor/profile?user=${encodeURIComponent(key)}`, {
        method: "POST",
      });
      return response;
    }

    if (url.pathname === "/profile" && request.method === "GET") {
      const key = userKey(url);
      const actor = ctx.actor(env.USER_ACTOR, key);
      const response = await actor.fetch(`/actor/profile?user=${encodeURIComponent(key)}`);
      return response;
    }

    if (url.pathname === "/actor/inc" && request.method === "POST") {
      const key = userKey(url);
      const actor = ctx.actor(env.USER_ACTOR, key);
      const current = await actor.storage.get("count");
      const currentValue = current ? Number(current.value) || 0 : 0;
      const expectedVersion = current ? current.version : -1;
      const write = await actor.storage.put("count", String(currentValue + 1), {
        expectedVersion,
      });
      if (write.conflict) {
        return json({ ok: false, error: "conflict", version: write.version }, 409);
      }
      return json({ ok: true, user: key, value: currentValue + 1, version: write.version });
    }

    if (url.pathname === "/actor/value" && request.method === "GET") {
      const key = userKey(url);
      const actor = ctx.actor(env.USER_ACTOR, key);
      const current = await actor.storage.get("count");
      return json({
        ok: true,
        user: key,
        value: current ? Number(current.value) || 0 : 0,
        version: current ? current.version : -1,
      });
    }

    if (url.pathname === "/actor/profile" && request.method === "POST") {
      const key = userKey(url);
      const actor = ctx.actor(env.USER_ACTOR, key);
      const write = await actor.storage.put("profile", {
        user: key,
        createdAt: new Date("2026-01-02T03:04:05.000Z"),
        flags: new Set(["paid", "beta"]),
        prefs: new Map([["theme", "light"]]),
      });
      if (write.conflict) {
        return json({ ok: false, error: "conflict", version: write.version }, 409);
      }
      return json({ ok: true, user: key, version: write.version });
    }

    if (url.pathname === "/actor/profile" && request.method === "GET") {
      const key = userKey(url);
      const actor = ctx.actor(env.USER_ACTOR, key);
      const current = await actor.storage.get("profile");
      return json({
        ok: true,
        user: key,
        value: current ? current.value : null,
        version: current ? current.version : -1,
        encoding: current ? current.encoding : "utf8",
      });
    }

    return json({ ok: false, error: "route not found" }, 404);
  },
};
