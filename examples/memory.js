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
    const memory = env.USER_MEMORY.get(env.USER_MEMORY.idFromName(key));
    const count = memory.tvar("count", 0);
    const profile = memory.var("profile");
    const pings = memory.tvar("pings", 0);

    if (url.pathname === "/" && request.method === "GET") {
      return json({
        ok: true,
        worker: "memory-namespace",
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
      return json({
        ok: true,
        user: key,
        value: await memory.atomic(() => {
          const next = Number(count.read()) + 1;
          count.write(next);
          return next;
        }),
      });
    }

    if (url.pathname === "/value" && request.method === "GET") {
      return json({
        ok: true,
        user: key,
        value: await memory.atomic(() => Number(count.read()) || 0),
      });
    }

    if (url.pathname === "/profile" && request.method === "POST") {
      const nextProfile = {
        user: key,
        createdAt: new Date("2026-01-02T03:04:05.000Z"),
        flags: new Set(["paid", "beta"]),
        prefs: new Map([["theme", "light"]]),
      };
      const stored = await memory.atomic(() => {
        profile.write(nextProfile);
        return profile.read();
      });
      return json({ ok: true, user: key, profile: stored });
    }

    if (url.pathname === "/profile" && request.method === "GET") {
      return json({
        ok: true,
        user: key,
        profile: await memory.atomic(() => profile.read()),
      });
    }

    if (url.pathname === "/ping" && request.method === "GET") {
      return json(await memory.atomic(() => {
        const next = Number(pings.read()) + 1;
        pings.write(next);
        return {
          ok: true,
          namespaceId: String(memory.id),
          pings: next,
        };
      }));
    }

    return json({ ok: false, error: "route not found" }, 404);
  },
};
