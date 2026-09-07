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
        value: await memory.atomic((tx) => {
          const next = Number(tx.get("count") ?? 0) + 1;
          tx.put("count", next);
          return next;
        }),
      });
    }

    if (url.pathname === "/value" && request.method === "GET") {
      return json({
        ok: true,
        user: key,
        value: await memory.atomic((tx) => Number(tx.get("count") ?? 0)),
      });
    }

    if (url.pathname === "/profile" && request.method === "POST") {
      const nextProfile = {
        user: key,
        createdAt: new Date("2026-01-02T03:04:05.000Z"),
        flags: new Set(["paid", "beta"]),
        prefs: new Map([["theme", "light"]]),
      };
      const stored = await memory.atomic((tx) => {
        tx.put("profile", nextProfile);
        return tx.get("profile");
      });
      return json({ ok: true, user: key, profile: stored });
    }

    if (url.pathname === "/profile" && request.method === "GET") {
      return json({
        ok: true,
        user: key,
        profile: await memory.atomic((tx) => tx.get("profile")),
      });
    }

    if (url.pathname === "/ping" && request.method === "GET") {
      return json(await memory.atomic((tx) => {
        const next = Number(tx.get("pings") ?? 0) + 1;
        tx.put("pings", next);
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
