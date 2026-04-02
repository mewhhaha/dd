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

export function incrementUser(state, by = 1) {
  const currentValue = Number(state.get("count") || 0) || 0;
  state.set("count", String(currentValue + Number(by || 0)));
  return currentValue + Number(by || 0);
}

export function readUserValue(state) {
  return Number(state.get("count") || 0) || 0;
}

export function updateUserProfile(state, profile) {
  state.set("profile", profile);
  return state.get("profile") ?? null;
}

export function readUserProfile(state) {
  return state.get("profile") ?? null;
}

export function pingUser(state) {
  const count = Number(state.get("pings") || 0) + 1;
  state.set("pings", String(count));
  return {
    ok: true,
    actorId: String(state.id),
    pings: count,
  };
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const key = userKey(url);
    const actor = env.USER_ACTOR.get(env.USER_ACTOR.idFromName(key));

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
      return json({ ok: true, user: key, value: await actor.atomic(incrementUser, 1) });
    }

    if (url.pathname === "/value" && request.method === "GET") {
      return json({ ok: true, user: key, value: await actor.atomic(readUserValue) });
    }

    if (url.pathname === "/profile" && request.method === "POST") {
      const profile = await actor.atomic(updateUserProfile, {
        user: key,
        createdAt: new Date("2026-01-02T03:04:05.000Z"),
        flags: new Set(["paid", "beta"]),
        prefs: new Map([["theme", "light"]]),
      });
      return json({ ok: true, user: key, profile });
    }

    if (url.pathname === "/profile" && request.method === "GET") {
      return json({ ok: true, user: key, profile: await actor.atomic(readUserProfile) });
    }

    if (url.pathname === "/ping" && request.method === "GET") {
      return json(await actor.atomic(pingUser));
    }

    return json({ ok: false, error: "route not found" }, 404);
  },
};
