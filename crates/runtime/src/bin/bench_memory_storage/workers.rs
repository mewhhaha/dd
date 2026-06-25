pub(super) const MEMORY_READ_ASYNC_STORAGE_WORKER_SOURCE: &str = r#"
export function seed(state) {
  state.set("payload", "1");
  return true;
}

export function read(state) {
  return String(state.get("payload") ?? "0");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_MEMORY.idFromName(url.searchParams.get("key") ?? "hot");
    const memory = env.BENCH_MEMORY.get(id);
    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_memory_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }
    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_memory_profile_reset?.();
      return new Response("ok");
    }
    if (url.pathname === "/seed") {
      await memory.atomic(seed);
      return new Response("ok");
    }
    const value = await memory.atomic(read);
    return new Response(String(value));
  },
};
"#;

pub(super) const MEMORY_READ_ASYNC_MEMORY_WORKER_SOURCE: &str = r#"
export function read(_state) { return "1"; }

export default {
  async fetch(_request, env) {
    const url = new URL(_request.url);
    const id = env.BENCH_MEMORY.idFromName(url.searchParams.get("key") ?? "hot");
    const memory = env.BENCH_MEMORY.get(id);
    const value = await memory.atomic(read);
    return new Response(String(value));
  },
};
"#;

pub(super) const MEMORY_READ_SYNC_MEMORY_WORKER_SOURCE: &str = r#"
export function read(_state) { return "1"; }

export default {
  async fetch(_request, env) {
    const url = new URL(_request.url);
    const id = env.BENCH_MEMORY.idFromName(url.searchParams.get("key") ?? "hot");
    const memory = env.BENCH_MEMORY.get(id);
    return new Response(String(await memory.atomic(read)));
  },
};
"#;

pub(super) const MEMORY_DIRECT_READ_WORKER_SOURCE: &str = r#"
export function seed(state) {
  state.set("payload", "1");
  return true;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_MEMORY.idFromName(url.searchParams.get("key") ?? "hot");
    const memory = env.BENCH_MEMORY.get(id);
    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_memory_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }
    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_memory_profile_reset?.();
      return new Response("ok");
    }
    if (url.pathname === "/seed") {
      await memory.atomic(seed);
      return new Response("ok");
    }
    return new Response(String(await memory.read("payload") ?? "0"));
  },
};
"#;

pub(super) const MEMORY_DIRECT_WRITE_WORKER_SOURCE: &str = r#"
export function readStrong(state) {
  return String(state.get("payload") ?? "0");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_MEMORY.idFromName(url.searchParams.get("key") ?? "hot");
    const memory = env.BENCH_MEMORY.get(id);
    if (url.pathname === "/seed") {
      await memory.write("payload", "0");
      return new Response("ok");
    }
    if (url.pathname === "/read") {
      return new Response(String(await memory.read("payload") ?? "0"));
    }
    if (url.pathname === "/get-strong") {
      return new Response(String(await memory.atomic(readStrong)));
    }
    if (url.pathname === "/write") {
      await memory.write("payload", "1");
      return new Response("ok");
    }
    if (url.pathname === "/delete") {
      await memory.delete("payload");
      return new Response("ok");
    }
    return new Response("not found", { status: 404 });
  },
};
"#;

pub(super) const MEMORY_ATOMIC_READ_MEMORY_WORKER_SOURCE: &str = r#"
export function seed(state) {
  state.set("payload", "1");
  return true;
}

export function read(state) {
  return String(state.get("payload") ?? "1");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_MEMORY.idFromName(url.searchParams.get("key") ?? "hot");
    const memory = env.BENCH_MEMORY.get(id);
    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_memory_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }
    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_memory_profile_reset?.();
      return new Response("ok");
    }
    if (url.pathname === "/seed") {
      await memory.atomic(seed);
      return new Response("ok");
    }
    return new Response(String(await memory.atomic(read)));
  },
};
"#;

pub(super) const MEMORY_ATOMIC_WRITE_WORKER_SOURCE: &str = r#"
export function readPayload(state) {
  return String(state.get("payload") ?? "0");
}

export function readWritePayload(state) {
  state.get("payload");
  state.set("payload", "1");
  return "1";
}

export function writePayloadAndEffect(state) {
  state.set("payload", "1");
  state.emit("audit.bench.write", { payload: "1" });
  return "1";
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_MEMORY.idFromName(url.searchParams.get("key") ?? "hot");
    const memory = env.BENCH_MEMORY.get(id);
    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_memory_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }
    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_memory_profile_reset?.();
      return new Response("ok");
    }
    if (url.pathname === "/seed") {
      await memory.atomic(readWritePayload);
      return new Response("ok");
    }
    if (url.pathname === "/get") {
      return new Response(String(await memory.atomic(readPayload)));
    }
    if (url.pathname === "/readwrite") {
      return new Response(String(await memory.atomic(readWritePayload)));
    }
    if (url.pathname === "/write-effect") {
      return new Response(String(await memory.atomic(writePayloadAndEffect)));
    }
    return new Response("not found", { status: 404 });
  },
};
"#;

pub(super) const MEMORY_COORDINATED_INCREMENT_WORKER_SOURCE: &str = r#"
export function seed(state) {
  state.set("count", "0");
  return true;
}

export function increment(state) {
  const current = Number(state.get("count") ?? 0);
  const next = current + 1;
  state.set("count", String(next));
  return next;
}

export function readCount(state) {
  return String(state.get("count") ?? "0");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_MEMORY.idFromName(url.searchParams.get("key") ?? "hot");
    const memory = env.BENCH_MEMORY.get(id);
    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_memory_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }
    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_memory_profile_reset?.();
      return new Response("ok");
    }
    if (url.pathname === "/seed") {
      await memory.atomic(seed);
      return new Response("ok");
    }
    if (url.pathname === "/get") {
      return new Response(String(await memory.atomic(readCount)));
    }
    const value = await memory.atomic(increment);
    return new Response(String(value));
  },
};
"#;

pub(super) const MEMORY_COORDINATED_READ_WRITE_WORKER_SOURCE: &str = r#"
export function seed(state) {
  state.set("count", "0");
  return true;
}

export function readCount(state) {
  return String(state.get("count") ?? "0");
}

export function increment(state) {
  const current = Number(state.get("count") ?? 0);
  const next = current + 1;
  state.set("count", String(next));
  return next;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_MEMORY.idFromName(url.searchParams.get("key") ?? "hot");
    const memory = env.BENCH_MEMORY.get(id);
    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_memory_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }
    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_memory_profile_reset?.();
      return new Response("ok");
    }
    if (url.pathname === "/seed") {
      await memory.atomic(seed);
      return new Response("ok");
    }
    if (url.pathname === "/get") {
      return new Response(String(await memory.atomic(readCount)));
    }
    if (url.pathname === "/sum") {
      const keys = Math.max(1, Number(url.searchParams.get("keys") ?? "1") || 1);
      let total = 0;
      for (let i = 0; i < keys; i++) {
        const sumId = env.BENCH_MEMORY.idFromName(keys === 1 ? "hot" : `bench-${i}`);
        const sumMemory = env.BENCH_MEMORY.get(sumId);
        total += Number(await sumMemory.atomic(readCount));
      }
      return new Response(String(total));
    }
    if (url.pathname === "/read") {
      return new Response(String(await memory.atomic(readCount)));
    }
    if (url.pathname === "/write") {
      return new Response(String(await memory.atomic(increment)));
    }
    return new Response("not found", { status: 404 });
  },
};
"#;

pub(super) const MEMORY_ATOMIC_PUT_INCREMENT_WORKER_SOURCE: &str = r#"
export function seed(state) {
  state.set("count", "0");
  return true;
}

export function increment(state) {
  let next = 0;
  state.put("count", (previous) => {
    next = Number(previous ?? "0") + 1;
    return String(next);
  });
  return next;
}

export function readCount(state) {
  return String(state.get("count") ?? "0");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.BENCH_MEMORY.idFromName("hot");
    const memory = env.BENCH_MEMORY.get(id);
    if (url.pathname === "/seed") {
      await memory.atomic(seed);
      return new Response("ok");
    }
    if (url.pathname === "/get") {
      return new Response(String(await memory.atomic(readCount)));
    }
    return new Response(String(await memory.atomic(increment)));
  },
};
"#;

pub(super) const REALWORLD_RATE_LIMITER_WORKER_SOURCE: &str = r#"
const LIMIT = 1_000_000;
const WINDOW_MS = 60 * 60 * 1000;

function json(value, init = {}) {
  return new Response(JSON.stringify(value), {
    ...init,
    headers: [["content-type", "application/json"], ...(init.headers ?? [])],
  });
}

export function seedBucket(state) {
  state.set("count", "0");
  state.set("denied", "0");
  state.set("resetAt", "0");
  state.set("lastSeenAt", "0");
  return true;
}

export function readCount(state) {
  return String(state.get("count") ?? "0");
}

export function checkBucket(state) {
  const now = Date.now();
  const resetAt = Number(state.get("resetAt") ?? "0");
  const inWindow = resetAt > now;
  const current = inWindow ? Number(state.get("count") ?? "0") : 0;
  const nextCount = current + 1;
  const nextResetAt = inWindow ? resetAt : now + WINDOW_MS;
  const allowed = nextCount <= LIMIT;
  const denied = allowed ? Number(state.get("denied") ?? "0") : Number(state.get("denied") ?? "0") + 1;

  state.set("count", String(nextCount));
  state.set("denied", String(denied));
  state.set("resetAt", String(nextResetAt));
  state.set("lastSeenAt", String(now));

  return {
    allowed,
    count: nextCount,
    remaining: Math.max(0, LIMIT - nextCount),
    resetAt: nextResetAt,
  };
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const key = url.searchParams.get("key") ?? "hot";
    const id = env.BENCH_MEMORY.idFromName(`ratelimit:${key}`);
    const memory = env.BENCH_MEMORY.get(id);

    if (url.pathname === "/__profile") {
      return new Response(JSON.stringify(Deno.core.ops.op_memory_profile_take?.() ?? null), {
        headers: [["content-type", "application/json"]],
      });
    }
    if (url.pathname === "/__profile_reset") {
      Deno.core.ops.op_memory_profile_reset?.();
      return new Response("ok");
    }
    if (url.pathname === "/seed") {
      await memory.atomic(seedBucket);
      return new Response("ok");
    }
    if (url.pathname === "/get") {
      return new Response(String(await memory.atomic(readCount)));
    }
    if (url.pathname !== "/check") {
      return new Response("not found", { status: 404 });
    }

    const result = await memory.atomic(checkBucket);
    return json(result, {
      status: result.allowed ? 200 : 429,
      headers: [
        ["x-ratelimit-limit", String(LIMIT)],
        ["x-ratelimit-remaining", String(result.remaining)],
        ["x-ratelimit-reset", String(result.resetAt)],
      ],
    });
  },
};
"#;

pub(super) const REALWORLD_AUTH_WORKER_SOURCE: &str = r#"
const SESSION_TTL_MS = 24 * 60 * 60 * 1000;

function json(value, init = {}) {
  return new Response(JSON.stringify(value), {
    ...init,
    headers: [["content-type", "application/json"], ...(init.headers ?? [])],
  });
}

function userKey(key) {
  return `user:${key}`;
}

function username(key) {
  return `user_${key.replace(/[^a-zA-Z0-9_-]/g, "_")}`;
}

function userPayload(key) {
  return JSON.stringify({
    id: `usr_${key}`,
    username: username(key),
    roles: ["member"],
    createdAt: "2026-01-01T00:00:00.000Z",
  });
}

async function loadUser(env, key) {
  let raw = await env.AUTH_DB.get(userKey(key));
  if (raw == null) {
    raw = userPayload(key);
    await env.AUTH_DB.put(userKey(key), raw);
  }
  return JSON.parse(raw);
}

export function seedSession(state) {
  state.set("count", "0");
  state.set("lastSeenAt", "0");
  state.set("expiresAt", "0");
  return true;
}

export function readCount(state) {
  return String(state.get("count") ?? "0");
}

export function touchSession(state) {
  const now = Date.now();
  const count = Number(state.get("count") ?? "0") + 1;
  const expiresAt = now + SESSION_TTL_MS;
  state.set("count", String(count));
  state.set("lastSeenAt", String(now));
  state.set("expiresAt", String(expiresAt));
  return { count, lastSeenAt: now, expiresAt };
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const key = url.searchParams.get("key") ?? "hot";
    const id = env.AUTH_STATE.idFromName(`session:${key}`);
    const memory = env.AUTH_STATE.get(id);

    if (url.pathname === "/seed") {
      await env.AUTH_DB.put(userKey(key), userPayload(key));
      await memory.atomic(seedSession);
      return new Response("ok");
    }
    if (url.pathname === "/get") {
      return new Response(String(await memory.atomic(readCount)));
    }
    if (url.pathname !== "/api/session") {
      return new Response("not found", { status: 404 });
    }

    const session = await memory.atomic(touchSession);
    const user = await loadUser(env, key);
    return json({
      authenticated: true,
      user,
      session: {
        id: `sess_${key}`,
        touches: session.count,
        lastSeenAt: session.lastSeenAt,
        expiresAt: session.expiresAt,
      },
    });
  },
};
"#;

pub(super) const REALWORLD_AUTH_FRONTEND_WORKER_SOURCE: &str = r#"
function json(value, init = {}) {
  return new Response(JSON.stringify(value), {
    ...init,
    headers: [["content-type", "application/json"], ...(init.headers ?? [])],
  });
}

function authPath(pathname, key) {
  return `${pathname}?key=${encodeURIComponent(key)}`;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const key = url.searchParams.get("key") ?? "hot";

    if (url.pathname === "/seed") {
      return env.AUTH.fetch(authPath("/seed", key));
    }
    if (url.pathname === "/get") {
      return env.AUTH.fetch(authPath("/get", key));
    }
    if (url.pathname !== "/dashboard") {
      return new Response("not found", { status: 404 });
    }

    const authResponse = await env.AUTH.fetch(authPath("/api/session", key), {
      method: "GET",
      headers: {
        "authorization": `Bearer bench-${key}`,
        "cookie": `dd_session=sess_${key}`,
      },
    });
    if (!authResponse.ok) {
      return new Response("unauthorized", { status: 401 });
    }

    const session = JSON.parse(await authResponse.text());
    return json({
      ok: true,
      route: "dashboard",
      username: session.user.username,
      touches: session.session.touches,
    });
  },
};
"#;
