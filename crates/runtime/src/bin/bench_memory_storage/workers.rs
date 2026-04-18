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

pub(super) const MEMORY_ATOMIC_READ_ALLOW_CONCURRENCY_MEMORY_WORKER_SOURCE: &str = r#"
export function seed(state) {
  state.set("payload", "1");
  return true;
}

export function read(state) {
  return String(state.get("payload", { allowConcurrency: true }) ?? "1");
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

pub(super) const MEMORY_STM_INCREMENT_WORKER_SOURCE: &str = r#"
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

pub(super) const MEMORY_STM_READ_WRITE_WORKER_SOURCE: &str = r#"
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

pub(super) const MEMORY_STM_READ_WRITE_ALLOW_CONCURRENCY_WORKER_SOURCE: &str = r#"
export function seed(state) {
  state.set("count", "0");
  return true;
}

export function readCount(state) {
  return String(state.get("count", { allowConcurrency: true }) ?? "0");
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

pub(super) const MEMORY_STM_BLIND_WRITE_WORKER_SOURCE: &str = r#"
export function seed(state) {
  state.set("count", "0");
  return true;
}

export function readCount(state) {
  return String(state.get("count") ?? "0");
}

export function blindWrite(state) {
  state.set("count", "1");
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
      await memory.atomic(seed);
      return new Response("ok");
    }
    if (url.pathname === "/get") {
      return new Response(String(await memory.atomic(readCount)));
    }
    if (url.pathname === "/write") {
      return new Response(String(await memory.atomic(blindWrite)));
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
