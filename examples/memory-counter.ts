// Per-user counters on keyed memory namespaces: commands for one key run
// under that key's lock and commit atomically (README example, wasm edition).
declare function dd_register(
  fetchHandler: (method: string, url: string, body: string) => unknown,
): void;
declare function dd_json(value: unknown): string;
declare function dd_memory_atomic(
  binding: string,
  key: string,
  command: () => unknown,
): any;
declare function dd_tvar_read(name: string): any;
declare function dd_tvar_write(name: string, value: unknown): void;

dd_register((method: string, url: string, body: string) => {
  const parsed = new URL(url);
  const user = parsed.pathname === "/" ? "anonymous" : parsed.pathname.slice(1);
  const count = dd_memory_atomic("COUNTERS", user, () => {
    const current = dd_tvar_read("count");
    const base = current === undefined || current === null ? 0 : current;
    const next = method === "POST" ? base + 1 : base;
    if (method === "POST") {
      dd_tvar_write("count", next);
    }
    return next;
  });
  return {
    status: 200,
    headers: { "content-type": "application/json" },
    body: dd_json({ user: user, count: count }),
  };
});
