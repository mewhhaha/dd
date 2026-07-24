// Exercises KV and keyed memory namespaces through the dd wasm host.
declare function dd_register(
  fetchHandler: (method: string, url: string, body: string) => unknown,
): void;
declare function dd_json(value: unknown): string;
declare function dd_kv_get(binding: string, key: string): string | null;
declare function dd_kv_set(binding: string, key: string, value: string): void;
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
    const next = (current === undefined || current === null ? 0 : current) + 1;
    dd_tvar_write("count", next);
    return next;
  });
  const previous = dd_kv_get("MY_KV", "last-user");
  dd_kv_set("MY_KV", "last-user", user);
  return {
    status: 200,
    headers: { "content-type": "application/json" },
    body: dd_json({ user: user, count: count, previous: previous }),
  };
});
