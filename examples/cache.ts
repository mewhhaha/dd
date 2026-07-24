// Worker-scoped response cache: first hit computes, repeats serve cached.
declare function dd_register(
  fetchHandler: (method: string, url: string, body: string) => unknown,
): void;
declare function dd_cache_match(key: string): any;
declare function dd_cache_put(key: string, response: unknown): void;

dd_register((method: string, url: string, body: string) => {
  const cached = dd_cache_match(url);
  if (cached !== null) {
    return { status: 200, headers: { "x-cache": "hit" }, body: cached.body };
  }
  const fresh = {
    status: 200,
    headers: { "cache-control": "public, max-age=60" },
    body: "fresh response",
  };
  dd_cache_put(url, fresh);
  return { status: 200, headers: { "x-cache": "miss" }, body: fresh.body };
});
