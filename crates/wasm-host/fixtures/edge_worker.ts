// Exercises the cache, service bindings, and outbound fetch.
declare function dd_register(
  fetchHandler: (method: string, url: string, body: string) => unknown,
): void;
declare function dd_cache_match(key: string): any;
declare function dd_cache_put(key: string, response: unknown): void;
declare function dd_service_fetch(
  binding: string,
  method: string,
  url: string,
  body: string,
): any;
declare function dd_fetch(url: string, options: unknown): any;

dd_register((method: string, url: string, body: string) => {
  const parsed = new URL(url);
  const path = parsed.pathname;
  if (path === "/proxy") {
    const upstream = dd_fetch(body, { method: "GET" });
    return {
      status: upstream.status,
      headers: { "x-proxied": "yes" },
      body: upstream.body,
    };
  }
  if (path === "/auth") {
    const session = dd_service_fetch("auth", "GET", "http://auth.internal/session", "");
    return { status: session.status, headers: {}, body: session.body };
  }
  const cached = dd_cache_match(url);
  if (cached !== null) {
    return { status: 200, headers: { "x-cache": "hit" }, body: cached.body };
  }
  const response = {
    status: 200,
    headers: { "cache-control": "public, max-age=60" },
    body: "computed:" + path,
  };
  dd_cache_put(url, response);
  return { status: 200, headers: { "x-cache": "miss" }, body: "computed:" + path };
});
