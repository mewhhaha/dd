// Outbound fetch: proxies the URL given in the request body (POST /).
declare function dd_register(
  fetchHandler: (method: string, url: string, body: string) => unknown,
): void;
declare function dd_fetch(url: string, options: unknown): any;

dd_register((method: string, url: string, body: string) => {
  if (method !== "POST" || body === "") {
    return { status: 400, headers: {}, body: "POST a URL to proxy" };
  }
  const upstream = dd_fetch(body, { method: "GET" });
  return {
    status: upstream.status,
    headers: { "x-proxied": "yes" },
    body: upstream.body,
  };
});
