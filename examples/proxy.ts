// Outbound fetch: proxies the URL given in the request body (POST /).
// Native fetch returns a real promise; the handler returns the .then chain
// and the engine's event loop resolves it. /sync uses the synchronous
// dd_fetch variant instead.
declare function dd_register(
  fetchHandler: (method: string, url: string, body: string) => unknown,
): void;
declare function dd_fetch(url: string, options: unknown): any;

dd_register((method: string, url: string, body: string) => {
  if (method !== "POST" || body === "") {
    return { status: 400, headers: {}, body: "POST a URL to proxy" };
  }
  const parsed = new URL(url);
  if (parsed.pathname === "/sync") {
    const upstream = dd_fetch(body, { method: "GET" });
    return {
      status: upstream.status,
      headers: { "x-proxied": "sync" },
      body: upstream.body,
    };
  }
  return fetch(body)
    .then((response: any) => response.text())
    .then((text: any) => {
      return { status: 200, headers: { "x-proxied": "promise" }, body: text };
    });
});
