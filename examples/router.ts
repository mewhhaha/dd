// Path routing plus a service binding: /auth consults the worker deployed
// under the AUTH binding (deploy with: --service AUTH=auth).
declare function dd_register(
  fetchHandler: (method: string, url: string, body: string) => unknown,
): void;
declare function dd_json(value: unknown): string;
declare function dd_service_fetch(
  binding: string,
  method: string,
  url: string,
  body: string,
): any;

dd_register((method: string, url: string, body: string) => {
  const parsed = new URL(url);
  const path = parsed.pathname;
  if (path === "/") {
    return { status: 200, headers: {}, body: "routes: /, /echo, /auth" };
  }
  if (path === "/echo") {
    return {
      status: 200,
      headers: { "content-type": "application/json" },
      body: dd_json({ method: method, path: path, echo: body }),
    };
  }
  if (path === "/auth") {
    const session = dd_service_fetch("AUTH", "GET", "http://auth.internal/session", "");
    return { status: session.status, headers: {}, body: session.body };
  }
  return { status: 404, headers: {}, body: "not found: " + path };
});
