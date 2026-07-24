type Handler = (request: Request) => Promise<Response> | Response;

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

async function readText(request: Request): Promise<string> {
  const body = await request.text();
  return body.length > 0 ? body : "";
}

const routes: Record<string, Handler> = {
  "GET /": () =>
    json({
      ok: true,
      service: "bundled-router",
      message: "hello from bundled TypeScript worker",
    }),
  "GET /health": () => json({ ok: true }),
  "POST /echo": async (request) =>
    json({
      ok: true,
      echoed: await readText(request),
    }),
};

function routeKey(request: Request): string {
  const url = new URL(request.url);
  return `${request.method.toUpperCase()} ${url.pathname}`;
}

async function handle(request: Request): Promise<Response> {
  const handler = routes[routeKey(request)];
  if (!handler) {
    return json({ ok: false, error: "route not found" }, 404);
  }
  return handler(request);
}

export default {
  fetch(request: Request): Promise<Response> {
    return handle(request);
  },
};
