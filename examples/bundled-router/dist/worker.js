function json(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

async function readText(request) {
  const body = await request.text();
  return body.length > 0 ? body : "";
}

const routes = {
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

function routeKey(request) {
  const url = new URL(request.url);
  return `${request.method.toUpperCase()} ${url.pathname}`;
}

async function handle(request) {
  const handler = routes[routeKey(request)];
  if (!handler) {
    return json({ ok: false, error: "route not found" }, 404);
  }
  return handler(request);
}

export default {
  fetch(request) {
    return handle(request);
  },
};
