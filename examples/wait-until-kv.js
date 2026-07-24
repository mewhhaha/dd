export default {
  async fetch(request, env, ctx) {
    const kv = env.MY_KV;
    if (!kv) {
      return new Response("missing KV binding: MY_KV", { status: 500 });
    }
    const url = new URL(request.url);

    if (
      request.method === "GET"
      && url.pathname === "/"
    ) {
      return Response.json({
        ok: true,
        worker: "wait-until-kv",
        routes: [
          "GET /queue with x-request-id header",
        ],
        note: "bg task writes done:{id} in waitUntil",
      }, {
        headers: { "cache-control": "no-store" },
      });
    }

    if (url.pathname !== "/queue") {
      return new Response("not found", { status: 404 });
    }

    const requestId = request.headers.get("x-request-id") ?? String(Date.now());
    ctx.waitUntil(
      (async () => {
        await Deno.core.ops.op_sleep(20);
        await kv.put(`done:${requestId}`, "1");
      })(),
    );

    return new Response(`queued:${requestId}`, {
      headers: {
        "cache-control": "no-store",
        "content-type": "text/plain; charset=utf-8",
      },
    });
  },
};
