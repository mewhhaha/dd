export default {
  async fetch(request, env, ctx) {
    const kv = env.MY_KV;
    if (!kv) {
      return new Response("missing KV binding: MY_KV", { status: 500 });
    }

    if (
      request.method === "GET"
      && new URL(request.url).pathname === "/"
      && !request.headers.get("x-request-id")
    ) {
      return Response.json({
        ok: true,
        worker: "wait-until-kv",
        routes: [
          "GET / with x-request-id header",
        ],
        note: "bg task writes done:{id} in waitUntil",
      });
    }

    const requestId = request.headers.get("x-request-id") ?? String(Date.now());
    ctx.waitUntil(
      (async () => {
        await Deno.core.ops.op_sleep(20);
        await kv.set(`done:${requestId}`, "1");
      })(),
    );

    return new Response(`queued:${requestId}`, {
      headers: { "content-type": "text/plain; charset=utf-8" },
    });
  },
};
