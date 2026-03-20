export default {
  async fetch(request, env, ctx) {
    const kv = env.MY_KV;
    if (!kv) {
      return new Response("missing KV binding: MY_KV", { status: 500 });
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
