export default {
  async fetch(_request, env) {
    const current = Number((await env.MY_KV?.get("hits")) ?? "0") || 0;
    const next = current + 1;
    await env.MY_KV?.set("hits", String(next));

    return new Response(`hits=${next}`, {
      headers: {
        "content-type": "text/plain; charset=utf-8",
      },
    });
  },
};
