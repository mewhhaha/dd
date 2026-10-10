export default {
  async fetch(_request, _env, ctx) {
    ctx.waitUntil(
      (async () => {
        await new Promise((resolve) => setTimeout(resolve, 50));
        console.log("background work done");
      })(),
    );

    return new Response("response sent first", {
      headers: {
        "content-type": "text/plain; charset=utf-8",
      },
    });
  },
};
