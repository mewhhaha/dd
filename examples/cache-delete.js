export default {
  async fetch(request) {
    const cache = caches.default;
    const key = new Request(new URL(request.url).toString(), { method: "GET" });

    const response = new Response("temp", {
      headers: {
        "cache-control": "public, max-age=60",
      },
    });

    await cache.put(key, response);
    const deleted = await cache.delete(key);

    return new Response(deleted ? "deleted=true" : "deleted=false", {
      headers: {
        "content-type": "text/plain; charset=utf-8",
      },
    });
  },
};
