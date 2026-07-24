export default {
  async fetch(request) {
    const cache = globalThis.caches?.default;
    const url = new URL(request.url);
    const key = new Request(url.toString(), { method: "GET" });

    if (cache) {
      const cached = await cache.match(key);
      if (cached) {
        return cached;
      }
    }

    const response = new Response(`fresh: ${url.pathname || "/"}`, {
      headers: {
        "content-type": "text/plain; charset=utf-8",
        "cache-control": "public, max-age=60",
      },
    });

    if (cache) {
      await cache.put(key, response.clone());
    }

    return response;
  },
};
