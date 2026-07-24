export default {
  async fetch(request) {
    const cache = await caches.open("pages-v1");
    const url = new URL(request.url);
    const key = new Request(`http://cache${url.pathname}`, { method: "GET" });

    const cached = await cache.match(key);
    if (cached) {
      return cached;
    }

    const response = new Response(`named cache miss: ${url.pathname || "/"}`, {
      headers: {
        "content-type": "text/plain; charset=utf-8",
        "cache-control": "public, max-age=120",
      },
    });
    await cache.put(key, response.clone());
    return response;
  },
};
