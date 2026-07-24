export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (request.method !== "GET" || url.pathname !== "/greet") {
      return new Response("not found", { status: 404 });
    }

    const lang = request.headers.get("accept-language")?.split(",")[0]?.trim() || "en";
    const cache = caches.default;
    const key = new Request(url.toString(), {
      method: "GET",
      headers: [["accept-language", lang]],
    });

    const cached = await cache.match(key);
    if (cached) {
      return cached;
    }

    const body = lang.startsWith("fr") ? "salut" : lang.startsWith("es") ? "hola" : "hello";
    const response = new Response(body, {
      headers: {
        "content-type": "text/plain; charset=utf-8",
        "cache-control": "public, max-age=60",
        vary: "accept-language",
      },
    });

    await cache.put(key, response.clone());
    return response;
  },
};
