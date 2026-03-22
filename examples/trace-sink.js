let ingested = 0;

export default {
  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (request.method === "POST" && path === "/ingest") {
      if (!request.headers.get("x-dd-internal")) {
        return new Response("missing x-dd-internal", { status: 403 });
      }
      ingested += 1;
      return new Response("ok");
    }

    if (request.method === "GET" && path === "/count") {
      return new Response(String(ingested));
    }

    return new Response("trace-sink");
  },
};
