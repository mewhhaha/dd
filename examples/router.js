export default {
  async fetch(request) {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/health") {
      return new Response("ok", { status: 200 });
    }

    if (request.method === "POST" && url.pathname === "/echo") {
      const body = await request.text();
      return new Response(body, {
        status: 200,
        headers: [["content-type", "text/plain"]],
      });
    }

    return new Response("not found", { status: 404 });
  },
};
