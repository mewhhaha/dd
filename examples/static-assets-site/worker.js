function json(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

export default {
  async fetch(request) {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/") {
      return Response.redirect(new URL("/index.html", url), 302);
    }

    if (request.method === "GET" && url.pathname === "/api/time") {
      return json({
        ok: true,
        now: new Date().toISOString(),
        note: "Static files come from --assets-dir. This JSON comes from worker code.",
      });
    }

    return json({ ok: false, error: "not found" }, 404);
  },
};
