const MAX_TRACES = 500;
const traces = [];

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
    },
  });
}

function keepLatest(list, max) {
  if (list.length <= max) {
    return list;
  }
  return list.slice(list.length - max);
}

export default {
  async fetch(request) {
    const url = new URL(request.url);

    if (request.method === "POST" && url.pathname === "/ingest") {
      const internal = request.headers.get("x-dd-internal");
      if (!internal) {
        return new Response("missing x-dd-internal", { status: 403 });
      }

      let payload;
      try {
        payload = await request.json();
      } catch {
        return json({ ok: false, error: "invalid JSON body" }, 400);
      }

      traces.push({
        received_at_ms: Date.now(),
        event: payload,
      });
      const compact = keepLatest(traces, MAX_TRACES);
      traces.length = 0;
      traces.push(...compact);

      return json({ ok: true, count: traces.length }, 201);
    }

    if (request.method === "GET" && url.pathname === "/traces") {
      return json({ count: traces.length, traces });
    }

    if (request.method === "GET" && url.pathname === "/") {
      return json({
        ok: true,
        routes: ["POST /ingest", "GET /traces"],
        count: traces.length,
      });
    }

    return new Response("not found", { status: 404 });
  },
};
