const parsePath = (url) => {
  const { pathname } = new URL(url);
  return pathname || "/";
};

export default {
  async fetch(request, env) {
    const kv = env.MY_KV;
    if (!kv) {
      return new Response("missing KV binding: MY_KV", { status: 500 });
    }

    const path = parsePath(request.url);
    const current = Number((await kv.get("counter")) ?? "0") || 0;

    if (path === "/value" && request.method === "GET") {
      return new Response(String(current), {
        headers: { "content-type": "text/plain; charset=utf-8" },
      });
    }

    if (path === "/inc" && request.method === "POST") {
      const next = current + 1;
      await kv.set("counter", String(next));
      return new Response(String(next), {
        headers: { "content-type": "text/plain; charset=utf-8" },
      });
    }

    if (path === "/reset" && request.method === "POST") {
      await kv.delete("counter");
      return new Response("ok", {
        headers: { "content-type": "text/plain; charset=utf-8" },
      });
    }

    return new Response("not found", { status: 404 });
  },
};
