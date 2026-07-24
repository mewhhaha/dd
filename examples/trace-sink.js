let ingested = 0;

function htmlPage() {
  return `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>trace sink</title>
    <style>
      body { margin: 0; font: 16px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace; background: linear-gradient(180deg, #101828, #0b1020); color: #e5e7eb; }
      main { max-width: 720px; margin: 0 auto; padding: 48px 20px; }
      .card { background: rgba(255,255,255,0.06); border: 1px solid rgba(255,255,255,0.08); border-radius: 16px; padding: 24px; box-shadow: 0 24px 80px rgba(0,0,0,0.35); }
      h1 { margin: 0 0 12px; font-size: 32px; }
      p { margin: 0 0 12px; color: #cbd5e1; }
      code { background: rgba(255,255,255,0.08); padding: 2px 6px; border-radius: 6px; }
      .count { margin-top: 20px; font-size: 56px; font-weight: 700; color: #7dd3fc; }
      .label { color: #94a3b8; text-transform: uppercase; letter-spacing: 0.12em; font-size: 12px; }
    </style>
  </head>
  <body>
    <main>
      <div class="card">
        <div class="label">trace sink</div>
        <h1>worker traces receiver</h1>
        <p>This worker accepts internal trace posts at <code>POST /ingest</code>.</p>
        <p>Healthy status only. It is intentionally boring.</p>
        <div class="count" id="count">0</div>
        <p>events ingested</p>
      </div>
    </main>
    <script>
      const count = document.getElementById('count');
      async function refresh() {
        try {
          const res = await fetch('/count');
          count.textContent = await res.text();
        } catch {
          count.textContent = 'offline';
        }
      }
      refresh();
      setInterval(refresh, 2000);
    </script>
  </body>
</html>`;
}

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

    if (request.method === "GET" && path === "/") {
      return new Response(htmlPage(), {
        headers: {
          "content-type": "text/html; charset=utf-8",
        },
      });
    }

    return new Response("not found", { status: 404 });
  },
};
