let appVersion = 0;

function htmlPage() {
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>dd swr demo</title>
    <style>
      body {
        margin: 0;
        padding: 32px;
        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
        background: #f6f7fb;
        color: #111827;
      }
      .card {
        max-width: 720px;
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);
      }
      code {
        background: #f3f4f6;
        border-radius: 6px;
        padding: 2px 6px;
      }
    </style>
  </head>
  <body>
    <div class="card">
      <h1>dd SWR demo</h1>
      <p>This page loads <code>/app.js</code> with <code>stale-while-revalidate</code>.</p>
      <p>Reload after a few seconds and watch the rendered version update.</p>
      <p id="status">Loading…</p>
    </div>
    <script>
      (() => {
        const base = window.location.pathname.endsWith("/")
          ? window.location.pathname
          : window.location.pathname + "/";
        const script = document.createElement("script");
        script.src = base + "app.js";
        document.body.appendChild(script);
      })();
    </script>
  </body>
</html>`;
}

function appJs(version, generatedAt) {
  return `(() => {
  const el = document.getElementById("status");
  if (el) {
    el.textContent = "app.js version: ${version} (generated ${generatedAt})";
  }
})();`;
}

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname || "/";

    if (path === "/") {
      return new Response(htmlPage(), {
        headers: {
          "content-type": "text/html; charset=utf-8",
          "cache-control": "no-store",
        },
      });
    }

    if (path === "/app.js") {
      appVersion += 1;
      const generatedAt = new Date(Date.now()).toISOString();
      return new Response(appJs(appVersion, generatedAt), {
        headers: {
          "content-type": "application/javascript; charset=utf-8",
          "cache-control": "public, max-age=2, stale-while-revalidate=30, stale-if-error=120",
          "x-origin-app-version": String(appVersion),
        },
      });
    }

    return new Response("not found", { status: 404 });
  },
};
