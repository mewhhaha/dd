// Each preview child runs in separate Deno isolate.
// Isolation is runtime-level, not OS/container sandboxing.
/// <reference path="./types/dynamic-worker-config.d.ts" />

function html(body) {
  return new Response(
    `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Dynamic Preview</title><style>body{font-family:ui-sans-serif,system-ui,sans-serif;margin:32px;line-height:1.45}code{background:#f4f4f4;padding:2px 6px;border-radius:6px}</style></head><body>${body}</body></html>`,
    { headers: { "content-type": "text/html; charset=utf-8" } },
  );
}

class PreviewControl extends RpcTarget {
  constructor(previewId) {
    super();
    this.previewId = previewId;
    this.hits = 0;
    this.createdAt = new Date().toISOString();
  }

  async metadata() {
    this.hits += 1;
    return {
      previewId: this.previewId,
      hits: this.hits,
      createdAt: this.createdAt,
    };
  }
}

function previewModules() {
  return {
    "worker.js": `
      export default {
        async fetch(request, env) {
          const url = new URL(request.url);
          const meta = await env.PREVIEW.metadata();
          if (url.pathname === "/" || url.pathname === "/index.html") {
            return new Response(
              [
                "<h1>Preview Environment</h1>",
                "<p>preview: <code>" + String(meta.previewId) + "</code></p>",
                "<p>hits: <code>" + String(meta.hits) + "</code></p>",
                "<p>createdAt: <code>" + String(meta.createdAt) + "</code></p>",
                "<p>Try <a href='/api/health'>/api/health</a></p>",
              ].join(""),
              { headers: { "content-type": "text/html; charset=utf-8" } },
            );
          }
          if (url.pathname === "/api/health") {
            return Response.json({
              ok: true,
              preview: meta.previewId,
              hits: meta.hits,
            });
          }
          return new Response("preview route not found", { status: 404 });
        },
      };
    `,
  };
}

async function ensurePreview(env, previewId) {
  const key = String(previewId || "").trim().toLowerCase();
  if (!key) {
    throw new Error("preview id is required");
  }
  return env.SANDBOX.get(`preview:${key}`, async () => {
    /** @type {import("./types/dynamic-worker-config").DynamicWorkerConfig} */
    const config = {
      entrypoint: "worker.js",
      modules: previewModules(),
      allow_host_rpc: true,
      env: {
        PREVIEW: new PreviewControl(key),
      },
      timeout: 3_000,
    };
    return config;
  });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return html(
        [
          "<h1>Dynamic Preview Manager</h1>",
          "<p>Open a preview at <code>/preview/{id}</code></p>",
          "<p>Example: <a href='/preview/pr-123'>/preview/pr-123</a></p>",
        ].join(""),
      );
    }

    if (!url.pathname.startsWith("/preview/")) {
      return new Response("not found", { status: 404 });
    }

    const rest = url.pathname.slice("/preview/".length);
    const slashIdx = rest.indexOf("/");
    const previewId = slashIdx === -1 ? rest : rest.slice(0, slashIdx);
    const tailPath = slashIdx === -1 ? "/" : rest.slice(slashIdx);
    if (!previewId) {
      return new Response("missing preview id", { status: 400 });
    }

    const preview = await ensurePreview(env, previewId);
    const target = new URL(`http://worker${tailPath}`);
    target.search = url.search;
    const proxied = await preview.fetch(target.toString(), {
      method: request.method,
      headers: request.headers,
    });
    return proxied;
  },
};
