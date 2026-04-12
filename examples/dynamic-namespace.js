/// <reference path="./types/dynamic-worker-config.d.ts" />

const CHILD_ENTRY = `
import { nextCount, previewToken } from "./lib.js";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const host = await env.API.record(url.pathname);
    return Response.json({
      ok: true,
      counter: nextCount(),
      path: url.pathname,
      tokenPlaceholderPreview: previewToken(env.API_TOKEN),
      hostCount: Number(host?.count ?? 0),
      hostLastPath: String(host?.path ?? ""),
    });
  },
};
`;

const CHILD_LIB = `
let counter = 0;

export function nextCount() {
  counter += 1;
  return counter;
}

export function previewToken(value) {
  return String(value || "").slice(0, 4) + "...";
}
`;

class DemoApi extends RpcTarget {
  constructor() {
    super();
    this.count = 0;
  }

  async record(path) {
    this.count += 1;
    return {
      count: this.count,
      path: String(path || ""),
    };
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return new Response(
        "dynamic namespace demo: GET /run?path=/hello",
        { headers: { "content-type": "text/plain; charset=utf-8" } },
      );
    }

    if (url.pathname !== "/run") {
      return new Response("not found", { status: 404 });
    }

    const child = await env.SANDBOX.get("demo:v1", async () => {
      /** @type {import("./types/dynamic-worker-config").DynamicWorkerConfig} */
      const config = {
        entrypoint: "worker.js",
        modules: {
          "worker.js": CHILD_ENTRY,
          "./lib.js": CHILD_LIB,
        },
        env: {
          API_TOKEN: "placeholder-parent-token",
          API: new DemoApi(),
        },
        timeout: 2_000,
      };
      return config;
    });
    const childPath = url.searchParams.get("path") || "/hello";
    const response = await child.fetch(`http://worker${childPath}`);
    const body = await response.text();
    return new Response(body, {
      status: response.status,
      headers: { "content-type": "application/json; charset=utf-8" },
    });
  },
};
