/// <reference path="./types/dynamic-worker-config.d.ts" />

function json(data, init = {}) {
  return new Response(JSON.stringify(data, null, 2), {
    status: init.status ?? 200,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...(init.headers || {}),
    },
  });
}

function parsePromptFromRequest(request, url) {
  const fromQuery = url.searchParams.get("prompt");
  if (fromQuery) {
    return fromQuery;
  }
  if (request.method === "POST") {
    return request.text();
  }
  return "";
}

function pretendLlmPlan(prompt) {
  const text = String(prompt || "").toLowerCase();
  if (text.includes("sum")) {
    return {
      mode: "sum",
      modules: {
        "worker.js": `
          export default {
            async fetch(request) {
              const url = new URL(request.url);
              const values = url.searchParams.getAll("n").map((value) => Number(value));
              const total = values.reduce((acc, value) => acc + (Number.isFinite(value) ? value : 0), 0);
              return Response.json({ mode: "sum", values, total });
            },
          };
        `,
      },
    };
  }

  return {
    mode: "echo",
    modules: {
      "worker.js": `
        export default {
          async fetch(request, env) {
            const url = new URL(request.url);
            const input = url.searchParams.get("input") || "";
            const audit = await env.RUNTIME_AUDIT.recordRun({
              mode: "echo",
              inputLength: input.length,
            });
            return Response.json({
              mode: "echo",
              output: input.toUpperCase(),
              auditCount: Number(audit?.count ?? 0),
            });
          },
        };
      `,
    },
  };
}

class RuntimeAudit extends RpcTarget {
  constructor() {
    super();
    this.count = 0;
  }

  async recordRun(event) {
    this.count += 1;
    return {
      count: this.count,
      event,
      at: new Date().toISOString(),
    };
  }
}

async function ensureSandbox(env, plan) {
  const key = `plan:${plan.mode}`;
  const child = await env.SANDBOX.get(key, async () => {
    /** @type {import("./types/dynamic-worker-config").DynamicWorkerConfig} */
    const config = {
      entrypoint: "worker.js",
      modules: plan.modules,
      env: {
        RUNTIME_AUDIT: new RuntimeAudit(),
      },
      timeout: 2_500,
    };
    return config;
  });
  return child;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return new Response(
        [
          "pretend-llm dynamic executor",
          "GET /run?prompt=echo&input=hello",
          "GET /run?prompt=sum&n=1&n=2&n=3",
          "POST /run (raw body as prompt)",
        ].join("\n"),
        { headers: { "content-type": "text/plain; charset=utf-8" } },
      );
    }

    if (url.pathname !== "/run") {
      return new Response("not found", { status: 404 });
    }

    const prompt = (await parsePromptFromRequest(request, url)).trim();
    if (!prompt) {
      return json({ error: "missing prompt" }, { status: 400 });
    }

    const plan = pretendLlmPlan(prompt);
    const sandbox = await ensureSandbox(env, plan);
    const childUrl = new URL("http://worker/");
    if (plan.mode === "sum") {
      for (const value of url.searchParams.getAll("n")) {
        childUrl.searchParams.append("n", value);
      }
      if (!url.searchParams.getAll("n").length) {
        childUrl.searchParams.append("n", "1");
        childUrl.searchParams.append("n", "2");
        childUrl.searchParams.append("n", "3");
      }
    } else {
      childUrl.searchParams.set("input", url.searchParams.get("input") || "hello");
    }

    const result = await sandbox.fetch(childUrl.toString(), { method: "GET" });
    return new Response(await result.text(), {
      status: result.status,
      headers: { "content-type": "application/json; charset=utf-8" },
    });
  },
};
