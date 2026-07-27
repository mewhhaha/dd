import { renderToString } from "react-dom/server";
import { renderToReadableStream } from "react-dom/server.edge";

interface Env {
  GREETING?: string;
  TEST_KV?: {
    get(key: string): Promise<unknown | null>;
    put(key: string, value: unknown): Promise<void>;
  };
  TEST_MEMORY?: {
    idFromName(name: string): unknown;
    get(id: unknown): {
      atomic<T>(callback: () => T): Promise<T>;
      tvar<T>(key: string, defaultValue: T): {
        read(): T;
        write(value: T): void;
      };
    };
  };
}

function Greeting({ name }: { name: string }) {
  return (
    <main>
      <h1>Hello, {name}</h1>
      <p>Rendered by React inside QuickJS inside Wasm.</p>
    </main>
  );
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const name = url.searchParams.get("name") ?? "world";
    if (url.pathname === "/json") {
      return Response.json({
        greeting: env.GREETING ?? "hello",
        method: request.method,
        name,
        userAgent: request.headers.get("user-agent"),
      });
    }

    if (url.pathname === "/stream") {
      const stream = await renderToReadableStream(<Greeting name={name} />, {
        signal: request.signal,
      });
      return new Response(stream, {
        headers: { "content-type": "text/html; charset=utf-8" },
      });
    }

    if (url.pathname === "/crypto") {
      const bytes = crypto.getRandomValues(new Uint8Array(16));
      return Response.json({
        bytes: Array.from(bytes),
        uuid: crypto.randomUUID(),
      });
    }

    if (url.pathname === "/bindings") {
      if (!env.TEST_KV || !env.TEST_MEMORY) {
        return new Response("missing TEST_KV or TEST_MEMORY", { status: 500 });
      }
      const previous = await env.TEST_KV.get("message");
      await env.TEST_KV.put("message", name);
      const shard = env.TEST_MEMORY.get(env.TEST_MEMORY.idFromName("counter"));
      const count = await shard.atomic(() => {
        const counter = shard.tvar("value", 0);
        const next = counter.read() + 1;
        counter.write(next);
        return next;
      });
      return Response.json({ count, previous });
    }

    const markup = renderToString(<Greeting name={name} />);
    return new Response(`<!doctype html>${markup}`, {
      headers: { "content-type": "text/html; charset=utf-8" },
    });
  },
};
