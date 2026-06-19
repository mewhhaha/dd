import { Router, type JSX, type route } from "@mewhhaha/ruwuter";

type MemoryNamespace = {
  idFromName(name: string): unknown;
  get(id: unknown): MemoryShard;
};

type MemoryShard = {
  atomic<T>(callback: () => T): Promise<T>;
  tvar<T>(key: string, defaultValue: T): {
    read(): T;
    write(value: T): void;
  };
};

type Env = {
  EXAMPLE_MEMORY: MemoryNamespace;
};

const styles = `
  :root {
    color: #172022;
    background: #f5f7f5;
    font-family:
      Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI",
      sans-serif;
  }

  body {
    margin: 0;
  }

  main {
    display: grid;
    gap: 24px;
    margin: 0 auto;
    max-width: 880px;
    padding: 48px 24px;
  }

  h1,
  h2,
  p {
    margin: 0;
  }

  h1 {
    font-size: clamp(2rem, 8vw, 4.25rem);
    line-height: 0.95;
  }

  h2 {
    font-size: 1rem;
    letter-spacing: 0;
  }

  a {
    color: #176b5a;
    font-weight: 700;
  }

  .shell {
    display: grid;
    gap: 18px;
    min-height: 100vh;
  }

  .nav {
    align-items: center;
    border-bottom: 1px solid #dbe2dc;
    display: flex;
    gap: 16px;
    justify-content: space-between;
    padding: 18px 24px;
  }

  .nav strong {
    font-size: 0.85rem;
    text-transform: uppercase;
  }

  .nav div {
    display: flex;
    flex-wrap: wrap;
    gap: 14px;
  }

  .hero {
    display: grid;
    gap: 18px;
  }

  .stack {
    display: grid;
    gap: 24px;
  }

  .summary {
    color: #4f5b57;
    font-size: 1.08rem;
    line-height: 1.6;
    max-width: 680px;
  }

  .grid {
    display: grid;
    gap: 12px;
    grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
  }

  .tile {
    border: 1px solid #d7ded8;
    border-radius: 8px;
    display: grid;
    gap: 10px;
    padding: 18px;
  }

  .eyebrow {
    color: #66716d;
    font-size: 0.8rem;
    font-weight: 800;
    text-transform: uppercase;
  }
`;

type HomeData = {
  path: string;
  cards: Array<{ title: string; body: string }>;
};

type NoteData = {
  slug: string;
  path: string;
};

function Document({ children }: { children?: JSX.Element }) {
  return (
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>vite-ruwuter</title>
        <style>{styles}</style>
      </head>
      <body>
        <div class="shell">
          <nav class="nav" aria-label="Example">
            <strong>vite-ruwuter</strong>
            <div>
              <a href="/">Home</a>
              <a href="/notes/runtime">Runtime note</a>
            </div>
          </nav>
          <main>{children}</main>
        </div>
        <script type="module" src="/src/client.ts"></script>
      </body>
    </html>
  );
}

const layout = {
  default: Document,
  headers: () => ({
    "Cache-Control": "no-store",
    "Content-Type": "text/html; charset=utf-8",
  }),
};

const home = {
  loader: ({ request }: { request: Request }): HomeData => {
    const url = new URL(request.url);
    return {
      path: url.pathname,
      cards: [
        {
          title: "Worker-first routing",
          body: "The Vite dev server forwards application requests into the dd runtime.",
        },
        {
          title: "Vite stays native",
          body: "Module, client, HMR, and asset requests are served by Vite itself.",
        },
        {
          title: "Ruwuter JSX",
          body: "The worker response is rendered with Ruwuter's zero-dependency JSX runtime.",
        },
      ],
    };
  },
  default: ({ loaderData }: { loaderData: HomeData }) => (
    <div class="stack">
      <section class="hero" data-path={loaderData.path}>
        <p class="eyebrow">direct runtime dev</p>
        <p>
          <span class="dd-stm-badge">dd STM counter</span>
        </p>
        <h1>Ruwuter on dd</h1>
        <p class="summary">
          A tiny Vite project that routes every app request through a local dd worker while
          preserving Vite's development pipeline.
        </p>
      </section>
      <section class="grid" aria-label="Highlights">
        {loaderData.cards.map((card) => (
          <article class="tile">
            <h2>{card.title}</h2>
            <p>{card.body}</p>
          </article>
        ))}
      </section>
    </div>
  ),
};

const note = {
  loader: ({
    params,
    request,
  }: {
    params: { slug: string };
    request: Request;
  }): NoteData => {
    const url = new URL(request.url);
    return { slug: params.slug, path: url.pathname };
  },
  default: ({ loaderData }: { loaderData: NoteData }) => (
    <section class="hero" data-path={loaderData.path}>
      <p class="eyebrow">route params</p>
      <p>
        <span class="dd-stm-badge">dd STM counter</span>
      </p>
      <h1>{loaderData.slug}</h1>
      <p class="summary">
        This page came from a parameterized Ruwuter route running inside the dd runtime.
      </p>
    </section>
  ),
};

const routes: route[] = [
  [new URLPattern({ pathname: "/" }), [{ id: "layout", mod: layout }, { id: "home", mod: home }]],
  [
    new URLPattern({ pathname: "/notes/:slug" }),
    [{ id: "layout", mod: layout }, { id: "note", mod: note }],
  ],
];

const router = Router(routes);

async function incrementStmRequestCount(env: Env): Promise<number> {
  const memory = env.EXAMPLE_MEMORY.get(env.EXAMPLE_MEMORY.idFromName("vite-ruwuter"));
  const requests = memory.tvar("requests", 0);
  return memory.atomic(() => {
    const next = Number(requests.read()) + 1;
    requests.write(next);
    return next;
  });
}

function withStmHeader(response: Response, count: number): Response {
  const headers = new Headers(response.headers);
  headers.set("x-dd-stm-count", String(count));
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

export default {
  async fetch(request: Request, env: Env, context: unknown): Promise<Response> {
    const count = await incrementStmRequestCount(env);
    const response = await router.handle(request, env as never, context as never);
    return withStmHeader(response, count);
  },
};
