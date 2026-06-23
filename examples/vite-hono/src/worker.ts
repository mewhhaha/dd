import { Hono, type Context } from "hono";

type KvNamespace = {
  get(key: string): Promise<unknown | null>;
  put(key: string, value: unknown): void | Promise<void>;
  delete(key: string): void | Promise<void>;
};

type MemoryNamespace = {
  idFromName(name: string): unknown;
  get(id: unknown): MemoryShard;
};

type MemoryShard = {
  atomic<T>(callback: () => T): Promise<T>;
  apply(effects: MemoryEffect[]): Promise<void>;
  accept(request: Request): { handle: string; response: Response };
  tvar<T>(key: string, defaultValue: T): {
    read(): T;
    write(value: T): void;
  };
};

type MemoryEffect = {
  type: "socket.send";
  handle: string;
  payload: string;
  kind?: "text";
};

type Env = {
  EXAMPLE_MEMORY: MemoryNamespace;
  STORE_DB: KvNamespace;
};

type RuntimeContext = {
  sessionCookie: string | null;
  sessionId: string;
  stmCount: number;
  timerTick: string;
};

type Product = {
  slug: string;
  name: string;
  category: string;
  badge: string;
  description: string;
  story: string;
  priceCents: number;
  stock: number;
  imageUrl: string;
  specs: string[];
};

type CartLine = {
  slug: string;
  quantity: number;
};

type CartState = {
  lines: CartLine[];
  lastOrderId: string | null;
};

type CartLineView = CartLine & {
  product: Product;
  lineTotal: string;
  lineTotalCents: number;
};

type CartView = {
  lines: CartLineView[];
  itemCount: number;
  subtotal: string;
  subtotalCents: number;
  lastOrderId: string | null;
};

type StorefrontOrder = {
  id: string;
  createdAt: string;
  customerEmail: string;
  totalCents: number;
  lines: Array<{
    slug: string;
    name: string;
    quantity: number;
    priceCents: number;
  }>;
};

type StorefrontData = {
  cart: CartView;
  databaseStats: Array<{ label: string; value: string }>;
  path: string;
  products: Array<Product & { price: string }>;
  recentOrders: StorefrontOrder[];
  runtime: RuntimeContext;
};

type ProductData = StorefrontData & {
  product: Product & { price: string };
  relatedProducts: Array<Product & { price: string }>;
};

type HonoEnv = {
  Bindings: Env;
  Variables: {
    runtime: RuntimeContext;
  };
};

type StorefrontSocketWakeEvent = {
  type?: string;
  stub?: MemoryShard;
  handle?: string;
  data?: unknown;
};

const SESSION_COOKIE = "dd_storefront_session";
const CATALOG_VERSION = "2026-06-storefront-1";
const CATALOG_VERSION_KEY = "storefront:catalog-version";
const PRODUCT_SLUGS_KEY = "storefront:product-slugs";
const ORDERS_INDEX_KEY = "storefront:orders";
const LIVE_SOCKET_HANDLES_KEY = "storefront:live-handles";
const LIVE_SOCKET_EVENTS_KEY = "storefront:live-events";
const CART_SOCKET_HANDLES_KEY = "storefront:cart-handles";
const CART_SOCKET_EVENTS_KEY = "storefront:cart-events";
const EMPTY_CART: CartState = { lines: [], lastOrderId: null };
const WORKER_NAME = "vite-hono";

const PRODUCTS: Product[] = [
  {
    slug: "runtime",
    name: "Runtime valley print",
    category: "Limited print",
    badge: "Best seller",
    description: "A museum-grade valley print for calm desks and long deploy windows.",
    story:
      "Printed on heavy matte stock with a muted alpine palette, then packed flat from the edge warehouse.",
    priceCents: 6400,
    stock: 18,
    imageUrl: "https://assets.ui.sh/wallpapers/landscapes.webp?variant=valley",
    specs: ["16 x 20 in", "Matte archival paper", "Ships in 2 days"],
  },
  {
    slug: "cache-coast",
    name: "Cache coast print",
    category: "Limited print",
    badge: "New run",
    description: "A cool coastal study with soft water, low contrast, and room for focus.",
    story:
      "Each print is tracked through KV inventory and packed with a signed batch card.",
    priceCents: 5800,
    stock: 24,
    imageUrl: "https://assets.ui.sh/wallpapers/landscapes.webp?variant=coast",
    specs: ["12 x 18 in", "Soft gloss finish", "Signed batch card"],
  },
  {
    slug: "trace-meadow",
    name: "Trace meadow print",
    category: "Studio proof",
    badge: "Low stock",
    description: "A lighter meadow proof for bright rooms, studio shelves, and small walls.",
    story:
      "Proof inventory is deliberately small so every checkout records an order row in the local database.",
    priceCents: 4200,
    stock: 7,
    imageUrl: "https://assets.ui.sh/wallpapers/landscapes.webp?variant=meadow",
    specs: ["11 x 14 in", "Numbered proof", "Recycled mailer"],
  },
];

const styles = `
  :root {
    background: #fafafa;
    color: #171717;
    font-family: InterVariable, Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    font-feature-settings: "cv02", "cv03", "cv04", "cv11", "ss01", "ss03";
  }

  * { box-sizing: border-box; }
  body { margin: 0; min-width: 320px; }
  a { color: inherit; text-decoration: none; }
  button, input { font: inherit; }
  img { max-width: 100%; }
  h1, h2, h3, p, dl, dd, ul { margin: 0; }

  .app {
    background: #fafafa;
    display: flex;
    isolation: isolate;
    flex-direction: column;
    min-height: 100dvh;
  }

  .header, .footer {
    background: #ffffff;
    box-shadow: 0 1px 0 rgba(23, 23, 23, 0.1);
  }

  .header-inner, .footer-inner, .main {
    margin: 0 auto;
    max-width: 72rem;
    width: 100%;
  }

  .header-inner {
    align-items: center;
    display: flex;
    gap: 1rem;
    padding: 1rem;
  }

  .brand, .header-actions {
    align-items: center;
    display: flex;
    flex: 1;
  }

  .brand a {
    font-size: 1rem;
    font-weight: 600;
    line-height: 1.5rem;
  }

  .desktop-nav { display: none; }
  .header-actions { gap: 0.75rem; justify-content: flex-end; }
  .mobile-menu { position: relative; }

  .mobile-menu summary {
    border-radius: 6px;
    box-shadow: 0 0 0 1px rgba(23, 23, 23, 0.1);
    color: #404040;
    cursor: pointer;
    font-size: 1rem;
    font-weight: 500;
    line-height: 1.5rem;
    list-style: none;
    padding: 0.5rem 0.75rem;
  }

  .mobile-menu summary::-webkit-details-marker { display: none; }

  .mobile-panel {
    background: #ffffff;
    border-radius: 6px;
    box-shadow: 0 16px 32px rgba(23, 23, 23, 0.12), 0 0 0 1px rgba(23, 23, 23, 0.1);
    display: grid;
    font-size: 1rem;
    gap: 0.25rem;
    line-height: 1.75rem;
    margin-top: 0.5rem;
    padding: 0.5rem;
    position: absolute;
    right: 0;
    width: 12rem;
    z-index: 10;
  }

  .mobile-panel a {
    border-radius: 4px;
    color: #404040;
    padding: 0.5rem 0.75rem;
  }

  .mobile-panel a:hover, .mobile-menu summary:hover, .secondary-button:hover,
  .nav-link:hover, .related-card:hover, .product-card h3 a:hover {
    background: rgba(23, 23, 23, 0.05);
  }

  .main {
    flex: 1;
    padding: 2rem 1rem;
  }

  .footer-inner {
    color: #525252;
    display: flex;
    flex-direction: column;
    font-size: 1rem;
    gap: 0.75rem;
    line-height: 1.75rem;
    padding: 1.5rem 1rem;
  }

  .footer-inner a { color: #171717; font-weight: 400; }
  .footer-inner [data-storefront-live-status] { color: #171717; font-weight: 500; }
  .page-grid, .content-stack, .section, .heading-group, .catalog,
  .cart-panel, .product-card, .product-body, .detail-stack, .related-card {
    display: grid;
  }

  .page-grid, .content-stack, .catalog, .detail-stack { gap: 2rem; }
  .section, .heading-group { gap: 1rem; }

  .eyebrow {
    color: #047857;
    font-size: 1rem;
    font-weight: 500;
    line-height: 1.75rem;
  }

  h1 {
    color: #171717;
    font-size: 2.25rem;
    font-weight: 600;
    letter-spacing: 0;
    max-width: 20ch;
    text-wrap: balance;
  }

  h2 {
    color: #171717;
    font-size: 1.5rem;
    font-weight: 600;
    letter-spacing: 0;
    text-wrap: balance;
  }

  h3 {
    color: #171717;
    font-size: 1.125rem;
    font-weight: 600;
    letter-spacing: 0;
    text-wrap: balance;
  }

  .summary, .muted, dd {
    color: #525252;
    font-size: 1rem;
    line-height: 1.75rem;
    text-wrap: pretty;
  }

  .summary { max-width: 56ch; }
  .label, dt {
    color: #171717;
    font-size: 1rem;
    font-weight: 500;
    line-height: 1.75rem;
  }

  .tabular { font-variant-numeric: tabular-nums; }

  .stats {
    border-bottom: 1px solid rgba(23, 23, 23, 0.1);
    border-top: 1px solid rgba(23, 23, 23, 0.1);
    display: grid;
    gap: 1rem;
    padding: 1rem 0;
  }

  .stats div, .cart-line { display: grid; gap: 0.25rem; }
  .catalog-header, .product-meta, .cart-heading, .price-row, .line-row {
    align-items: center;
    display: flex;
    gap: 0.75rem;
    justify-content: space-between;
  }

  .product-grid, .related-grid, .detail-grid, .detail-info { display: grid; gap: 1rem; }
  .detail-grid, .detail-info { gap: 2rem; }

  .product-card, .cart-panel, .image-frame, .related-card {
    background: #ffffff;
    border-radius: 8px;
    box-shadow: 0 1px 2px rgba(23, 23, 23, 0.05), 0 0 0 1px rgba(23, 23, 23, 0.1);
  }

  .product-card { overflow: hidden; }
  .product-image-link {
    display: block;
    overflow: hidden;
  }

  .product-card img, .image-frame img {
    aspect-ratio: 4 / 3;
    display: block;
    object-fit: cover;
    outline: 1px solid rgba(0, 0, 0, 0.05);
    outline-offset: -1px;
    width: 100%;
  }

  .image-frame { min-width: 0; overflow: hidden; }
  .image-frame img { aspect-ratio: 5 / 4; }
  .product-body, .cart-panel, .related-card { gap: 1rem; padding: 1rem; }

  .badge {
    background: #fef3c7;
    border-radius: 6px;
    color: #78350f;
    font-size: 0.875rem;
    font-weight: 500;
    line-height: 1.25rem;
    padding: 0.25rem 0.5rem;
  }

  .secondary-button, .primary-button {
    align-items: center;
    border: 0;
    border-radius: 6px;
    cursor: pointer;
    display: inline-flex;
    font-size: 0.875rem;
    font-weight: 500;
    justify-content: center;
    line-height: 1.5rem;
    padding: 0.5rem 0.75rem;
  }

  .secondary-button {
    background: #ffffff;
    box-shadow: 0 0 0 1px rgba(23, 23, 23, 0.1);
    color: #404040;
  }

  .primary-button {
    background: #047857;
    box-shadow: 0 0 0 1px #047857;
    color: #ffffff;
  }

  .primary-button:hover { background: #065f46; }
  .primary-button:disabled, .secondary-button:disabled {
    background: #d4d4d4;
    box-shadow: 0 0 0 1px #d4d4d4;
    color: #525252;
    cursor: not-allowed;
  }

  .cart-lines {
    border-bottom: 1px solid rgba(23, 23, 23, 0.1);
    border-top: 1px solid rgba(23, 23, 23, 0.1);
    display: grid;
  }

  .cart-line { padding: 0.75rem 0; }
  .cart-line + .cart-line { border-top: 1px solid rgba(23, 23, 23, 0.1); }

  .success {
    background: #ecfdf5;
    border-radius: 6px;
    color: #065f46;
    font-size: 1rem;
    line-height: 1.75rem;
    padding: 0.75rem;
  }

  .checkout-form, .add-form, .quantity-form { display: grid; gap: 0.75rem; }

  input {
    background: #ffffff;
    border: 0;
    border-radius: 6px;
    box-shadow: 0 0 0 1px rgba(23, 23, 23, 0.1);
    color: #171717;
    font-size: 1rem;
    line-height: 1.75rem;
    padding: 0.625rem 0.75rem;
  }

  input:focus { outline: 2px solid #047857; outline-offset: -1px; }

  .facts {
    border-bottom: 1px solid rgba(23, 23, 23, 0.1);
    border-top: 1px solid rgba(23, 23, 23, 0.1);
    display: grid;
    gap: 0.75rem;
    padding: 1rem 0;
  }

  .spec-list {
    border-top: 1px solid rgba(23, 23, 23, 0.1);
    display: grid;
    gap: 0.75rem;
    list-style: none;
    padding: 1rem 0 0;
  }

  @media (min-width: 640px) {
    .header-inner, .footer-inner, .main {
      padding-left: 1.5rem;
      padding-right: 1.5rem;
    }

    .brand a, .eyebrow, dt, dd, .label, .muted, .summary, .success {
      font-size: 0.875rem;
      line-height: 1.5rem;
    }

    input {
      font-size: 0.875rem;
      line-height: 1.5rem;
      padding-bottom: 0.5rem;
      padding-top: 0.5rem;
    }

    h1 { font-size: 3rem; }
    .related-grid, .product-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    .stats { grid-template-columns: repeat(4, minmax(0, 1fr)); }
    .quantity-form { grid-template-columns: 7rem minmax(0, 1fr); }
    .quantity-form .primary-button { align-self: end; }
  }

  @media (min-width: 1024px) {
    .header-inner, .footer-inner, .main {
      padding-left: 2rem;
      padding-right: 2rem;
    }

    .desktop-nav {
      align-items: center;
      color: #525252;
      display: flex;
      font-size: 0.875rem;
      gap: 1.75rem;
      line-height: 1.5rem;
    }

    .mobile-menu { display: none; }
    .footer-inner {
      align-items: center;
      flex-direction: row;
      justify-content: space-between;
    }

    .page-grid { grid-template-columns: minmax(0, 1fr) 20rem; }
    .cart-column {
      align-self: start;
      position: sticky;
      top: 1.5rem;
    }

    .detail-grid { grid-template-columns: minmax(0, 1fr) 24rem; }
    .detail-info { align-content: start; }
  }
`;

const app = new Hono<HonoEnv>();

app.get("/api/storefront/live", async (context) => {
  return await acceptStorefrontLiveSocket(context.req.raw, context.env, WORKER_NAME);
});

app.get("/api/cart/live", async (context) => {
  return await acceptStorefrontCartSocket(context.req.raw, context.env, WORKER_NAME);
});

app.get("/app/status", (context) => {
  return context.text("worker app namespace");
});

app.get("/app/v1.0", (context) => {
  return context.text("worker app version route");
});

app.get("/src/status", (context) => {
  return context.text("worker src namespace");
});

app.get("/search", (context) => {
  return context.text(`worker search ${context.req.query("url") ?? ""}`);
});

app.get("/api/stream", () => {
  const encoder = new TextEncoder();
  return new Response(
    new ReadableStream({
      start(controller) {
        controller.enqueue(encoder.encode("first\n"));
        setTimeout(() => {
          controller.enqueue(encoder.encode("second\n"));
          controller.close();
        }, 25);
      },
    }),
    {
      headers: {
        "content-type": "text/plain; charset=utf-8",
      },
    },
  );
});

app.use("*", async (context, next) => {
  const session = storefrontSessionFromRequest(context.req.raw);
  const runtime: RuntimeContext = {
    ...session,
    stmCount: await incrementStmRequestCount(context.env),
    timerTick: await sampleRuntimeTimer(),
  };
  context.set("runtime", runtime);
  await next();
  context.res.headers.set("x-dd-stm-count", String(runtime.stmCount));
  if (runtime.sessionCookie) {
    context.res.headers.append("set-cookie", runtime.sessionCookie);
  }
});

app.get("/", async (context) => {
  const runtime = context.get("runtime");
  const data = await loadStorefront(context.env, runtime, pathFromContext(context));
  return context.html(renderHome(data));
});

app.get("/api/cart", async (context) => {
  const runtime = context.get("runtime");
  const path = cartFragmentPath(context.req.raw);
  const data = await loadStorefront(context.env, runtime, path);
  return context.html(renderCartPanel(data.cart, data.recentOrders, path, path !== "/"));
});

app.post("/", async (context) => {
  const runtime = context.get("runtime");
  await handleStorefrontAction(context.env, runtime.sessionId, await context.req.parseBody());
  if (isFixiRequest(context.req.raw)) {
    const data = await loadStorefront(context.env, runtime, "/");
    return context.html(renderCartPanel(data.cart, data.recentOrders, "/", false));
  }
  return context.redirect("/", 303);
});

app.get("/notes/:slug", async (context) => {
  const runtime = context.get("runtime");
  const path = pathFromContext(context);
  const data = await loadProduct(context.env, runtime, path, context.req.param("slug"));
  if (!data) {
    return context.text("Product not found", 404);
  }
  return context.html(renderProduct(data));
});

app.post("/notes/:slug", async (context) => {
  const runtime = context.get("runtime");
  const slug = context.req.param("slug");
  const path = pathFromContext(context);
  await handleStorefrontAction(
    context.env,
    runtime.sessionId,
    await context.req.parseBody(),
    slug,
  );
  if (isFixiRequest(context.req.raw)) {
    const data = await loadStorefront(context.env, runtime, path);
    return context.html(renderCartPanel(data.cart, data.recentOrders, path, true));
  }
  return context.redirect(path, 303);
});

export default {
  fetch(request: Request, env: Env): Response | Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/api/storefront/live") {
      return acceptStorefrontLiveSocket(request, env, WORKER_NAME);
    }
    if (url.pathname === "/api/cart/live") {
      return acceptStorefrontCartSocket(request, env, WORKER_NAME);
    }
    return app.fetch(request, env);
  },
  async wake(event: StorefrontSocketWakeEvent): Promise<void> {
    await handleStorefrontLiveSocketWake(event, WORKER_NAME);
  },
};

function renderHome(data: StorefrontData): string {
  return documentShell(
    "Edge Goods Hono",
    `
      <div class="page-grid" data-route="home">
        <div class="content-stack">
          <section class="section">
            <div class="heading-group">
              <p class="eyebrow">Hono storefront</p>
              <h1>Edge Goods Hono</h1>
              <p class="summary">
                A small shop rendered by Hono with a KV-backed catalog, order records, and
                a keyed-memory cart running inside the dd worker runtime.
              </p>
            </div>
            <dl class="stats">
              ${data.databaseStats.map((stat) => `
                <div>
                  <dt>${escapeHtml(stat.label)}</dt>
                  <dd class="tabular">${escapeHtml(stat.value)}</dd>
                </div>
              `).join("")}
            </dl>
          </section>

          <section class="catalog" aria-labelledby="catalog-heading">
            <div class="catalog-header">
              <div class="section">
                <h2 id="catalog-heading">Worker catalog</h2>
                <p class="summary">
                  Product records are seeded into KV on first request and read back by the route
                  handler.
                </p>
              </div>
              <a
                class="secondary-button"
                href="/notes/runtime"
              >
                Featured print
              </a>
            </div>
            <div class="product-grid">
              ${data.products.map((product) => renderProductCard(product, "/", `/notes/${product.slug}`)).join("")}
            </div>
          </section>
        </div>
        <div class="cart-column">
          ${renderCartPanel(data.cart, data.recentOrders, "/", false)}
        </div>
      </div>
    `,
  );
}

function renderProduct(data: ProductData): string {
  return documentShell(
    data.product.name,
    `
      <div class="detail-stack" data-route="project" data-path="${escapeAttribute(data.path)}">
        <section class="detail-grid">
          <div class="image-frame">
            <img
              alt=""
              src="${escapeAttribute(data.product.imageUrl)}"
            >
          </div>
          <div class="detail-info">
            <div class="heading-group">
              <p class="eyebrow">${escapeHtml(data.product.category)}</p>
              <h1>
                ${escapeHtml(data.product.name)}
              </h1>
              <p class="summary">${escapeHtml(data.product.story)}</p>
            </div>
            <div class="facts">
              ${renderFact("Price", data.product.price)}
              ${renderFact("Stock", `${data.product.stock} prints`)}
              ${renderFact("Route handler", String(data.runtime.stmCount), "data-stm-count")}
              ${renderFact("Timer tick", data.runtime.timerTick)}
            </div>

            <form
              class="quantity-form"
              method="post"
              action="${escapeAttribute(data.path)}"
              fx-action="${escapeAttribute(data.path)}"
              fx-method="post"
              fx-target="#storefront-cart"
              fx-swap="outerHTML"
            >
              <input name="intent" type="hidden" value="add-to-cart">
              <input name="slug" type="hidden" value="${escapeAttribute(data.product.slug)}">
              <label class="label">
                Quantity
                <input aria-label="Quantity" max="9" min="1" name="quantity" type="number" value="1">
              </label>
              <button class="primary-button" data-testid="detail-add-to-cart" type="submit">Add to cart</button>
            </form>

            <p class="summary">
              The route is handled by Hono inside the dd worker runtime after reading product
              data from KV and cart data from keyed memory.
            </p>
          </div>
        </section>

        <section class="detail-grid">
          <div class="section">
            <h2>Product details</h2>
            <ul class="spec-list" role="list">
              ${data.product.specs.map((spec) => `<li class="muted">${escapeHtml(spec)}</li>`).join("")}
            </ul>
          </div>
          ${renderCartPanel(data.cart, data.recentOrders, data.path, true)}
        </section>

        <section class="section">
          <h2>More from the catalog</h2>
          <div class="related-grid">
            ${data.relatedProducts.map((product) => `
              <a
                class="related-card"
                href="/notes/${escapeAttribute(product.slug)}"
              >
                <span class="label">${escapeHtml(product.name)}</span>
                <span class="muted tabular">${escapeHtml(product.price)}</span>
              </a>
            `).join("")}
          </div>
        </section>
      </div>
    `,
  );
}

function renderProductCard(
  product: Product & { price: string },
  action: string,
  detailHref: string,
): string {
  return `
    <article class="product-card">
      <a
        class="product-image-link"
        href="${escapeAttribute(detailHref)}"
        aria-label="${escapeAttribute(product.name)}"
      >
        <img
          alt=""
          src="${escapeAttribute(product.imageUrl)}"
        >
      </a>
      <div class="product-body">
        <div class="section">
          <div class="product-meta">
            <p class="eyebrow">${escapeHtml(product.category)}</p>
            <span class="badge">${escapeHtml(product.badge)}</span>
          </div>
          <h3>
            <a
              href="${escapeAttribute(detailHref)}"
            >
              ${escapeHtml(product.name)}
            </a>
          </h3>
          <p class="summary">${escapeHtml(product.description)}</p>
        </div>
        <div class="price-row">
          <p class="label tabular">
            ${escapeHtml(product.price)}
          </p>
          <form
            class="add-form"
            method="post"
            action="${escapeAttribute(action)}"
            fx-action="${escapeAttribute(action)}"
            fx-method="post"
            fx-target="#storefront-cart"
            fx-swap="outerHTML"
          >
            <input name="intent" type="hidden" value="add-to-cart">
            <input name="slug" type="hidden" value="${escapeAttribute(product.slug)}">
            <input name="quantity" type="hidden" value="1">
            <button class="secondary-button" data-testid="home-add-${escapeAttribute(product.slug)}" type="submit">Add</button>
          </form>
        </div>
      </div>
    </article>
  `;
}

function renderCartPanel(
  cart: CartView,
  recentOrders: StorefrontOrder[],
  action: string,
  compact: boolean,
): string {
  const itemLabel = cart.itemCount === 0
    ? "No prints selected."
    : `${cart.itemCount} item${cart.itemCount === 1 ? "" : "s"} in memory.`;
  return `
    <aside class="cart-panel" id="storefront-cart" aria-labelledby="cart-heading">
      <div class="cart-heading">
        <div class="section">
          <h2 id="cart-heading">Cart</h2>
          <p class="muted" data-testid="cart-count">${escapeHtml(itemLabel)}</p>
        </div>
        <p class="label tabular">${escapeHtml(cart.subtotal)}</p>
      </div>

      ${cart.lastOrderId ? `<p class="success">Order ${escapeHtml(cart.lastOrderId)} was written to KV.</p>` : ""}

      <div class="cart-lines">
        ${cart.lines.length === 0
          ? `<p class="cart-line muted">Add a print to exercise the database-backed cart.</p>`
          : cart.lines.map((line) => `
            <div class="cart-line">
              <div class="line-row">
                <p class="label">${escapeHtml(line.product.name)}</p>
                <p class="muted tabular">${escapeHtml(line.lineTotal)}</p>
              </div>
              <p class="muted tabular">Quantity ${line.quantity}</p>
            </div>
          `).join("")}
      </div>

      <form
        class="checkout-form"
        method="post"
        action="${escapeAttribute(action)}"
        fx-action="${escapeAttribute(action)}"
        fx-method="post"
        fx-target="#storefront-cart"
        fx-swap="outerHTML"
      >
        <input name="intent" type="hidden" value="checkout">
        <input aria-label="Email" name="email" placeholder="receipt@example.com" type="email">
        <button class="${compact ? "secondary-button" : "primary-button"}" ${cart.lines.length === 0 ? "disabled" : ""} type="submit">
          ${compact ? "Checkout from detail" : "Checkout"}
        </button>
      </form>

      ${compact ? "" : `
        <form
          method="post"
          action="${escapeAttribute(action)}"
          fx-action="${escapeAttribute(action)}"
          fx-method="post"
          fx-target="#storefront-cart"
          fx-swap="outerHTML"
        >
          <input name="intent" type="hidden" value="clear-cart">
          <button class="secondary-button" ${cart.lines.length === 0 ? "disabled" : ""} type="submit">
            Clear cart
          </button>
        </form>
      `}

      <section class="section">
        <h2>Recent orders</h2>
        ${recentOrders.length === 0
          ? `<p class="muted">Checkout writes the first order row.</p>`
          : `<div class="section">${recentOrders.map((order) => `
              <p class="muted tabular">
                ${escapeHtml(order.id)} - ${order.lines.length} line${order.lines.length === 1 ? "" : "s"} - ${formatCurrency(order.totalCents)}
              </p>
            `).join("")}</div>`}
      </section>
    </aside>
  `;
}

function renderFact(
  label: string,
  value: string,
  dataAttribute?: string,
): string {
  const attribute = dataAttribute ? ` ${dataAttribute}="${escapeAttribute(value)}"` : "";
  return `
    <div class="price-row">
      <p class="muted">${escapeHtml(label)}</p>
      <p class="label tabular"${attribute}>${escapeHtml(value)}</p>
    </div>
  `;
}

function documentShell(title: string, body: string): string {
  return `<!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>${escapeHtml(title)}</title>
        <link rel="stylesheet" href="https://rsms.me/inter/inter.css">
        <style>${styles}</style>
      </head>
      <body>
        <div class="app" data-framework="hono">
          <header class="header">
            <div class="header-inner">
              <div class="brand">
                <a href="/" aria-label="Homepage">Edge Goods Hono</a>
              </div>
              <nav class="desktop-nav" aria-label="Main">
                <a class="nav-link" href="/">Shop</a>
                <a class="nav-link" href="/notes/runtime">Runtime print</a>
                <a class="nav-link" href="/notes/cache-coast">Coast print</a>
              </nav>
              <div class="header-actions">
                <details class="mobile-menu">
                  <summary>Menu</summary>
                  <div class="mobile-panel">
                    <a href="/">Shop</a>
                    <a href="/notes/runtime">Runtime print</a>
                    <a href="/notes/cache-coast">Coast print</a>
                  </div>
                </details>
              </div>
            </div>
          </header>
          <main class="main">${body}</main>
          <footer class="footer">
            <div class="footer-inner">
              <p>
                Hono routes backed by KV, memory, timers, and a
                <span data-storefront-live-status>socket starting</span>.
              </p>
              <a href="/notes/trace-meadow">View studio proof</a>
            </div>
          </footer>
        </div>
        <script type="module" src="/assets/client.js"></script>
      </body>
    </html>`;
}

async function incrementStmRequestCount(env: Env): Promise<number> {
  const memory = env.EXAMPLE_MEMORY.get(env.EXAMPLE_MEMORY.idFromName("vite-hono"));
  const requests = memory.tvar("requests", 0);
  return memory.atomic(() => {
    const next = Number(requests.read()) + 1;
    requests.write(next);
    return next;
  });
}

async function sampleRuntimeTimer(): Promise<string> {
  const started = Date.now();
  await new Promise((resolve) => setTimeout(resolve, 1));
  return `${Math.max(0, Date.now() - started)} ms`;
}

async function loadStorefront(
  env: Env,
  runtime: RuntimeContext,
  path: string,
): Promise<StorefrontData> {
  const [products, cart, recentOrders] = await Promise.all([
    loadCatalog(env.STORE_DB),
    readCart(env, runtime.sessionId),
    loadRecentOrders(env.STORE_DB),
  ]);
  return snapshotFrom(products, cart, recentOrders, runtime, path);
}

async function loadProduct(
  env: Env,
  runtime: RuntimeContext,
  path: string,
  slug: string,
): Promise<ProductData | null> {
  const data = await loadStorefront(env, runtime, path);
  const product = data.products.find((item) => item.slug === slug);
  if (!product) {
    return null;
  }
  return {
    ...data,
    product,
    relatedProducts: data.products.filter((item) => item.slug !== slug).slice(0, 2),
  };
}

async function handleStorefrontAction(
  env: Env,
  sessionId: string,
  formData: Record<string, FormDataEntryValue | FormDataEntryValue[]>,
  fallbackSlug = "",
): Promise<void> {
  const intent = formField(formData, "intent", "add-to-cart");
  if (intent === "clear-cart") {
    await writeCart(env, sessionId, EMPTY_CART);
    await broadcastStorefrontCartUpdate(env, WORKER_NAME, sessionId);
    return;
  }
  if (intent === "checkout") {
    await checkout(env, sessionId, formField(formData, "email"));
    return;
  }
  await addToCart(
    env,
    sessionId,
    formField(formData, "slug", fallbackSlug),
    Number(formField(formData, "quantity", "1")),
  );
}

async function addToCart(
  env: Env,
  sessionId: string,
  slug: string,
  quantity: number,
): Promise<CartState> {
  const products = await loadCatalog(env.STORE_DB);
  const product = products.find((item) => item.slug === slug);
  if (!product) {
    return await readCart(env, sessionId);
  }
  const memory = cartMemory(env, sessionId);
  const cart = memory.tvar<CartState>("cart", EMPTY_CART);
  const safeQuantity = Math.max(1, Math.min(9, Math.trunc(quantity)));
  const nextCart = await memory.atomic(() => {
    const current = normalizeCart(cart.read());
    const lines = [...current.lines];
    const line = lines.find((item) => item.slug === product.slug);
    if (line) {
      line.quantity = Math.min(product.stock, line.quantity + safeQuantity);
    } else {
      lines.push({ slug: product.slug, quantity: Math.min(product.stock, safeQuantity) });
    }
    const next = normalizeCart({ lines, lastOrderId: null });
    cart.write(next);
    return next;
  });
  await broadcastStorefrontCartUpdate(env, WORKER_NAME, sessionId);
  return nextCart;
}

async function checkout(
  env: Env,
  sessionId: string,
  customerEmail: string,
): Promise<StorefrontOrder | null> {
  const products = await loadCatalog(env.STORE_DB);
  const currentCart = await readCart(env, sessionId);
  const cart = cartViewFrom(currentCart, products);
  if (cart.lines.length === 0) {
    return null;
  }
  const order: StorefrontOrder = {
    id: crypto.randomUUID().slice(0, 8),
    createdAt: new Date().toISOString(),
    customerEmail: normalizeEmail(customerEmail),
    totalCents: cart.subtotalCents,
    lines: cart.lines.map((line) => ({
      slug: line.slug,
      name: line.product.name,
      quantity: line.quantity,
      priceCents: line.product.priceCents,
    })),
  };

  await env.STORE_DB.put(orderKey(order.id), JSON.stringify(order));
  const existingOrderIds = await loadOrderIds(env.STORE_DB);
  await env.STORE_DB.put(
    ORDERS_INDEX_KEY,
    JSON.stringify([order.id, ...existingOrderIds.filter((id) => id !== order.id)].slice(0, 12)),
  );
  await writeCart(env, sessionId, { lines: [], lastOrderId: order.id });
  await broadcastStorefrontCartUpdate(env, WORKER_NAME, sessionId);
  return order;
}

async function readCart(env: Env, sessionId: string): Promise<CartState> {
  const memory = cartMemory(env, sessionId);
  const cart = memory.tvar<CartState>("cart", EMPTY_CART);
  return memory.atomic(() => normalizeCart(cart.read()));
}

async function writeCart(env: Env, sessionId: string, nextCart: CartState): Promise<CartState> {
  const memory = cartMemory(env, sessionId);
  const cart = memory.tvar<CartState>("cart", EMPTY_CART);
  return memory.atomic(() => {
    const normalized = normalizeCart(nextCart);
    cart.write(normalized);
    return normalized;
  });
}

async function acceptStorefrontLiveSocket(
  request: Request,
  env: Env,
  workerName: string,
): Promise<Response> {
  if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
    return new Response("Expected WebSocket upgrade", {
      status: 426,
      headers: { upgrade: "websocket" },
    });
  }
  const room = env.EXAMPLE_MEMORY.get(env.EXAMPLE_MEMORY.idFromName(`${workerName}:live`));
  return await room.atomic(() => {
    const handles = room.tvar<string[]>(LIVE_SOCKET_HANDLES_KEY, []);
    const events = room.tvar<number>(LIVE_SOCKET_EVENTS_KEY, 0);
    const { handle, response } = room.accept(request);
    const nextEvent = Number(events.read()) + 1;
    const nextHandles = [
      ...normalizeSocketHandles(handles.read()).filter((value) => value !== handle).slice(-7),
      handle,
    ];
    handles.write(nextHandles);
    events.write(nextEvent);
    return response;
  });
}

async function acceptStorefrontCartSocket(
  request: Request,
  env: Env,
  workerName: string,
): Promise<Response> {
  if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
    return new Response("Expected WebSocket upgrade", {
      status: 426,
      headers: { upgrade: "websocket" },
    });
  }
  const runtime = storefrontSessionFromRequest(request);
  const room = cartSocketRoom(env, workerName, runtime.sessionId);
  return await room.atomic(() => {
    const handles = room.tvar<string[]>(CART_SOCKET_HANDLES_KEY, []);
    const events = room.tvar<number>(CART_SOCKET_EVENTS_KEY, 0);
    const { handle, response } = room.accept(request);
    const nextEvent = Number(events.read()) + 1;
    const nextHandles = [
      ...normalizeSocketHandles(handles.read()).filter((value) => value !== handle).slice(-7),
      handle,
    ];
    handles.write(nextHandles);
    events.write(nextEvent);
    return response;
  });
}

async function broadcastStorefrontCartUpdate(
  env: Env,
  workerName: string,
  sessionId: string,
): Promise<void> {
  const room = cartSocketRoom(env, workerName, sessionId);
  const effects = await room.atomic(() => {
    const handles = room.tvar<string[]>(CART_SOCKET_HANDLES_KEY, []);
    const events = room.tvar<number>(CART_SOCKET_EVENTS_KEY, 0);
    const nextEvent = Number(events.read()) + 1;
    const nextHandles = normalizeSocketHandles(handles.read()).slice(-8);
    handles.write(nextHandles);
    events.write(nextEvent);
    return nextHandles.map((handle) => socketSendEffect(
      handle,
      cartSocketPayload(workerName, "updated", nextEvent),
    ));
  });
  await room.apply(effects);
}

async function handleStorefrontLiveSocketWake(
  event: StorefrontSocketWakeEvent,
  workerName: string,
): Promise<void> {
  const room = event.stub;
  const handle = String(event.handle ?? "");
  if (!room || !handle) {
    return;
  }
  if (event.type === "socketclose") {
    await room.atomic(() => {
      const handles = room.tvar<string[]>(LIVE_SOCKET_HANDLES_KEY, []);
      const cartHandles = room.tvar<string[]>(CART_SOCKET_HANDLES_KEY, []);
      handles.write(normalizeSocketHandles(handles.read()).filter((value) => value !== handle));
      cartHandles.write(normalizeSocketHandles(cartHandles.read()).filter((value) => value !== handle));
    });
    return;
  }
  if (event.type !== "socketmessage") {
    return;
  }
  const effects = await room.atomic(() => {
    const events = room.tvar<number>(LIVE_SOCKET_EVENTS_KEY, 0);
    const nextEvent = Number(events.read()) + 1;
    events.write(nextEvent);
    return [
      socketSendEffect(
        handle,
        liveSocketPayload(workerName, String(event.data ?? "message"), nextEvent),
      ),
    ];
  });
  await room.apply(effects);
}

function cartMemory(env: Env, sessionId: string): MemoryShard {
  return env.EXAMPLE_MEMORY.get(env.EXAMPLE_MEMORY.idFromName(`storefront-cart:${sessionId}`));
}

function cartSocketRoom(env: Env, workerName: string, sessionId: string): MemoryShard {
  return env.EXAMPLE_MEMORY.get(
    env.EXAMPLE_MEMORY.idFromName(`${workerName}:cart-live:${sessionId}`),
  );
}

function socketSendEffect(handle: string, payload: string): MemoryEffect {
  return {
    type: "socket.send",
    handle,
    payload,
    kind: "text",
  };
}

async function ensureCatalog(db: KvNamespace): Promise<void> {
  if ((await db.get(CATALOG_VERSION_KEY)) === CATALOG_VERSION) {
    return;
  }
  await Promise.all(PRODUCTS.map((item) => db.put(productKey(item.slug), JSON.stringify(item))));
  await db.put(PRODUCT_SLUGS_KEY, JSON.stringify(PRODUCTS.map((item) => item.slug)));
  await db.put(CATALOG_VERSION_KEY, CATALOG_VERSION);
}

async function loadCatalog(db: KvNamespace): Promise<Product[]> {
  await ensureCatalog(db);
  const slugs = parseStringArray(await db.get(PRODUCT_SLUGS_KEY));
  const productSlugs = slugs.length > 0 ? slugs : PRODUCTS.map((item) => item.slug);
  const products = await Promise.all(
    productSlugs.map(async (slug) => parseProduct(await db.get(productKey(slug)))),
  );
  const catalog = products.filter((item): item is Product => item !== null);
  return catalog.length > 0 ? catalog : PRODUCTS;
}

async function loadOrderIds(db: KvNamespace): Promise<string[]> {
  return parseStringArray(await db.get(ORDERS_INDEX_KEY));
}

async function loadRecentOrders(db: KvNamespace): Promise<StorefrontOrder[]> {
  const orderIds = (await loadOrderIds(db)).slice(0, 4);
  const orders = await Promise.all(
    orderIds.map(async (id) => parseOrder(await db.get(orderKey(id)))),
  );
  return orders.filter((order): order is StorefrontOrder => order !== null);
}

function snapshotFrom(
  products: Product[],
  cart: CartState,
  recentOrders: StorefrontOrder[],
  runtime: RuntimeContext,
  path: string,
): StorefrontData {
  return {
    products: products.map(withPrice),
    cart: cartViewFrom(cart, products),
    recentOrders,
    runtime,
    path,
    databaseStats: [
      { label: "Catalog rows", value: String(products.length) },
      { label: "Open cart", value: runtime.sessionId.slice(0, 8) },
      { label: "Orders stored", value: String(recentOrders.length) },
      { label: "Timer tick", value: runtime.timerTick },
    ],
  };
}

function cartViewFrom(cart: CartState, products: Product[]): CartView {
  const productsBySlug = new Map(products.map((item) => [item.slug, item]));
  const lines = cart.lines.flatMap((line) => {
    const product = productsBySlug.get(line.slug);
    if (!product) {
      return [];
    }
    const quantity = Math.max(1, Math.min(product.stock, Math.trunc(line.quantity)));
    const lineTotalCents = quantity * product.priceCents;
    return [{
      slug: line.slug,
      quantity,
      product,
      lineTotal: formatCurrency(lineTotalCents),
      lineTotalCents,
    }];
  });
  const subtotalCents = lines.reduce((total, line) => total + line.lineTotalCents, 0);
  return {
    lines,
    itemCount: lines.reduce((total, line) => total + line.quantity, 0),
    subtotal: formatCurrency(subtotalCents),
    subtotalCents,
    lastOrderId: cart.lastOrderId,
  };
}

function normalizeCart(value: unknown): CartState {
  if (!value || typeof value !== "object") {
    return EMPTY_CART;
  }
  const input = value as Partial<CartState>;
  const lines = Array.isArray(input.lines)
    ? input.lines.flatMap((line) => {
      if (!line || typeof line !== "object") {
        return [];
      }
      const slug = String((line as Partial<CartLine>).slug ?? "");
      const quantity = Math.max(
        1,
        Math.min(99, Math.trunc(Number((line as Partial<CartLine>).quantity) || 1)),
      );
      return slug ? [{ slug, quantity }] : [];
    })
    : [];
  return {
    lines,
    lastOrderId: input.lastOrderId ? String(input.lastOrderId) : null,
  };
}

function withPrice(product: Product): Product & { price: string } {
  return {
    ...product,
    price: formatCurrency(product.priceCents),
  };
}

function normalizeSocketHandles(value: unknown): string[] {
  return Array.isArray(value) ? value.map(String).filter(Boolean) : [];
}

function liveSocketPayload(workerName: string, message: string, eventCount: number): string {
  return JSON.stringify({
    type: "storefront.live",
    worker: workerName,
    message,
    eventCount,
  });
}

function cartSocketPayload(workerName: string, message: string, eventCount: number): string {
  return JSON.stringify({
    type: "storefront.cart",
    worker: workerName,
    message,
    eventCount,
  });
}

function storefrontSessionFromRequest(request: Request): RuntimeContext {
  const cookie = parseCookieHeader(request.headers.get("cookie") ?? "");
  const existing = cookie.get(SESSION_COOKIE);
  if (existing && /^[a-zA-Z0-9-]{16,80}$/.test(existing)) {
    return { sessionId: existing, sessionCookie: null, stmCount: 0, timerTick: "0 ms" };
  }
  const id = crypto.randomUUID();
  return {
    sessionId: id,
    sessionCookie: `${SESSION_COOKIE}=${encodeURIComponent(id)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000`,
    stmCount: 0,
    timerTick: "0 ms",
  };
}

function pathFromContext(context: Context<HonoEnv>): string {
  return new URL(context.req.url).pathname;
}

function cartFragmentPath(request: Request): string {
  const path = new URL(request.url).searchParams.get("path") ?? "/";
  return path.startsWith("/notes/") ? path : "/";
}

function isFixiRequest(request: Request): boolean {
  return request.headers.get("fx-request")?.toLowerCase() === "true";
}

function formField(
  formData: Record<string, FormDataEntryValue | FormDataEntryValue[]>,
  name: string,
  fallback = "",
): string {
  const value = formData[name];
  const first = Array.isArray(value) ? value[0] : value;
  return typeof first === "string" ? first : fallback;
}

function formatCurrency(cents: number): string {
  return `$${(Math.max(0, cents) / 100).toFixed(2)}`;
}

function normalizeEmail(value: string): string {
  const email = value.trim().toLowerCase();
  return email.includes("@") ? email : "guest@example.test";
}

function parseProduct(value: unknown): Product | null {
  const parsed = parseJsonObject(value);
  if (!parsed) {
    return null;
  }
  const slug = String(parsed.slug ?? "");
  const name = String(parsed.name ?? "");
  if (!slug || !name) {
    return null;
  }
  return {
    slug,
    name,
    category: String(parsed.category ?? "Print"),
    badge: String(parsed.badge ?? "In stock"),
    description: String(parsed.description ?? ""),
    story: String(parsed.story ?? ""),
    priceCents: Number(parsed.priceCents ?? 0),
    stock: Number(parsed.stock ?? 0),
    imageUrl: String(parsed.imageUrl ?? ""),
    specs: Array.isArray(parsed.specs) ? parsed.specs.map(String) : [],
  };
}

function parseOrder(value: unknown): StorefrontOrder | null {
  const parsed = parseJsonObject(value);
  if (!parsed) {
    return null;
  }
  const id = String(parsed.id ?? "");
  if (!id) {
    return null;
  }
  return {
    id,
    createdAt: String(parsed.createdAt ?? ""),
    customerEmail: String(parsed.customerEmail ?? ""),
    totalCents: Number(parsed.totalCents ?? 0),
    lines: Array.isArray(parsed.lines)
      ? parsed.lines.map((line) => ({
        slug: String(line?.slug ?? ""),
        name: String(line?.name ?? ""),
        quantity: Number(line?.quantity ?? 0),
        priceCents: Number(line?.priceCents ?? 0),
      }))
      : [],
  };
}

function parseStringArray(value: unknown): string[] {
  try {
    const parsed = typeof value === "string" ? JSON.parse(value || "[]") : value;
    return Array.isArray(parsed) ? parsed.map(String).filter(Boolean) : [];
  } catch {
    return [];
  }
}

function parseJsonObject(value: unknown): Record<string, unknown> | null {
  try {
    const parsed = typeof value === "string" ? JSON.parse(value) : value;
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? parsed as Record<string, unknown>
      : null;
  } catch {
    return null;
  }
}

function parseCookieHeader(header: string): Map<string, string> {
  const cookies = new Map<string, string>();
  for (const part of header.split(";")) {
    const [name, ...valueParts] = part.trim().split("=");
    if (!name || valueParts.length === 0) {
      continue;
    }
    cookies.set(name, decodeURIComponent(valueParts.join("=")));
  }
  return cookies;
}

function escapeHtml(value: unknown): string {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function escapeAttribute(value: unknown): string {
  return escapeHtml(value);
}

function productKey(slug: string): string {
  return `storefront:product:${slug}`;
}

function orderKey(id: string): string {
  return `storefront:order:${id}`;
}
