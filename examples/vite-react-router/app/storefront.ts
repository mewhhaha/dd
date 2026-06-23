export type KvNamespace = {
  get(key: string): Promise<unknown | null>;
  put(key: string, value: unknown): void | Promise<void>;
  delete(key: string): void | Promise<void>;
  list?(options?: { prefix?: string; limit?: number }): Promise<Array<{ key: string; value: unknown }>>;
};

export type MemoryNamespace = {
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

export type StorefrontEnv = {
  EXAMPLE_MEMORY: MemoryNamespace;
  STORE_DB: KvNamespace;
};

export type Product = {
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

export type CartLine = {
  slug: string;
  quantity: number;
};

export type CartLineView = CartLine & {
  product: Product;
  lineTotal: string;
  lineTotalCents: number;
};

export type CartView = {
  lines: CartLineView[];
  itemCount: number;
  subtotal: string;
  subtotalCents: number;
  lastOrderId: string | null;
};

export type StorefrontOrder = {
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

export type StorefrontSnapshot = {
  products: Array<Product & { price: string }>;
  cart: CartView;
  recentOrders: StorefrontOrder[];
  sessionId: string;
  databaseStats: Array<{ label: string; value: string }>;
};

export type ProductSnapshot = StorefrontSnapshot & {
  product: Product & { price: string };
  relatedProducts: Array<Product & { price: string }>;
};

type CartState = {
  lines: CartLine[];
  lastOrderId: string | null;
};

type StorefrontSocketWakeEvent = {
  type?: string;
  stub?: MemoryShard;
  handle?: string;
  data?: unknown;
};

export const FEATURED_PRODUCT_SLUG = "runtime";

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

export function createStorefront(env: StorefrontEnv, sessionId: string, workerName = "storefront") {
  const memory = env.EXAMPLE_MEMORY.get(
    env.EXAMPLE_MEMORY.idFromName(`storefront-cart:${sessionId}`),
  );
  const cartVar = memory.tvar<CartState>("cart", EMPTY_CART);

  async function readCart(): Promise<CartState> {
    return await memory.atomic(() => normalizeCart(cartVar.read()));
  }

  async function writeCart(nextCart: CartState): Promise<CartState> {
    return await memory.atomic(() => {
      const normalized = normalizeCart(nextCart);
      cartVar.write(normalized);
      return normalized;
    });
  }

  async function loadHome(): Promise<StorefrontSnapshot> {
    const [products, cart, recentOrders] = await Promise.all([
      loadCatalog(env.STORE_DB),
      readCart(),
      loadRecentOrders(env.STORE_DB),
    ]);
    return snapshotFrom(products, cart, recentOrders, sessionId);
  }

  async function loadProduct(slug: string): Promise<ProductSnapshot | null> {
    const [products, cart, recentOrders] = await Promise.all([
      loadCatalog(env.STORE_DB),
      readCart(),
      loadRecentOrders(env.STORE_DB),
    ]);
    const product = products.find((item) => item.slug === slug);
    if (!product) {
      return null;
    }
    const snapshot = snapshotFrom(products, cart, recentOrders, sessionId);
    return {
      ...snapshot,
      product: withPrice(product),
      relatedProducts: products
        .filter((item) => item.slug !== product.slug)
        .slice(0, 2)
        .map(withPrice),
    };
  }

  async function addToCart(slug: string, quantity = 1): Promise<CartState> {
    const products = await loadCatalog(env.STORE_DB);
    const product = products.find((item) => item.slug === slug);
    if (!product) {
      return await readCart();
    }
    const safeQuantity = Math.max(1, Math.min(9, Math.trunc(quantity)));
    const nextCart = await memory.atomic(() => {
      const current = normalizeCart(cartVar.read());
      const lines = [...current.lines];
      const line = lines.find((item) => item.slug === product.slug);
      if (line) {
        line.quantity = Math.min(product.stock, line.quantity + safeQuantity);
      } else {
        lines.push({ slug: product.slug, quantity: Math.min(product.stock, safeQuantity) });
      }
      const next = normalizeCart({ lines, lastOrderId: null });
      cartVar.write(next);
      return next;
    });
    await broadcastStorefrontCartUpdate(env, workerName, sessionId);
    return nextCart;
  }

  async function clearCart(): Promise<CartState> {
    const nextCart = await writeCart(EMPTY_CART);
    await broadcastStorefrontCartUpdate(env, workerName, sessionId);
    return nextCart;
  }

  async function checkout(customerEmail: string): Promise<StorefrontOrder | null> {
    const products = await loadCatalog(env.STORE_DB);
    const currentCart = await readCart();
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
    const nextOrderIds = [order.id, ...existingOrderIds.filter((id) => id !== order.id)].slice(0, 12);
    await env.STORE_DB.put(ORDERS_INDEX_KEY, JSON.stringify(nextOrderIds));
    await writeCart({ lines: [], lastOrderId: order.id });
    await broadcastStorefrontCartUpdate(env, workerName, sessionId);
    return order;
  }

  return {
    addToCart,
    checkout,
    clearCart,
    loadHome,
    loadProduct,
    sessionId,
  };
}

export function storefrontSessionFromRequest(request: Request): {
  id: string;
  setCookie: string | null;
} {
  const cookie = parseCookieHeader(request.headers.get("cookie") ?? "");
  const existing = cookie.get(SESSION_COOKIE);
  if (existing && /^[a-zA-Z0-9-]{16,80}$/.test(existing)) {
    return { id: existing, setCookie: null };
  }

  const id = crypto.randomUUID();
  return {
    id,
    setCookie: `${SESSION_COOKIE}=${encodeURIComponent(id)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000`,
  };
}

export function formatCurrency(cents: number): string {
  return `$${(Math.max(0, cents) / 100).toFixed(2)}`;
}

export async function acceptStorefrontLiveSocket(
  request: Request,
  env: StorefrontEnv,
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

export async function acceptStorefrontCartSocket(
  request: Request,
  env: StorefrontEnv,
  workerName: string,
): Promise<Response> {
  if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
    return new Response("Expected WebSocket upgrade", {
      status: 426,
      headers: { upgrade: "websocket" },
    });
  }
  const session = storefrontSessionFromRequest(request);
  const room = cartSocketRoom(env, workerName, session.id);
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

export async function broadcastStorefrontCartUpdate(
  env: StorefrontEnv,
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

export async function handleStorefrontLiveSocketWake(
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

async function ensureCatalog(db: KvNamespace): Promise<void> {
  if ((await db.get(CATALOG_VERSION_KEY)) === CATALOG_VERSION) {
    return;
  }
  await Promise.all(
    PRODUCTS.map((product) => db.put(productKey(product.slug), JSON.stringify(product))),
  );
  await db.put(PRODUCT_SLUGS_KEY, JSON.stringify(PRODUCTS.map((product) => product.slug)));
  await db.put(CATALOG_VERSION_KEY, CATALOG_VERSION);
}

async function loadCatalog(db: KvNamespace): Promise<Product[]> {
  await ensureCatalog(db);
  const slugs = parseStringArray(await db.get(PRODUCT_SLUGS_KEY));
  const productSlugs = slugs.length > 0 ? slugs : PRODUCTS.map((product) => product.slug);
  const products = await Promise.all(
    productSlugs.map(async (slug) => parseProduct(await db.get(productKey(slug)))),
  );
  const catalog = products.filter((product): product is Product => product !== null);
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
  sessionId: string,
): StorefrontSnapshot {
  return {
    products: products.map(withPrice),
    cart: cartViewFrom(cart, products),
    recentOrders,
    sessionId,
    databaseStats: [
      { label: "Catalog rows", value: String(products.length) },
      { label: "Open cart", value: sessionId.slice(0, 8) },
      { label: "Orders stored", value: String(recentOrders.length) },
    ],
  };
}

function cartViewFrom(cart: CartState, products: Product[]): CartView {
  const productsBySlug = new Map(products.map((product) => [product.slug, product]));
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
      const quantity = Math.max(1, Math.min(99, Math.trunc(Number((line as Partial<CartLine>).quantity) || 1)));
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
  const parsed = typeof value === "string" ? JSON.parse(value || "[]") : value;
  return Array.isArray(parsed) ? parsed.map(String).filter(Boolean) : [];
}

function parseJsonObject(value: unknown): Record<string, unknown> | null {
  const parsed = typeof value === "string" ? JSON.parse(value) : value;
  return parsed && typeof parsed === "object" && !Array.isArray(parsed)
    ? parsed as Record<string, unknown>
    : null;
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

function cartSocketRoom(env: StorefrontEnv, workerName: string, sessionId: string): MemoryShard {
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

function productKey(slug: string): string {
  return `storefront:product:${slug}`;
}

function orderKey(id: string): string {
  return `storefront:order:${id}`;
}
