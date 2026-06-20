import { Form, Link, redirect, type ActionFunctionArgs, type LoaderFunctionArgs } from "react-router";
import { getDdRequestContext } from "../dd-context";

export function meta() {
  return [{ title: "Edge Goods storefront" }];
}

export async function loader({ context, request }: LoaderFunctionArgs) {
  const dd = getDdRequestContext(context, request);
  const stmCount = await dd.incrementStmRequestCount();
  const timerTick = await dd.sampleTimerTick();
  const snapshot = await dd.storefront.loadHome();
  return {
    ...snapshot,
    databaseStats: [
      ...snapshot.databaseStats,
      { label: "Timer tick", value: timerTick },
    ],
    stmCount,
    workerName: dd.workerName,
  };
}

export async function action({ context, request }: ActionFunctionArgs) {
  const dd = getDdRequestContext(context, request);
  const formData = await request.formData();
  const intent = String(formData.get("intent") ?? "add-to-cart");

  if (intent === "clear-cart") {
    await dd.storefront.clearCart();
  } else if (intent === "checkout") {
    await dd.storefront.checkout(String(formData.get("email") ?? ""));
  } else {
    await dd.storefront.addToCart(
      String(formData.get("slug") ?? ""),
      Number(formData.get("quantity") ?? 1),
    );
  }

  const redirectTo = String(formData.get("redirectTo") ?? new URL(request.url).pathname);
  return redirect(redirectTo);
}

type LoaderData = Awaited<ReturnType<typeof loader>>;

export default function Home({ loaderData }: { loaderData: LoaderData }) {
  return (
    <div className="grid gap-8 lg:grid-cols-[minmax(0,1fr)_20rem]" data-route="home">
      <div className="grid min-w-0 gap-8">
        <section className="grid gap-6">
          <div className="grid gap-4">
            <p className="text-base/7 font-medium text-emerald-700 sm:text-sm/6">
              React Router storefront
            </p>
            <h1 className="max-w-[20ch] text-4xl font-semibold tracking-tight text-balance text-neutral-950 sm:text-5xl">
              Edge Goods
            </h1>
            <p className="max-w-[56ch] text-base/7 text-pretty text-neutral-600 sm:text-sm/6">
              A tiny shop with a KV-backed catalog, order records, and a keyed-memory cart
              running inside the dd worker runtime.
            </p>
          </div>
          <dl className="grid gap-4 border-y border-neutral-950/10 py-4 sm:grid-cols-4">
            {loaderData.databaseStats.map((stat) => (
              <div className="grid gap-1" key={stat.label}>
                <dt className="text-base/7 font-medium text-neutral-950 sm:text-sm/6">
                  {stat.label}
                </dt>
                <dd className="text-base/7 tabular-nums text-neutral-600 sm:text-sm/6">
                  {stat.value}
                </dd>
              </div>
            ))}
          </dl>
        </section>

        <section className="@container grid gap-4" aria-labelledby="catalog-heading">
          <div className="flex items-end justify-between gap-4">
            <div className="grid gap-2">
              <h2 id="catalog-heading" className="text-2xl font-semibold tracking-tight text-balance text-neutral-950">
                Studio catalog
              </h2>
              <p className="max-w-[56ch] text-base/7 text-pretty text-neutral-600 sm:text-sm/6">
                Product records are seeded into KV on first request and read back by the route loader.
              </p>
            </div>
            <Link
              className="hidden rounded-md px-3 py-2 text-sm/6 font-medium text-neutral-700 ring-1 ring-neutral-950/10 hover:bg-neutral-950/5 sm:inline-flex"
              to="/projects/runtime"
            >
              Featured print
            </Link>
          </div>
          <div className="grid gap-4 @2xl:grid-cols-3">
            {loaderData.products.map((product) => (
              <article
                className="grid overflow-hidden rounded-lg bg-white shadow-sm ring-1 ring-neutral-950/10"
                key={product.slug}
              >
                <img
                  alt=""
                  className="aspect-4/3 w-full object-cover outline-1 -outline-offset-1 outline-black/5"
                  src={product.imageUrl}
                />
                <div className="grid gap-4 p-4">
                  <div className="grid gap-2">
                    <div className="flex items-center justify-between gap-3">
                      <p className="text-base/7 font-medium text-emerald-700 sm:text-sm/6">
                        {product.category}
                      </p>
                      <span className="rounded-md bg-amber-100 px-2 py-1 text-sm/5 font-medium text-amber-900">
                        {product.badge}
                      </span>
                    </div>
                    <h3 className="text-lg font-semibold text-balance text-neutral-950">
                      <Link className="hover:text-emerald-700" to={`/projects/${product.slug}`}>
                        {product.name}
                      </Link>
                    </h3>
                    <p className="text-base/7 text-pretty text-neutral-600 sm:text-sm/6">
                      {product.description}
                    </p>
                  </div>
                  <div className="flex items-center justify-between gap-3">
                    <p className="text-base/7 font-medium tabular-nums text-neutral-950 sm:text-sm/6">
                      {product.price}
                    </p>
                    <Form action="/?index" method="post">
                      <input name="intent" type="hidden" value="add-to-cart" />
                      <input name="slug" type="hidden" value={product.slug} />
                      <input name="quantity" type="hidden" value="1" />
                      <button
                        className="rounded-md px-3 py-2 text-sm/6 font-medium text-neutral-700 ring-1 ring-neutral-950/10 hover:bg-neutral-950/5 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-emerald-600"
                        type="submit"
                      >
                        Add
                      </button>
                    </Form>
                  </div>
                </div>
              </article>
            ))}
          </div>
        </section>
      </div>

      <aside className="grid gap-6 lg:sticky lg:top-6 lg:self-start" aria-labelledby="cart-heading">
        <section className="grid gap-5 rounded-lg bg-white p-5 shadow-sm ring-1 ring-neutral-950/10">
          <div className="flex items-start justify-between gap-4">
            <div className="grid gap-1">
              <h2 id="cart-heading" className="text-xl font-semibold tracking-tight text-neutral-950">
                Cart
              </h2>
              <p className="text-base/7 text-neutral-600 sm:text-sm/6">
                {loaderData.cart.itemCount === 0
                  ? "No prints selected."
                  : `${loaderData.cart.itemCount} item${loaderData.cart.itemCount === 1 ? "" : "s"} in memory.`}
              </p>
            </div>
            <p className="text-base/7 font-medium tabular-nums text-neutral-950 sm:text-sm/6">
              {loaderData.cart.subtotal}
            </p>
          </div>

          {loaderData.cart.lastOrderId ? (
            <p className="rounded-md bg-emerald-50 p-3 text-base/7 text-emerald-800 sm:text-sm/6">
              Order {loaderData.cart.lastOrderId} was written to KV.
            </p>
          ) : null}

          <div className="grid divide-y divide-neutral-950/10">
            {loaderData.cart.lines.length === 0 ? (
              <p className="py-3 text-base/7 text-neutral-600 sm:text-sm/6">
                Add a print to exercise the database-backed cart.
              </p>
            ) : (
              loaderData.cart.lines.map((line) => (
                <div className="grid gap-1 py-3" key={line.slug}>
                  <div className="flex justify-between gap-3">
                    <p className="min-w-0 text-base/7 font-medium text-neutral-950 sm:text-sm/6">
                      {line.product.name}
                    </p>
                    <p className="shrink-0 text-base/7 tabular-nums text-neutral-700 sm:text-sm/6">
                      {line.lineTotal}
                    </p>
                  </div>
                  <p className="text-base/7 tabular-nums text-neutral-600 sm:text-sm/6">
                    Quantity {line.quantity}
                  </p>
                </div>
              ))
            )}
          </div>

          <Form action="/?index" className="grid gap-3" method="post">
            <input name="intent" type="hidden" value="checkout" />
            <input
              aria-label="Email"
              className="rounded-md bg-white px-3 py-2.5 text-base/7 text-neutral-950 ring-1 ring-neutral-950/10 placeholder:text-neutral-400 focus:outline-2 focus:-outline-offset-1 focus:outline-emerald-600 sm:py-2 sm:text-sm/6"
              name="email"
              placeholder="receipt@example.com"
              type="email"
            />
            <button
              className="rounded-md bg-emerald-700 px-3 py-2 text-sm/6 font-medium text-white ring-1 ring-emerald-700 hover:bg-emerald-800 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-emerald-600 disabled:cursor-not-allowed disabled:bg-neutral-300 disabled:text-neutral-600 disabled:ring-neutral-300"
              disabled={loaderData.cart.lines.length === 0}
              type="submit"
            >
              Checkout
            </button>
          </Form>
          <Form action="/?index" method="post">
            <input name="intent" type="hidden" value="clear-cart" />
            <button
              className="rounded-md px-3 py-2 text-sm/6 font-medium text-neutral-700 ring-1 ring-neutral-950/10 hover:bg-neutral-950/5 disabled:cursor-not-allowed disabled:text-neutral-400"
              disabled={loaderData.cart.lines.length === 0}
              type="submit"
            >
              Clear cart
            </button>
          </Form>
        </section>

        <section className="grid gap-3 border-t border-neutral-950/10 pt-5">
          <h2 className="text-base/7 font-semibold text-neutral-950 sm:text-sm/6">
            Recent orders
          </h2>
          {loaderData.recentOrders.length === 0 ? (
            <p className="text-base/7 text-neutral-600 sm:text-sm/6">
              Checkout writes the first order row.
            </p>
          ) : (
            <div className="grid gap-2">
              {loaderData.recentOrders.map((order) => (
                <p className="text-base/7 tabular-nums text-neutral-600 sm:text-sm/6" key={order.id}>
                  {order.id} · {order.lines.length} line{order.lines.length === 1 ? "" : "s"} ·{" "}
                  {`$${(order.totalCents / 100).toFixed(2)}`}
                </p>
              ))}
            </div>
          )}
        </section>
      </aside>
    </div>
  );
}
