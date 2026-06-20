import {
  Link,
  redirect,
  type ActionFunctionArgs,
  type LoaderFunctionArgs,
} from "react-router";
import { addToCart, checkoutCart } from "../cart-actions";
import { ddRequestContext } from "../dd-context";

export function meta({ params }: { params: { slug?: string } }) {
  return [{ title: `Edge Goods RSC · ${params.slug ?? "product"}` }];
}

export async function loader({ context, params }: LoaderFunctionArgs) {
  const dd = context.get(ddRequestContext);
  const snapshot = await dd.storefront.loadProduct(params.slug ?? "unknown");
  if (!snapshot) {
    throw new Response("Product not found", { status: 404 });
  }
  return {
    ...snapshot,
    slug: params.slug ?? "unknown",
    stmCount: await dd.incrementStmRequestCount(),
    timerTick: await dd.sampleTimerTick(),
    workerName: dd.workerName,
  };
}

export async function action({ context, params, request }: ActionFunctionArgs) {
  const dd = context.get(ddRequestContext);
  const formData = await request.formData();
  const intent = String(formData.get("intent") ?? "add-to-cart");
  if (intent === "checkout") {
    await dd.storefront.checkout(String(formData.get("email") ?? ""));
  } else {
    await dd.storefront.addToCart(
      String(formData.get("slug") ?? params.slug ?? ""),
      Number(formData.get("quantity") ?? 1),
    );
  }
  return redirect(new URL(request.url).pathname);
}

type LoaderData = Awaited<ReturnType<typeof loader>>;

export async function ServerComponent({
  loaderData,
  params,
}: {
  loaderData: LoaderData;
  params: { slug?: string };
}) {
  const slug = params.slug ?? "unknown";
  return (
    <div className="grid gap-8" data-route="project" data-path={`/projects/${slug}`}>
      <section className="grid gap-8 lg:grid-cols-[minmax(0,1fr)_24rem]">
        <div className="min-w-0 overflow-hidden rounded-lg bg-white shadow-sm ring-1 ring-neutral-950/10">
          <img
            alt=""
            className="aspect-[5/4] w-full object-cover outline-1 -outline-offset-1 outline-black/5"
            src={loaderData.product.imageUrl}
          />
        </div>
        <div className="grid content-start gap-6">
          <div className="grid gap-3">
            <p className="text-base/7 font-medium text-emerald-700 sm:text-sm/6">
              {loaderData.product.category}
            </p>
            <h1 className="max-w-[20ch] text-4xl font-semibold tracking-tight text-balance text-neutral-950 sm:text-5xl">
              {loaderData.product.name}
            </h1>
            <p className="max-w-[56ch] text-base/7 text-pretty text-neutral-600 sm:text-sm/6">
              {loaderData.product.story}
            </p>
          </div>

          <div className="grid gap-3 border-y border-neutral-950/10 py-4">
            <div className="flex items-center justify-between gap-4">
              <p className="text-base/7 text-neutral-600 sm:text-sm/6">Price</p>
              <p className="text-base/7 font-medium tabular-nums text-neutral-950 sm:text-sm/6">
                {loaderData.product.price}
              </p>
            </div>
            <div className="flex items-center justify-between gap-4">
              <p className="text-base/7 text-neutral-600 sm:text-sm/6">Stock</p>
              <p className="text-base/7 font-medium tabular-nums text-neutral-950 sm:text-sm/6">
                {loaderData.product.stock} prints
              </p>
            </div>
            <div className="flex items-center justify-between gap-4">
              <p className="text-base/7 text-neutral-600 sm:text-sm/6">Route loader</p>
              <p className="text-base/7 font-medium tabular-nums text-neutral-950 sm:text-sm/6" data-stm-count={loaderData.stmCount}>
                {loaderData.stmCount}
              </p>
            </div>
            <div className="flex items-center justify-between gap-4">
              <p className="text-base/7 text-neutral-600 sm:text-sm/6">Timer tick</p>
              <p className="text-base/7 font-medium tabular-nums text-neutral-950 sm:text-sm/6">
                {loaderData.timerTick}
              </p>
            </div>
          </div>

          <form action={addToCart} className="grid gap-3 sm:grid-cols-[7rem_1fr]">
            <input name="slug" type="hidden" value={loaderData.product.slug} />
            <label className="grid gap-1 text-base/7 font-medium text-neutral-950 sm:text-sm/6">
              Quantity
              <input
                className="rounded-md bg-white px-3 py-2.5 text-base/7 tabular-nums text-neutral-950 ring-1 ring-neutral-950/10 focus:outline-2 focus:-outline-offset-1 focus:outline-emerald-600 sm:py-2 sm:text-sm/6"
                defaultValue="1"
                min="1"
                max="9"
                name="quantity"
                type="number"
              />
            </label>
            <button
              className="self-end rounded-md bg-emerald-700 px-3 py-2 text-sm/6 font-medium text-white ring-1 ring-emerald-700 hover:bg-emerald-800 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-emerald-600"
              data-testid="detail-add-to-cart"
              type="submit"
            >
              Add to cart
            </button>
          </form>

          <p className="text-base/7 text-pretty text-neutral-600 sm:text-sm/6">
            dd worker <span className="font-medium text-neutral-950">{loaderData.workerName}</span>{" "}
            rendered this React Router server route component through Vite's RSC runtime.
          </p>
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_20rem]">
        <div className="grid gap-3">
          <h2 className="text-2xl font-semibold tracking-tight text-balance text-neutral-950">
            Product details
          </h2>
          <ul className="grid gap-3 border-t border-neutral-950/10 pt-4" role="list">
            {loaderData.product.specs.map((spec) => (
              <li className="text-base/7 text-neutral-600 sm:text-sm/6" key={spec}>
                {spec}
              </li>
            ))}
          </ul>
        </div>
        <aside className="grid gap-4 rounded-lg bg-white p-5 shadow-sm ring-1 ring-neutral-950/10">
          <div className="flex items-start justify-between gap-4">
            <div>
              <h2 className="text-xl font-semibold tracking-tight text-neutral-950">Cart</h2>
              <p className="text-base/7 text-neutral-600 sm:text-sm/6" data-testid="detail-cart-count">
                {loaderData.cart.itemCount} item{loaderData.cart.itemCount === 1 ? "" : "s"} selected.
              </p>
            </div>
            <p className="text-base/7 font-medium tabular-nums text-neutral-950 sm:text-sm/6">
              {loaderData.cart.subtotal}
            </p>
          </div>
          <form action={checkoutCart} className="grid gap-3">
            <input
              aria-label="Email"
              className="rounded-md bg-white px-3 py-2.5 text-base/7 text-neutral-950 ring-1 ring-neutral-950/10 placeholder:text-neutral-400 focus:outline-2 focus:-outline-offset-1 focus:outline-emerald-600 sm:py-2 sm:text-sm/6"
              name="email"
              placeholder="receipt@example.com"
              type="email"
            />
            <button
              className="rounded-md px-3 py-2 text-sm/6 font-medium text-neutral-700 ring-1 ring-neutral-950/10 hover:bg-neutral-950/5 disabled:cursor-not-allowed disabled:text-neutral-400"
              disabled={loaderData.cart.lines.length === 0}
              type="submit"
            >
              Checkout from detail
            </button>
          </form>
        </aside>
      </section>

      <section className="grid gap-4">
        <h2 className="text-2xl font-semibold tracking-tight text-balance text-neutral-950">
          More from the catalog
        </h2>
        <div className="grid gap-4 sm:grid-cols-2">
          {loaderData.relatedProducts.map((product) => (
            <Link
              className="grid gap-2 rounded-lg bg-white p-4 shadow-sm ring-1 ring-neutral-950/10 hover:bg-neutral-950/2"
              key={product.slug}
              to={`/projects/${product.slug}`}
            >
              <span className="text-base/7 font-medium text-neutral-950 sm:text-sm/6">
                {product.name}
              </span>
              <span className="text-base/7 tabular-nums text-neutral-600 sm:text-sm/6">
                {product.price}
              </span>
            </Link>
          ))}
        </div>
      </section>
    </div>
  );
}
