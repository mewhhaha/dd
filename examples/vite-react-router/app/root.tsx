import type { ReactNode } from "react";
import { Link, Links, Meta, Outlet, Scripts, ScrollRestoration } from "react-router";
import { CartLiveSync, LiveSocketStatus } from "./live-status";
import "./tailwind.css";

export function Layout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" className="bg-neutral-50">
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <Meta />
        <Links />
        <link rel="stylesheet" href="https://rsms.me/inter/inter.css" />
      </head>
      <body className="font-sans text-neutral-950 antialiased">
        {children}
        <ScrollRestoration />
        <Scripts />
      </body>
    </html>
  );
}

export default function App() {
  return (
    <div className="isolate flex min-h-dvh flex-col bg-neutral-50" data-framework="react-router">
      <header className="border-b border-neutral-950/10 bg-white">
        <div className="mx-auto flex max-w-6xl items-center gap-4 px-4 py-4 sm:px-6 lg:px-8">
          <div className="flex flex-1 items-center">
            <Link
              to="/"
              aria-label="Homepage"
              className="text-base/6 font-semibold text-neutral-950 sm:text-sm/6"
            >
              Edge Goods
            </Link>
          </div>
          <nav className="hidden items-center gap-7 text-sm/6 text-neutral-600 lg:flex" aria-label="Main">
            <Link className="hover:text-neutral-950" to="/">
              Shop
            </Link>
            <Link className="hover:text-neutral-950" to="/projects/runtime">
              Runtime print
            </Link>
            <Link className="hover:text-neutral-950" to="/projects/cache-coast">
              Coast print
            </Link>
          </nav>
          <div className="flex flex-1 items-center justify-end gap-3">
            <details className="relative lg:hidden">
              <summary className="cursor-pointer list-none rounded-md px-3 py-2 text-base/6 font-medium text-neutral-700 ring-1 ring-neutral-950/10 hover:bg-neutral-950/5 sm:text-sm/6 [&::-webkit-details-marker]:hidden">
                Menu
              </summary>
              <div className="absolute right-0 z-10 mt-2 grid w-48 gap-1 rounded-md bg-white p-2 text-base/7 shadow-lg ring-1 ring-neutral-950/10 sm:text-sm/6">
                <Link className="rounded px-3 py-2 text-neutral-700 hover:bg-neutral-950/5" to="/">
                  Shop
                </Link>
                <Link className="rounded px-3 py-2 text-neutral-700 hover:bg-neutral-950/5" to="/projects/runtime">
                  Runtime print
                </Link>
                <Link className="rounded px-3 py-2 text-neutral-700 hover:bg-neutral-950/5" to="/projects/cache-coast">
                  Coast print
                </Link>
              </div>
            </details>
          </div>
        </div>
      </header>
      <main className="mx-auto w-full max-w-6xl flex-1 px-4 py-8 sm:px-6 sm:py-10 lg:px-8">
        <Outlet />
      </main>
      <CartLiveSync />
      <footer className="border-t border-neutral-950/10 bg-white">
        <div className="mx-auto flex max-w-6xl flex-col gap-3 px-4 py-6 text-base/7 text-neutral-600 sm:px-6 sm:text-sm/6 lg:flex-row lg:items-center lg:justify-between lg:px-8">
          <p>
            Database-backed catalog, memory cart, timer tick, and{" "}
            <LiveSocketStatus />
            .
          </p>
          <Link className="font-normal text-neutral-950" to="/projects/trace-meadow">
            View studio proof
          </Link>
        </div>
      </footer>
    </div>
  );
}
