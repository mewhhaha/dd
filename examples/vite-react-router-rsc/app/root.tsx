import type { ReactNode } from "react";
import { Link, Links, Meta, Outlet, ScrollRestoration } from "react-router";
import "./tailwind.css";

const styles = `
  :root {
    color: #151b22;
    background: #eef5f2;
    font-family:
      Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  }
  * { box-sizing: border-box; }
  body { margin: 0; min-width: 320px; }
  a { color: inherit; }
  main {
    width: min(960px, calc(100vw - 32px));
    margin: 0 auto;
    padding: 56px 0;
  }
  nav {
    display: flex;
    align-items: center;
    gap: 18px;
    margin-bottom: 40px;
    color: #53646b;
    font-size: 14px;
  }
  nav strong { color: #11171d; }
  section {
    border: 1px solid #cbd7d2;
    border-radius: 8px;
    background: #fbfffd;
    padding: clamp(24px, 6vw, 48px);
    box-shadow: 0 10px 28px rgba(30, 48, 42, 0.08);
  }
  h1 {
    margin: 0 0 16px;
    color: #11171d;
    font-size: clamp(34px, 7vw, 64px);
    line-height: 0.98;
    letter-spacing: 0;
  }
  p {
    max-width: 68ch;
    color: #465860;
    font-size: 17px;
    line-height: 1.65;
  }
  .actions { display: flex; gap: 12px; flex-wrap: wrap; margin-top: 28px; }
  .button {
    display: inline-flex;
    min-height: 42px;
    align-items: center;
    justify-content: center;
    border-radius: 6px;
    border: 1px solid #aab7ba;
    padding: 0 16px;
    color: #15191f;
    text-decoration: none;
    font-weight: 650;
  }
  .button.primary {
    border-color: #167a60;
    background: #167a60;
    color: white;
  }
  code {
    border-radius: 4px;
    background: #e5eeeb;
    padding: 2px 5px;
    font-size: 0.92em;
  }
`;

export function ServerLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <Meta />
        <Links />
        <style dangerouslySetInnerHTML={{ __html: styles }} />
      </head>
      <body>
        {children}
        <ScrollRestoration />
      </body>
    </html>
  );
}

export function ServerComponent() {
  return (
    <main data-framework="react-router-rsc">
      <nav>
        <strong>vite-react-router-rsc</strong>
        <span className="dd-stm-badge">dd STM counter</span>
        <Link to="/">Home</Link>
        <Link to="/projects/runtime">Runtime</Link>
      </nav>
      <Outlet />
    </main>
  );
}
