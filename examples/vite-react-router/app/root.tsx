import type { ReactNode } from "react";
import { Link, Links, Meta, Outlet, Scripts, ScrollRestoration } from "react-router";
import "./tailwind.css";

const styles = `
  :root {
    color: #1b1f24;
    background: #f7f3ea;
    font-family:
      Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  }
  * { box-sizing: border-box; }
  body { margin: 0; min-width: 320px; }
  a { color: inherit; }
  main {
    width: min(920px, calc(100vw - 32px));
    margin: 0 auto;
    padding: 56px 0;
  }
  nav {
    display: flex;
    align-items: center;
    gap: 18px;
    margin-bottom: 40px;
    color: #506070;
    font-size: 14px;
  }
  nav strong { color: #15191f; }
  section {
    border: 1px solid #d9d2c3;
    border-radius: 8px;
    background: #fffdf8;
    padding: clamp(24px, 6vw, 48px);
    box-shadow: 0 10px 28px rgba(49, 43, 31, 0.08);
  }
  h1 {
    margin: 0 0 16px;
    color: #11151a;
    font-size: clamp(34px, 7vw, 64px);
    line-height: 0.98;
    letter-spacing: 0;
  }
  p {
    max-width: 68ch;
    color: #465260;
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
    border: 1px solid #b8c0c7;
    padding: 0 16px;
    color: #15191f;
    text-decoration: none;
    font-weight: 650;
  }
  .button.primary {
    border-color: #1f6feb;
    background: #1f6feb;
    color: white;
  }
  code {
    border-radius: 4px;
    background: #eef1f4;
    padding: 2px 5px;
    font-size: 0.92em;
  }
`;

export function Layout({ children }: { children: ReactNode }) {
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
        <Scripts />
      </body>
    </html>
  );
}

export default function App() {
  return (
    <main data-framework="react-router">
      <nav>
        <strong>vite-react-router</strong>
        <span className="dd-stm-badge">dd STM counter</span>
        <Link to="/">Home</Link>
        <Link to="/projects/runtime">Runtime</Link>
      </nav>
      <Outlet />
    </main>
  );
}
