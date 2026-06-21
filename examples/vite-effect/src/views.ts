import { htmlResponse } from "./http";
import type { AuditEvent, AuthSessionView, Session, User } from "./types";

export function renderHome(): Response {
  return htmlResponse(shell("Passkey Auth", `
    <section class="panel">
      <p class="eyebrow">Effect worker</p>
      <h1>Passkey Auth</h1>
      <form data-passkey-register>
        <label>
          Username
          <input autocomplete="username webauthn" name="username" required>
        </label>
        <label>
          Display name
          <input autocomplete="name" name="displayName" required>
        </label>
        <div class="actions">
          <button data-passkey-randomize type="button">New test account</button>
          <button type="submit">Create passkey</button>
        </div>
      </form>
      <div class="divider"><span>or</span></div>
      <form data-passkey-login>
        <button type="submit">Sign in with passkey</button>
      </form>
      <p class="status" data-passkey-message></p>
    </section>
  `));
}

export function renderAccount(session: Session, user: User): Response {
  return htmlResponse(shell("Signed in", `
    <section class="panel">
      <p class="eyebrow">Signed in by passkey</p>
      <h1>${escapeHtml(user.displayName)}</h1>
      <dl>
        <div><dt>Username</dt><dd>${escapeHtml(user.username)}</dd></div>
        <div><dt>Role</dt><dd>${escapeHtml(user.role)}</dd></div>
        <div><dt>Passkeys</dt><dd>${user.credentials.length}</dd></div>
        <div><dt>Session</dt><dd>${escapeHtml(session.id.slice(0, 8))}</dd></div>
      </dl>
      <div class="actions">
        ${user.role === "admin" ? `<a href="/audit">Audit trail</a>` : ""}
        <a href="/api/session">JSON session</a>
        <form data-effect-form method="post" action="/logout">
          <button type="submit">Sign out</button>
        </form>
      </div>
    </section>
  `));
}

export function renderAccountSummary(view: AuthSessionView): Response {
  return htmlResponse(shell("Signed in", `
    <section class="panel">
      <p class="eyebrow">Signed in by passkey</p>
      <h1>${escapeHtml(view.user.displayName)}</h1>
      <dl>
        <div><dt>Username</dt><dd>${escapeHtml(view.user.username)}</dd></div>
        <div><dt>Role</dt><dd>${escapeHtml(view.user.role)}</dd></div>
        <div><dt>Passkeys</dt><dd>${view.user.passkeys}</dd></div>
        <div><dt>Session</dt><dd>${escapeHtml(view.session.id.slice(0, 8))}</dd></div>
      </dl>
      <div class="actions">
        ${view.user.role === "admin" ? `<a href="/audit">Audit trail</a>` : ""}
        <a href="/api/session">JSON session</a>
        <form data-effect-form method="post" action="/logout">
          <button type="submit">Sign out</button>
        </form>
      </div>
    </section>
  `));
}

export function renderAudit(events: AuditEvent[]): Response {
  return htmlResponse(shell("Audit trail", `
    <section class="panel wide">
      <p class="eyebrow">KV audit trail</p>
      <h1>Recent auth events</h1>
      <div class="events">
        ${events.length === 0 ? `<p class="summary">No events yet.</p>` : events.map((event) => `
          <article>
            <strong>${escapeHtml(event.type)}</strong>
            <span>${escapeHtml(event.username)}</span>
            <time>${escapeHtml(event.at)}</time>
            <p>${escapeHtml(event.detail)}</p>
          </article>
        `).join("")}
      </div>
      <p><a href="/me">Back to account</a></p>
    </section>
  `));
}

function shell(title: string, body: string): string {
  return `<!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>${escapeHtml(title)}</title>
        <style>
          :root { color: #171717; background: #f7f7f5; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
          * { box-sizing: border-box; }
          body { min-width: 320px; margin: 0; }
          main { display: grid; min-height: 100dvh; place-items: center; padding: 2rem 1rem; }
          .panel { display: grid; width: min(100%, 32rem); gap: 1.25rem; border-radius: 8px; background: #fff; padding: 1.25rem; box-shadow: 0 1px 2px rgb(0 0 0 / 0.06), 0 0 0 1px rgb(0 0 0 / 0.08); }
          .wide { width: min(100%, 46rem); }
          h1, p, dl { margin: 0; }
          h1 { font-size: clamp(2rem, 7vw, 4rem); line-height: 0.95; letter-spacing: 0; }
          .eyebrow { color: #0369a1; font-size: 0.875rem; font-weight: 700; text-transform: uppercase; }
          .summary, .status, dd, article p, time, span { color: #525252; line-height: 1.6; }
          form, dl, .events { display: grid; gap: 0.875rem; }
          label { display: grid; gap: 0.375rem; font-weight: 600; }
          input { width: 100%; border: 0; border-radius: 6px; padding: 0.7rem 0.8rem; box-shadow: 0 0 0 1px rgb(0 0 0 / 0.14); font: inherit; }
          button, a { border: 0; border-radius: 6px; background: #0f766e; color: #fff; display: inline-flex; align-items: center; justify-content: center; min-height: 2.75rem; padding: 0.65rem 0.85rem; font: inherit; font-weight: 700; text-decoration: none; }
          button:disabled { opacity: 0.6; }
          .divider { align-items: center; color: #737373; display: grid; font-size: 0.875rem; grid-template-columns: 1fr auto 1fr; gap: 0.75rem; }
          .divider::before, .divider::after { background: rgb(0 0 0 / 0.1); content: ""; height: 1px; }
          dl div, article { display: grid; gap: 0.25rem; border-top: 1px solid rgb(0 0 0 / 0.08); padding-top: 0.875rem; }
          dt, strong { font-weight: 700; }
          dd { margin: 0; }
          .actions { display: flex; flex-wrap: wrap; gap: 0.75rem; align-items: center; }
          .actions form { display: inline; }
          .actions button { background: #334155; }
          [data-passkey-register] .actions { display: grid; grid-template-columns: 1fr 1fr; }
          [data-passkey-randomize] { background: #334155; }
          article { grid-template-columns: 1fr auto; }
          article p { grid-column: 1 / -1; }
        </style>
      </head>
      <body>
        <main data-worker="vite-effect">${body}</main>
        <script type="module" src="/assets/client.js"></script>
      </body>
    </html>`;
}

function escapeHtml(value: unknown): string {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
