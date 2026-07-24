import "./tailwind.css";
import "fixi-js";

export const marker = "vite-hono-client-module";

type FixiConfigWindow = Window & {
  fixiCfg?: {
    transition?: false;
  };
};

if (import.meta.hot) {
  import.meta.hot.accept();
}

document.documentElement.dataset.ddExample = "vite-hono";
document.documentElement.dataset.client = marker;
(window as FixiConfigWindow).fixiCfg = {
  ...(window as FixiConfigWindow).fixiCfg,
  transition: false,
};

let activeCartSocket: WebSocket | null = null;

setupFragmentForms();
connectCartSocket();

function connectCartSocket() {
  activeCartSocket?.close();
  activeCartSocket = null;
  const liveStatus = document.querySelector("[data-storefront-live-status]");
  if (!liveStatus || typeof WebSocket === "undefined") {
    if (liveStatus) {
      liveStatus.textContent = "socket unavailable";
    }
    return;
  }

  const cancelConnect = runAfterPageLoad(() => {
    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    const socket = new WebSocket(`${protocol}//${location.host}/api/cart/live`);
    activeCartSocket = socket;

    socket.addEventListener("open", () => {
      liveStatus.textContent = "socket open";
    });

    socket.addEventListener("message", (event) => {
      const status = cartSocketStatus(event.data);
      if (status) {
        liveStatus.textContent = status;
      }
      if (isCartUpdateMessage(event.data)) {
        void refreshCartFragment();
      }
    });

    socket.addEventListener("close", () => {
      liveStatus.textContent = "socket closed";
    });

    socket.addEventListener("error", () => {
      liveStatus.textContent = "socket error";
    });
  });

  window.addEventListener("beforeunload", () => {
    cancelConnect();
    activeCartSocket?.close();
  }, { once: true });
}

function setupFragmentForms() {
  document.addEventListener("submit", (event) => {
    void handleFragmentFormSubmit(event);
  }, true);
}

async function handleFragmentFormSubmit(event: SubmitEvent) {
  const form = event.target;
  if (!(form instanceof HTMLFormElement) || !form.hasAttribute("fx-action")) {
    return;
  }

  event.preventDefault();
  event.stopImmediatePropagation();

  const submitter = event.submitter instanceof HTMLElement ? event.submitter : undefined;
  const targetSelector = form.getAttribute("fx-target") || "";
  const target = targetSelector ? document.querySelector(targetSelector) : null;
  if (!target) {
    return;
  }

  const action = form.getAttribute("fx-action") || form.action || location.href;
  const method = (form.getAttribute("fx-method") || form.method || "get").toUpperCase();
  const requestUrl = new URL(action, location.href);
  const formData = submitter ? new FormData(form, submitter) : new FormData(form);
  const init: RequestInit = {
    headers: {
      accept: "text/html",
      "fx-request": "true",
    },
    method,
  };

  if (method === "GET") {
    for (const [name, value] of formData) {
      requestUrl.searchParams.append(name, String(value));
    }
  } else {
    init.body = formData;
  }

  const response = await fetch(requestUrl, init);
  if (!response.ok) {
    return;
  }

  replaceCartFragment(await response.text());
}

async function refreshCartFragment() {
  const currentCart = document.querySelector("#storefront-cart");
  if (!currentCart) {
    return;
  }

  const url = new URL("/api/cart", location.href);
  url.searchParams.set("path", location.pathname);
  const response = await fetch(url, {
    headers: { accept: "text/html" },
  });
  if (!response.ok) {
    return;
  }

  replaceCartFragment(await response.text());
}

function replaceCartFragment(html: string) {
  const currentCart = document.querySelector("#storefront-cart");
  if (!currentCart) {
    return;
  }

  const template = document.createElement("template");
  template.innerHTML = html;
  const nextCart = template.content.querySelector("#storefront-cart");
  if (!nextCart) {
    return;
  }
  currentCart.replaceWith(nextCart);
}

function runAfterPageLoad(callback: () => void): () => void {
  if (document.readyState === "complete") {
    const timeout = window.setTimeout(callback, 0);
    return () => window.clearTimeout(timeout);
  }

  window.addEventListener("load", callback, { once: true });
  return () => window.removeEventListener("load", callback);
}

function cartSocketStatus(value: unknown): string | null {
  try {
    const payload = JSON.parse(String(value)) as { type?: unknown; message?: unknown };
    if (payload.type !== "storefront.cart") {
      return null;
    }
    if (payload.message === "connected") {
      return "socket connected";
    }
    if (payload.message === "updated") {
      return "cart synced";
    }
    return "socket message";
  } catch {
    return null;
  }
}

function isCartUpdateMessage(value: unknown): boolean {
  try {
    const payload = JSON.parse(String(value)) as { type?: unknown; message?: unknown };
    return payload.type === "storefront.cart" && payload.message === "updated";
  } catch {
    return false;
  }
}
