import "./tailwind.css";
import "fixi-js";

export const marker = "vite-hono-client-module";

if (import.meta.hot) {
  import.meta.hot.accept();
}

document.documentElement.dataset.ddExample = "vite-hono";
document.documentElement.dataset.client = marker;

type ViewTransitionDocument = Document & {
  startViewTransition?: (callback: () => void | Promise<void>) => {
    finished: Promise<void>;
  };
};

let activeLiveSocket: WebSocket | null = null;
const prefersReducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");

connectLiveStatus();
installProductNavigation();

function connectLiveStatus() {
  activeLiveSocket?.close();
  activeLiveSocket = null;

  const liveStatus = document.querySelector("[data-storefront-live-status]");
  if (!liveStatus || typeof WebSocket === "undefined") {
    return;
  }
  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  const socket = new WebSocket(`${protocol}//${location.host}/live`);
  activeLiveSocket = socket;

  socket.addEventListener("open", () => {
    liveStatus.textContent = "socket open";
    socket.send(JSON.stringify({ type: "hello", path: location.pathname }));
  });

  socket.addEventListener("message", (event) => {
    try {
      const payload = JSON.parse(String(event.data));
      liveStatus.textContent = payload.message || "socket message";
    } catch {
      liveStatus.textContent = "socket message";
    }
  });

  socket.addEventListener("close", () => {
    liveStatus.textContent = "socket closed";
  });

  socket.addEventListener("error", () => {
    liveStatus.textContent = "socket error";
  });
}

function installProductNavigation() {
  document.addEventListener("click", (event) => {
    if (
      event.defaultPrevented ||
      event.button !== 0 ||
      event.metaKey ||
      event.altKey ||
      event.ctrlKey ||
      event.shiftKey
    ) {
      return;
    }

    const target = event.target instanceof Element ? event.target : null;
    const link = target?.closest<HTMLAnchorElement>("a[data-transition-link]");
    if (!link || link.target || link.hasAttribute("download")) {
      return;
    }

    const url = new URL(link.href);
    if (url.origin !== location.origin || url.href === location.href) {
      return;
    }

    event.preventDefault();
    void navigateWithTransition(url, {
      productSlug: link.dataset.productSlug,
      push: true,
    });
  });

  window.addEventListener("popstate", () => {
    void navigateWithTransition(new URL(location.href), { push: false });
  });
}

async function navigateWithTransition(
  url: URL,
  options: { productSlug?: string; push: boolean },
) {
  const productSlug =
    options.productSlug ?? productSlugFromLocation(new URL(location.href)) ?? productSlugFromLocation(url);
  let nextDocument: Document;

  try {
    nextDocument = await fetchDocument(url);
  } catch {
    location.href = url.href;
    return;
  }

  if (prefersReducedMotion.matches) {
    applyDocument(nextDocument, url, options.push);
    return;
  }

  const startViewTransition = (document as ViewTransitionDocument).startViewTransition;
  if (startViewTransition) {
    const transition = startViewTransition.call(document, () => {
      applyDocument(nextDocument, url, options.push);
    });
    await transition.finished.catch(() => undefined);
    return;
  }

  await applyDocumentWithFallbackTransition(nextDocument, url, options.push, productSlug);
}

async function fetchDocument(url: URL): Promise<Document> {
  const response = await fetch(url, {
    headers: { accept: "text/html" },
  });
  if (!response.ok) {
    throw new Error(`Failed to load ${url.pathname}: ${response.status}`);
  }
  const text = await response.text();
  return new DOMParser().parseFromString(text, "text/html");
}

function applyDocument(nextDocument: Document, url: URL, push: boolean) {
  const currentApp = document.querySelector(".app");
  const nextApp = nextDocument.querySelector(".app");
  if (!currentApp || !nextApp) {
    location.href = url.href;
    return;
  }

  document.title = nextDocument.title;
  currentApp.replaceWith(nextApp);
  window.scrollTo(0, 0);
  if (push) {
    history.pushState({}, "", url);
  }
  document.documentElement.dataset.client = marker;
  document.querySelector(".app")?.dispatchEvent(new CustomEvent("fx:process", {
    bubbles: true,
    composed: true,
  }));
  connectLiveStatus();
}

async function applyDocumentWithFallbackTransition(
  nextDocument: Document,
  url: URL,
  push: boolean,
  productSlug?: string,
) {
  const sourceImage = productSlug
    ? document.querySelector<HTMLImageElement>(productImageSelector(productSlug))
    : null;
  const sourceRect = sourceImage?.getBoundingClientRect();
  const clone = sourceImage && sourceRect
    ? sourceImage.cloneNode(false) as HTMLImageElement
    : null;

  if (sourceImage && clone && sourceRect) {
    Object.assign(clone.style, {
      borderRadius: getComputedStyle(sourceImage).borderRadius || "8px",
      height: `${sourceRect.height}px`,
      left: `${sourceRect.left}px`,
      margin: "0",
      objectFit: "cover",
      pointerEvents: "none",
      position: "fixed",
      top: `${sourceRect.top}px`,
      width: `${sourceRect.width}px`,
      zIndex: "9999",
    });
    document.body.append(clone);
  }

  applyDocument(nextDocument, url, push);

  const targetImage = productSlug
    ? document.querySelector<HTMLImageElement>(productImageSelector(productSlug))
    : null;
  if (!clone || !sourceRect || !targetImage) {
    await animateAppIn();
    clone?.remove();
    return;
  }

  const targetRect = targetImage.getBoundingClientRect();
  targetImage.style.visibility = "hidden";
  await Promise.all([
    animateAppIn(),
    clone.animate([
      {
        borderRadius: clone.style.borderRadius,
        height: `${sourceRect.height}px`,
        left: `${sourceRect.left}px`,
        top: `${sourceRect.top}px`,
        width: `${sourceRect.width}px`,
      },
      {
        borderRadius: getComputedStyle(targetImage).borderRadius || "8px",
        height: `${targetRect.height}px`,
        left: `${targetRect.left}px`,
        top: `${targetRect.top}px`,
        width: `${targetRect.width}px`,
      },
    ], {
      duration: 520,
      easing: "cubic-bezier(0.22, 1, 0.36, 1)",
    }).finished.catch(() => undefined),
  ]);
  targetImage.style.visibility = "";
  clone.remove();
}

async function animateAppIn() {
  const app = document.querySelector(".app");
  await app?.animate([
    { opacity: 0.92, transform: "translateY(8px)" },
    { opacity: 1, transform: "translateY(0)" },
  ], {
    duration: 220,
    easing: "cubic-bezier(0.22, 1, 0.36, 1)",
  }).finished.catch(() => undefined);
}

function productSlugFromLocation(url: URL): string | undefined {
  const match = url.pathname.match(/^\/notes\/([^/]+)$/);
  return match ? decodeURIComponent(match[1]) : undefined;
}

function productImageSelector(slug: string): string {
  return `[data-product-image="${slug.replace(/["\\]/g, "\\$&")}"]`;
}
