import "./tailwind.css";

export const marker = "vite-hono-client-module";

if (import.meta.hot) {
  import.meta.hot.accept();
}

document.documentElement.dataset.ddExample = "vite-hono";

document.documentElement.dataset.client = marker;

const liveStatus = document.querySelector("[data-storefront-live-status]");

if (liveStatus && typeof WebSocket !== "undefined") {
  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  const socket = new WebSocket(`${protocol}//${location.host}/live`);

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
