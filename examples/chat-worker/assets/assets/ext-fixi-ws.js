(()=>{
  if (document.__fixi_ws_plugin) return;
  document.__fixi_ws_plugin = true;

  const roots = new WeakSet();
  const senders = new WeakSet();
  const transition = document.startViewTransition?.bind(document);

  function setStatus(root, status) {
    root.dataset.wsState = status;
    const selector = root.getAttribute("ext-fx-ws-status");
    if (!selector) return;
    const target = document.querySelector(selector);
    if (target) target.textContent = status;
  }

  function resolveSocketUrl(value) {
    const url = new URL(value, window.location.href);
    if (url.protocol === "http:") url.protocol = "ws:";
    if (url.protocol === "https:") url.protocol = "wss:";
    return url.toString();
  }

  function applySwap(target, swap, text) {
    if (/(before|after)(begin|end)/.test(swap)) target.insertAdjacentHTML(swap, text);
    else if (swap in target) target[swap] = text;
    else if (swap !== "none") throw new Error("unsupported swap " + swap);
  }

  async function applyPayload(payload) {
    const swaps = Array.isArray(payload) ? payload : [payload];
    for (const swap of swaps) {
      if (!swap || typeof swap !== "object" || typeof swap.target !== "string") continue;
      const mode = typeof swap.swap === "string" ? swap.swap : "innerHTML";
      const text = typeof swap.text === "string" ? swap.text : "";
      for (const target of document.querySelectorAll(swap.target)) {
        if (transition) await transition(() => applySwap(target, mode, text)).finished;
        else applySwap(target, mode, text);
      }
    }
  }

  function initRoot(root) {
    if (roots.has(root)) return;
    roots.add(root);
    const connectValue = root.getAttribute("ext-fx-ws-connect");
    if (!connectValue) return;
    const openValue = root.getAttribute("ext-fx-ws-open");
    const shouldReconnect = root.getAttribute("ext-fx-ws-reconnect") !== "false";
    let reconnectDelayMs = 500;
    let reconnectTimer = 0;
    let generation = 0;

    const clearReconnectTimer = () => {
      if (reconnectTimer) {
        window.clearTimeout(reconnectTimer);
        reconnectTimer = 0;
      }
    };

    const connect = () => {
      clearReconnectTimer();
      const socketGeneration = ++generation;
      setStatus(root, "connecting");
      const socket = new WebSocket(resolveSocketUrl(connectValue));
      root.__fixiWs = socket;
      socket.addEventListener("open", () => {
        if (root.__fixiWs !== socket || socketGeneration !== generation) return;
        reconnectDelayMs = 500;
        setStatus(root, "open");
        if (!openValue) return;
        try {
          socket.send(openValue);
        } catch {
          setStatus(root, "error");
        }
      });
      socket.addEventListener("message", async (event) => {
        if (root.__fixiWs !== socket || socketGeneration !== generation) return;
        try {
          await applyPayload(JSON.parse(event.data));
          document.dispatchEvent(new CustomEvent("fx:process", { bubbles: true }));
        } catch {
          setStatus(root, "error");
        }
      });
      socket.addEventListener("error", () => {
        if (root.__fixiWs !== socket || socketGeneration !== generation) return;
        setStatus(root, "error");
      });
      socket.addEventListener("close", () => {
        if (root.__fixiWs !== socket || socketGeneration !== generation) return;
        setStatus(root, "closed");
        if (!shouldReconnect) return;
        reconnectTimer = window.setTimeout(() => {
          if (socketGeneration !== generation) return;
          connect();
        }, reconnectDelayMs);
        reconnectDelayMs = Math.min(reconnectDelayMs * 2, 5000);
      });
    };

    const teardown = () => {
      clearReconnectTimer();
      const socket = root.__fixiWs;
      if (!socket) return;
      root.__fixiWs = null;
      generation += 1;
      try {
        socket.close(1001, "page-hidden");
      } catch {
        // Ignore teardown races on unload.
      }
    };

    window.addEventListener("pagehide", teardown, { once: true });
    window.addEventListener("beforeunload", teardown, { once: true });

    connect();
  }

  function initSender(element) {
    if (!(element instanceof HTMLFormElement) || senders.has(element)) return;
    senders.add(element);
    element.addEventListener("submit", (event) => {
      event.preventDefault();
      const root = element.closest("[ext-fx-ws-connect]");
      const socket = root?.__fixiWs;
      if (!root || !socket || socket.readyState !== WebSocket.OPEN) {
        if (root) setStatus(root, "waiting");
        return;
      }
      const payload = Object.fromEntries(new FormData(element, event.submitter).entries());
      socket.send(JSON.stringify(payload));
      if (element.matches("[ext-fx-ws-reset]")) element.reset();
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll("[ext-fx-ws-connect]").forEach(initRoot);
    document.querySelectorAll("form[ext-fx-ws-send]").forEach(initSender);
  });

  document.addEventListener("fx:process", (event) => {
    const target = event.target;
    if (target instanceof Element && target.matches("[ext-fx-ws-connect]")) initRoot(target);
    if (target?.querySelectorAll) target.querySelectorAll("[ext-fx-ws-connect]").forEach(initRoot);
    if (target instanceof Element && target.matches("form[ext-fx-ws-send]")) initSender(target);
    if (target?.querySelectorAll) target.querySelectorAll("form[ext-fx-ws-send]").forEach(initSender);
  });
})();
