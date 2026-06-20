import { createServer } from "vite";

const server = await createServer({
  configFile: new URL("./vite.config.ts", import.meta.url).pathname,
  server: {
    host: "127.0.0.1",
    port: 0,
  },
});

try {
  await server.listen();
  const { port } = server.httpServer.address();
  const base = `http://127.0.0.1:${port}`;

  const appResponse = await fetch(`${base}/notes/runtime`);
  const firstStmCount = stmCount(appResponse, "first app request");
  const appText = await appResponse.text();
  if (
    !appText.includes("Edge Goods Hono") ||
    !appText.includes("Runtime valley print") ||
    !appText.includes("Timer tick") ||
    !appText.includes('data-path="/notes/runtime"')
  ) {
    throw new Error(`app request did not hit Hono worker: ${appText}`);
  }
  const cookie = sessionCookie(appResponse);

  const cartResponse = await fetch(`${base}/notes/runtime`, {
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded",
      cookie,
      "fx-request": "true",
    },
    body: new URLSearchParams({
      intent: "add-to-cart",
      slug: "runtime",
      quantity: "2",
    }),
  });
  const cartText = await cartResponse.text();
  const normalizedCartText = stripRendererComments(cartText);
  if (
    cartResponse.redirected ||
    !normalizedCartText.includes('id="storefront-cart"') ||
    !normalizedCartText.includes("2 items in memory") ||
    !cartText.includes("$128.00") ||
    cartText.includes("<!doctype html>")
  ) {
    throw new Error(`Hono fixi cart mutation did not return a cart fragment: ${cartText}`);
  }

  const checkoutResponse = await fetch(`${base}/`, {
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded",
      cookie,
    },
    body: new URLSearchParams({
      intent: "checkout",
      email: "smoke@example.test",
    }),
  });
  const checkoutText = await checkoutResponse.text();
  if (!checkoutText.includes("was written to KV") || !checkoutText.includes("Orders stored")) {
    throw new Error(`Hono checkout did not write an order row: ${checkoutText}`);
  }

  const secondAppResponse = await fetch(`${base}/notes/runtime?dd-stm-smoke=2`);
  const secondStmCount = stmCount(secondAppResponse, "second app request");
  if (secondStmCount <= firstStmCount) {
    throw new Error(`dd STM counter did not advance: ${firstStmCount} -> ${secondStmCount}`);
  }
  await secondAppResponse.arrayBuffer();

  const moduleResponse = await fetch(`${base}/src/client.ts`, {
    headers: { "sec-fetch-dest": "script" },
  });
  const moduleText = await moduleResponse.text();
  if (!moduleText.includes("vite-hono-client-module") || moduleText.includes("Edge Goods Hono")) {
    throw new Error(`Vite module request did not bypass worker: ${moduleText}`);
  }

  const cssResponse = await fetch(`${base}/src/tailwind.css`);
  const cssText = await cssResponse.text();
  if (!cssText.includes("tailwindcss") || !cssText.includes("font-sans")) {
    throw new Error(`Tailwind CSS request did not render through the Vite plugin: ${cssText}`);
  }

  const viteClientResponse = await fetch(`${base}/@vite/client`);
  const viteClientText = await viteClientResponse.text();
  if (!viteClientText.includes("createHotContext") || viteClientText.includes("Edge Goods Hono")) {
    throw new Error("Vite client request did not bypass worker");
  }
  if (!moduleText.includes("fixi-js")) {
    throw new Error(`Hono client module did not import fixi-js: ${moduleText}`);
  }

  await assertLiveSocket(base, "vite-hono");
  await assertLiveSocket(localhostBase(base), "vite-hono");
} finally {
  await server.close();
}

function stmCount(response, label) {
  const raw = response.headers.get("x-dd-stm-count");
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error(`${label} did not expose a valid dd STM count: ${raw}`);
  }
  return value;
}

function sessionCookie(response) {
  const raw = response.headers.get("set-cookie");
  if (!raw) {
    throw new Error("Hono response did not set storefront session cookie");
  }
  return raw.split(";")[0];
}

function stripRendererComments(text) {
  return text.replace(/<!--\s*-->/g, "");
}

function assertLiveSocket(base, workerName) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(`${base.replace(/^http/, "ws")}/live`);
    const timeout = setTimeout(() => {
      socket.close();
      reject(new Error(`timed out waiting for ${workerName} websocket echo`));
    }, 5_000);

    socket.addEventListener("open", () => {
      socket.send("smoke");
    });
    socket.addEventListener("message", (event) => {
      const payload = JSON.parse(String(event.data));
      if (payload.worker !== workerName) {
        reject(new Error(`unexpected websocket worker: ${event.data}`));
        return;
      }
      if (payload.message === "smoke") {
        clearTimeout(timeout);
        socket.close();
        resolve();
      }
    });
    socket.addEventListener("error", () => {
      clearTimeout(timeout);
      reject(new Error(`failed to connect ${workerName} websocket`));
    });
  });
}

function localhostBase(base) {
  return base.replace("127.0.0.1", "localhost");
}
