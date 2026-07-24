import { readFile, writeFile } from "node:fs/promises";
import { createServer } from "vite";

const HOME_ROUTE = new URL("./app/routes/home.tsx", import.meta.url);

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

  const appResponse = await fetch(`${base}/projects/runtime`);
  const firstStmCount = stmCount(appResponse, "first app request");
  const appText = await appResponse.text();
  if (
    !appText.includes("Edge Goods") ||
    !appText.includes("Runtime valley print") ||
    !appText.includes("Timer tick") ||
    !appText.includes("React Router framework server build inside the dd worker") ||
    !appText.includes('data-path="/projects/runtime"') ||
    !appText.includes(`data-stm-count="${firstStmCount}"`)
  ) {
    throw new Error(`app request did not hit React Router worker: ${appText}`);
  }
  if (appText.includes("new WebSocket") || appText.includes("socket.addEventListener")) {
    throw new Error("React Router shell includes a pre-hydration live socket script");
  }
  const cookie = sessionCookie(appResponse);

  const cartResponse = await fetch(`${base}/projects/runtime`, {
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded",
      cookie,
      origin: base,
    },
    body: new URLSearchParams({
      intent: "add-to-cart",
      slug: "runtime",
      quantity: "2",
    }),
  });
  const cartText = await cartResponse.text();
  const normalizedCartText = stripReactComments(cartText);
  if (!normalizedCartText.includes("2 items selected") || !cartText.includes("$128.00")) {
    throw new Error(`cart mutation did not persist through dd memory: ${cartText}`);
  }

  const checkoutResponse = await fetch(`${base}/?index`, {
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded",
      cookie,
      origin: base,
    },
    body: new URLSearchParams({
      intent: "checkout",
      email: "smoke@example.test",
    }),
  });
  const checkoutText = await checkoutResponse.text();
  if (!checkoutText.includes("was written to KV") || !checkoutText.includes("Orders stored")) {
    throw new Error(`checkout did not write an order row: ${checkoutText}`);
  }

  const secondAppResponse = await fetch(`${base}/projects/runtime?dd-stm-smoke=2`);
  const secondStmCount = stmCount(secondAppResponse, "second app request");
  if (secondStmCount <= firstStmCount) {
    throw new Error(`dd STM counter did not advance: ${firstStmCount} -> ${secondStmCount}`);
  }
  const secondAppText = await secondAppResponse.text();
  if (!secondAppText.includes(`data-stm-count="${secondStmCount}"`)) {
    throw new Error(`route loader did not render the second STM count: ${secondAppText}`);
  }

  const moduleResponse = await fetch(`${base}/app/root.tsx`, {
    headers: { "sec-fetch-dest": "script" },
  });
  const moduleText = await moduleResponse.text();
  if (
    !moduleText.includes("Edge Goods") ||
    moduleText.includes("<html")
  ) {
    throw new Error(`Vite module request did not bypass worker: ${moduleText}`);
  }

  const cssResponse = await fetch(`${base}/app/tailwind.css`);
  const cssText = await cssResponse.text();
  if (!cssText.includes("tailwindcss") || !cssText.includes("font-sans")) {
    throw new Error(`Tailwind CSS request did not render through the Vite plugin: ${cssText}`);
  }

  const viteClientResponse = await fetch(`${base}/@vite/client`);
  const viteClientText = await viteClientResponse.text();
  if (!viteClientText.includes("createHotContext") || viteClientText.includes("Edge Goods")) {
    throw new Error("Vite client request did not bypass worker");
  }

  await assertLiveSocket(base, "vite-react-router");
  await assertLiveSocket(localhostBase(base), "vite-react-router");
  await assertReactRouterHotUpdate(base, viteClientText);
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
    throw new Error("app response did not set storefront session cookie");
  }
  return raw.split(";")[0];
}

function stripReactComments(text) {
  return text.replace(/<!--\s*-->/g, "");
}

function assertLiveSocket(base, workerName) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(`${base.replace(/^http/, "ws")}/api/cart/live`);
    const timeout = setTimeout(() => {
      socket.close();
      reject(new Error(`timed out waiting for ${workerName} cart websocket`));
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
      if (payload.message === "smoke" || payload.message === "connected") {
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

async function assertReactRouterHotUpdate(base, viteClientText) {
  const original = await readFile(HOME_ROUTE, "utf8");
  if (!original.includes('data-route="home"')) {
    throw new Error("React Router HMR smoke could not find the home route probe marker");
  }
  const next = original.replace('data-route="home"', 'data-route="home-smoke"');
  const token = viteClientText.match(/\bwsToken\s*=\s*"([^"]+)"/)?.[1];
  const socketUrl = new URL("/", base);
  if (token) {
    socketUrl.searchParams.set("token", token);
  }
  socketUrl.protocol = socketUrl.protocol === "https:" ? "wss:" : "ws:";

  let restored = false;
  const restore = async () => {
    if (!restored) {
      restored = true;
      await writeFile(HOME_ROUTE, original);
    }
  };

  await new Promise((resolve, reject) => {
    const socket = new WebSocket(String(socketUrl), "vite-hmr");
    const timeout = setTimeout(() => {
      void restore().finally(() => {
        socket.close();
        reject(new Error("timed out waiting for React Router HMR update payload"));
      });
    }, 8_000);

    socket.addEventListener("open", () => {
      void writeFile(HOME_ROUTE, next).catch(async (error) => {
        clearTimeout(timeout);
        await restore();
        reject(error);
      });
    });

    socket.addEventListener("message", (event) => {
      const text = String(event.data);
      if (!text.includes('"event":"react-router:hmr"') || !text.includes("app/routes/home.tsx")) {
        return;
      }
      clearTimeout(timeout);
      void fetch(`${base}/`).then(async (response) => {
        const updatedText = await response.text();
        if (!updatedText.includes('data-route="home-smoke"')) {
          throw new Error("React Router route did not serve the edited home component after HMR");
        }
      }).then(() => restore()).then(() => {
        socket.close();
        resolve();
      }, reject);
    });

    socket.addEventListener("error", () => {
      clearTimeout(timeout);
      void restore().finally(() => {
        reject(new Error("failed to connect to Vite HMR websocket"));
      });
    });
  });
}
