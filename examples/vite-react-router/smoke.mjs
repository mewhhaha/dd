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

  const appResponse = await fetch(`${base}/projects/runtime`);
  const firstStmCount = stmCount(appResponse, "first app request");
  const appText = await appResponse.text();
  if (
    !appText.includes("vite-react-router") ||
    !appText.includes("React Router framework server build inside the dd worker") ||
    !appText.includes('data-path="/projects/runtime"') ||
    !appText.includes(`data-stm-count="${firstStmCount}"`)
  ) {
    throw new Error(`app request did not hit React Router worker: ${appText}`);
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
    !moduleText.includes("vite-react-router") ||
    moduleText.includes("<html")
  ) {
    throw new Error(`Vite module request did not bypass worker: ${moduleText}`);
  }

  const cssResponse = await fetch(`${base}/app/tailwind.css`);
  const cssText = await cssResponse.text();
  if (!cssText.includes(".dd-stm-badge") || !cssText.includes("tailwindcss")) {
    throw new Error(`Tailwind CSS request did not render through the Vite plugin: ${cssText}`);
  }

  const viteClientResponse = await fetch(`${base}/@vite/client`);
  const viteClientText = await viteClientResponse.text();
  if (!viteClientText.includes("createHotContext") || viteClientText.includes("React Router on dd")) {
    throw new Error("Vite client request did not bypass worker");
  }
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
