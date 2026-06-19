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
    !appText.includes("vite-ruwuter") ||
    (!appText.includes("Ruwuter on dd") && !appText.includes(">runtime<")) ||
    !appText.includes('data-path="/notes/runtime"')
  ) {
    throw new Error(`app request did not hit ruwuter worker: ${appText}`);
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
  if (!moduleText.includes("vite-ruwuter-client-module") || moduleText.includes("Ruwuter on dd")) {
    throw new Error(`Vite module request did not bypass worker: ${moduleText}`);
  }

  const cssResponse = await fetch(`${base}/src/tailwind.css`);
  const cssText = await cssResponse.text();
  if (!cssText.includes(".dd-stm-badge") || !cssText.includes("tailwindcss")) {
    throw new Error(`Tailwind CSS request did not render through the Vite plugin: ${cssText}`);
  }

  const viteClientResponse = await fetch(`${base}/@vite/client`);
  const viteClientText = await viteClientResponse.text();
  if (!viteClientText.includes("createHotContext") || viteClientText.includes("Ruwuter on dd")) {
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
