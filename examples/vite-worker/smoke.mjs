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

  const appResponse = await fetch(`${base}/hello`);
  const appText = await appResponse.text();
  if (!appText.includes("worker-root") || !appText.includes('data-path="/hello"')) {
    throw new Error(`root request did not hit worker: ${appText}`);
  }

  const moduleResponse = await fetch(`${base}/src/client.ts`, {
    headers: { "sec-fetch-dest": "script" },
  });
  const moduleText = await moduleResponse.text();
  if (!moduleText.includes("vite-client-module") || moduleText.includes("worker-root")) {
    throw new Error(`Vite module request did not bypass worker: ${moduleText}`);
  }

  const viteClientResponse = await fetch(`${base}/@vite/client`);
  const viteClientText = await viteClientResponse.text();
  if (!viteClientText.includes("createHotContext") || viteClientText.includes("worker-root")) {
    throw new Error("Vite client request did not bypass worker");
  }
} finally {
  await server.close();
}
