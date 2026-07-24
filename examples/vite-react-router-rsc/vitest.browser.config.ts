import { playwright, defineBrowserCommand } from "@vitest/browser-playwright";
import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const require = createRequire(import.meta.url);
const exampleDir = fileURLToPath(new URL(".", import.meta.url));
const viteBin = resolve(dirname(require.resolve("vite/package.json")), "bin/vite.js");

const runRscAddToCartFlow = defineBrowserCommand(async ({ context }) => {
  const appServer = await startAppServer();
  const appPage = await context.newPage();
  const consoleErrors: string[] = [];
  const pageErrors: string[] = [];

  appPage.on("console", (message) => {
    if (message.type() === "error") {
      consoleErrors.push(message.text());
    }
  });
  appPage.on("pageerror", (error) => {
    pageErrors.push(error.message);
  });

  try {
    await appPage.goto(`${appServer.base}/projects/runtime`, { waitUntil: "domcontentloaded" });
    await appPage.getByRole("heading", { name: "Runtime valley print" }).waitFor();
    await appPage.getByTestId("detail-add-to-cart").click();
    await appPage.getByTestId("detail-cart-count").waitFor({ state: "visible" });
    await appPage.getByText("1 item selected.", { exact: true }).waitFor();

    return {
      bodyText: await appPage.locator("body").innerText(),
      consoleErrors,
      pageErrors,
      url: appPage.url(),
    };
  } finally {
    await appPage.close();
    await stopAppServer(appServer);
  }
});

type AppServer = {
  base: string;
  child: ReturnType<typeof spawn>;
  output: string;
};

async function startAppServer(): Promise<AppServer> {
  const child = spawn(
    process.execPath,
    [viteBin, "--host", "127.0.0.1", "--port", "0"],
    {
      cwd: exampleDir,
      env: {
        ...process.env,
        FORCE_COLOR: "0",
        NO_COLOR: "1",
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  const server: AppServer = { base: "", child, output: "" };

  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`timed out waiting for RSC app server:\n${server.output}`));
    }, 30_000);

    const onOutput = (chunk: Buffer) => {
      server.output += chunk.toString();
      const match = server.output.match(/https?:\/\/127\.0\.0\.1:(\d+)\//);
      if (match) {
        server.base = `http://127.0.0.1:${match[1]}`;
        cleanup();
        resolve();
      }
    };
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      cleanup();
      reject(new Error(`RSC app server exited before listening (${signal ?? code}):\n${server.output}`));
    };
    const cleanup = () => {
      clearTimeout(timeout);
      child.stdout.off("data", onOutput);
      child.stderr.off("data", onOutput);
      child.off("exit", onExit);
      child.off("error", reject);
    };

    child.stdout.on("data", onOutput);
    child.stderr.on("data", onOutput);
    child.once("exit", onExit);
    child.once("error", reject);
  });

  return server;
}

async function stopAppServer(server: AppServer) {
  if (server.child.exitCode !== null || server.child.signalCode !== null) {
    return;
  }
  const exited = new Promise<void>((resolve) => {
    server.child.once("exit", () => resolve());
  });
  server.child.kill("SIGTERM");
  await Promise.race([exited, delay(2_000)]);
  if (server.child.exitCode === null && server.child.signalCode === null) {
    server.child.kill("SIGKILL");
    await Promise.race([exited, delay(2_000)]);
  }
}

function delay(ms: number) {
  return new Promise((resolve) => {
    const timeout = setTimeout(resolve, ms);
    timeout.unref?.();
  });
}

export default defineConfig({
  test: {
    browser: {
      commands: {
        runRscAddToCartFlow,
      },
      enabled: true,
      headless: true,
      instances: [{ browser: "chromium" }],
      provider: playwright(),
    },
    include: ["browser/**/*.test.ts"],
    testTimeout: 30_000,
  },
});
