import { playwright, defineBrowserCommand } from "@vitest/browser-playwright";
import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const require = createRequire(import.meta.url);
const exampleDir = fileURLToPath(new URL(".", import.meta.url));
const viteBin = resolve(dirname(require.resolve("vite/package.json")), "bin/vite.js");

const runReactRouterAddToCartFlows = defineBrowserCommand(async ({ context }) => {
  const appServer = await startAppServer();
  const appPage = await context.newPage();
  const consoleErrors: string[] = [];
  const pageErrors: string[] = [];
  const manifestStatuses: number[] = [];

  appPage.on("console", (message) => {
    if (message.type() === "error") {
      consoleErrors.push(message.text());
    }
  });
  appPage.on("pageerror", (error) => {
    pageErrors.push(error.message);
  });
  appPage.on("response", (response) => {
    if (response.url().includes("/__manifest")) {
      manifestStatuses.push(response.status());
    }
  });

  try {
    await appPage.setViewportSize({ width: 390, height: 720 });

    await appPage.goto(appServer.base, { waitUntil: "domcontentloaded" });
    await appPage.getByRole("heading", { name: "Edge Goods" }).waitFor();
    await waitForImages(appPage);
    const homeAddButton = appPage.getByTestId("home-add-runtime");
    await homeAddButton.scrollIntoViewIfNeeded();
    const homeScrollBeforeAdd = await appPage.evaluate(() => window.scrollY);
    await homeAddButton.click();
    await appPage.getByTestId("home-cart-count").getByText("1 item in memory.").waitFor();
    const homeScrollAfterAdd = await appPage.evaluate(() => window.scrollY);
    const homeUrl = appPage.url();

    await appPage.goto(`${appServer.base}/projects/runtime`, { waitUntil: "domcontentloaded" });
    await appPage.getByRole("heading", { name: "Runtime valley print" }).waitFor();
    await waitForImages(appPage);
    const detailAddButton = appPage.getByTestId("detail-add-to-cart");
    await detailAddButton.scrollIntoViewIfNeeded();
    const detailScrollBeforeAdd = await appPage.evaluate(() => window.scrollY);
    const detailButtonTopBeforeAdd = await elementTop(detailAddButton);
    await detailAddButton.click();
    await appPage.getByTestId("detail-cart-count").getByText("2 items selected.").waitFor();
    const detailScrollAfterAdd = await appPage.evaluate(() => window.scrollY);
    const detailButtonTopAfterAdd = await elementTop(detailAddButton);

    return {
      bodyText: await appPage.locator("body").innerText(),
      consoleErrors,
      detailButtonTopAfterAdd,
      detailButtonTopBeforeAdd,
      detailScrollAfterAdd,
      detailScrollBeforeAdd,
      detailUrl: appPage.url(),
      homeScrollAfterAdd,
      homeScrollBeforeAdd,
      homeUrl,
      manifestStatuses,
      pageErrors,
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

  await new Promise<void>((resolvePromise, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`timed out waiting for React Router app server:\n${server.output}`));
    }, 30_000);

    const onOutput = (chunk: Buffer) => {
      server.output += chunk.toString();
      const match = server.output.match(/https?:\/\/127\.0\.0\.1:(\d+)\//);
      if (match) {
        server.base = `http://127.0.0.1:${match[1]}`;
        cleanup();
        resolvePromise();
      }
    };
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      cleanup();
      reject(new Error(`React Router app server exited before listening (${signal ?? code}):\n${server.output}`));
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
  const exited = new Promise<void>((resolvePromise) => {
    server.child.once("exit", () => resolvePromise());
  });
  server.child.kill("SIGTERM");
  await Promise.race([exited, delay(2_000)]);
  if (server.child.exitCode === null && server.child.signalCode === null) {
    server.child.kill("SIGKILL");
    await Promise.race([exited, delay(2_000)]);
  }
}

function delay(ms: number) {
  return new Promise((resolvePromise) => {
    const timeout = setTimeout(resolvePromise, ms);
    timeout.unref?.();
  });
}

async function elementTop(locator: { evaluate<T>(callback: (element: Element) => T): Promise<T> }) {
  return await locator.evaluate((element) => element.getBoundingClientRect().top);
}

async function waitForImages(page: { evaluate<T>(callback: () => T | Promise<T>): Promise<T> }) {
  await page.evaluate(async () => {
    await Promise.all(
      Array.from(document.images, (image) => {
        if (image.complete) {
          return undefined;
        }
        return new Promise<void>((resolvePromise) => {
          image.addEventListener("load", () => resolvePromise(), { once: true });
          image.addEventListener("error", () => resolvePromise(), { once: true });
        });
      }),
    );
  });
}

export default defineConfig({
  test: {
    browser: {
      commands: {
        runReactRouterAddToCartFlows,
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
