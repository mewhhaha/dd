import { bundleWorkerEntry } from "@dd/vite";
import { spawn } from "node:child_process";
import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { extname, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("./", import.meta.url));
const distDir = resolve(root, "dist");
const workerOutput = resolve(distDir, "worker.js");
const clientAssetsDir = resolve(root, "dist/react-router-rsc/client");
const rscServerEntry = resolve(root, "dist/react-router-rsc/server/index.js");
const rscAssetsManifest = resolve(root, "dist/react-router-rsc/server/__vite_rsc_assets_manifest.js");
const ssrBuildEntry = resolve(root, "dist/react-router-rsc/server/__ssr_build/index.js");
const viteBin = fileURLToPath(new URL("./node_modules/vite/bin/vite.js", import.meta.url));
const workerEntry = new URL("./src/worker.ts", import.meta.url);
const asyncHooksShim = fileURLToPath(new URL("./src/node-async-hooks.ts", import.meta.url));
export const rscClientEntry = fileURLToPath(new URL("./app/entry.client.tsx", import.meta.url));

const workerName = "vite-react-router-rsc";
const deployConfig = {
  name: workerName,
  entrypoint: "worker.js",
  assets_dir: "react-router-rsc/client",
  config: {
    public: true,
    bindings: [
      {
        type: "memory",
        binding: "EXAMPLE_MEMORY",
      },
    ],
  },
};

export async function buildReactRouterRsc() {
  await new Promise((resolveBuild, rejectBuild) => {
    const child = spawn(process.execPath, [viteBin, "build"], {
      cwd: root,
      env: {
        ...process.env,
        DD_RSC_BUILD_ONLY: "1",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let output = "";
    child.stdout.on("data", (chunk) => {
      output += chunk;
    });
    child.stderr.on("data", (chunk) => {
      output += chunk;
    });
    child.on("error", rejectBuild);
    child.on("close", (code) => {
      if (code === 0) {
        resolveBuild();
        return;
      }
      rejectBuild(new Error(`React Router RSC build failed with exit code ${code}\n${output}`));
    });
  });
}

export async function bundleDdRscWorker() {
  return bundleWorkerEntry(workerEntry, {
    viteConfig: {
      define: {
        "process.env.NODE_ENV": JSON.stringify("production"),
      },
      resolve: {
        alias: {
          "virtual:dd-react-router-rsc-server": rscServerEntry,
          "node:async_hooks": asyncHooksShim,
          async_hooks: asyncHooksShim,
        },
      },
    },
  });
}

export async function readDdRscWorker() {
  return readFile(workerOutput, "utf8");
}

export async function writeDdRscBuildArtifacts() {
  await mkdir(distDir, { recursive: true });
  await writeFile(workerOutput, await bundleDdRscWorker());
  await writeFile(resolve(distDir, "dd.deploy.json"), `${JSON.stringify(deployConfig, null, 2)}\n`);
  await mkdir(clientAssetsDir, { recursive: true });
  await writeFile(
    resolve(clientAssetsDir, "_headers"),
    "/assets/*\n  Cache-Control: public, max-age=31536000, immutable\n",
  );
}

export async function hasReactRouterRscServerOutput(sinceMs = 0) {
  try {
    const files = await Promise.all([
      stat(rscServerEntry),
      stat(rscAssetsManifest),
      stat(ssrBuildEntry),
    ]);
    return files.every((file) => file.mtimeMs >= sinceMs);
  } catch {
    return false;
  }
}

export function serveReactRouterRscClientAssets() {
  return {
    name: "dd-react-router-rsc-client-assets",
    enforce: "pre",
    configureServer(server) {
      server.middlewares.use(async (req, res, next) => {
        const method = (req.method ?? "GET").toUpperCase();
        if (method !== "GET" && method !== "HEAD") {
          next();
          return;
        }

        const url = new URL(req.url ?? "/", "http://dd.local");
        if (!url.pathname.startsWith("/assets/")) {
          next();
          return;
        }

        const file = resolve(clientAssetsDir, decodeURIComponent(url.pathname.slice(1)));
        const rel = relative(clientAssetsDir, file);
        if (rel.startsWith("..") || rel === "" || rel.includes(`..${sep}`)) {
          next();
          return;
        }

        try {
          const body = await readFile(file);
          res.statusCode = 200;
          res.setHeader("content-type", contentType(file));
          res.setHeader("cache-control", "no-store");
          res.end(method === "HEAD" ? undefined : body);
        } catch {
          next();
        }
      });
    },
  };
}

function contentType(file) {
  switch (extname(file)) {
    case ".css":
      return "text/css; charset=utf-8";
    case ".js":
      return "text/javascript; charset=utf-8";
    case ".map":
      return "application/json; charset=utf-8";
    default:
      return "application/octet-stream";
  }
}
