import { spawn } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { build, mergeConfig, normalizePath } from "vite";

const packageDirectory = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const runtimePath = resolve(packageDirectory, "src/worker-runtime.js");
export async function buildJavyWorker(entry, output, options = {}) {
  const entryPath = resolve(entry);
  const outputPath = resolve(output);
  const temporaryDirectory = await mkdtemp(resolve(tmpdir(), "dd-javy-build-"));
  const bundledPath = resolve(temporaryDirectory, "worker.mjs");
  try {
    const bundledSource = await bundleJavyWorker(entryPath, options);
    await writeFile(bundledPath, bundledSource);
    await mkdir(dirname(outputPath), { recursive: true });
    await compileJavyModule(bundledPath, outputPath, options);
    return {
      output: outputPath,
      bytes: (await readFile(outputPath)).byteLength,
    };
  } finally {
    await rm(temporaryDirectory, { recursive: true, force: true });
  }
}

export async function bundleJavyWorker(entry, options = {}) {
  const entryPath = normalizePath(resolve(entry));
  const workerRuntimePath = normalizePath(runtimePath);
  const temporaryDirectory = await mkdtemp(resolve(tmpdir(), "dd-javy-entry-"));
  const wrapperPath = resolve(temporaryDirectory, "entry.mjs");
  await writeFile(
    wrapperPath,
    [
      `import { runWorker } from ${JSON.stringify(workerRuntimePath)};`,
      `import worker from ${JSON.stringify(entryPath)};`,
      "await runWorker(worker);",
    ].join("\n"),
  );
  try {
    const baseConfig = {
      configFile: false,
      envFile: true,
      logLevel: options.logLevel ?? "warn",
      mode: options.mode ?? "production",
      resolve: {
        conditions: ["workerd", "worker", "browser"],
      },
      esbuild: {
        jsxDev: false,
      },
      build: {
        write: false,
        target: "es2023",
        sourcemap: options.sourcemap ?? false,
        minify: options.minify ?? true,
        emptyOutDir: false,
        rollupOptions: {
          input: wrapperPath,
          output: {
            format: "es",
            codeSplitting: false,
            entryFileNames: "worker.mjs",
          },
        },
      },
    };
    const output = await build(mergeConfig(baseConfig, options.viteConfig ?? {}));
    const outputs = Array.isArray(output) ? output : [output];
    for (const rollupOutput of outputs) {
      for (const chunk of rollupOutput.output) {
        if (chunk.type === "chunk" && chunk.isEntry) {
          return chunk.code;
        }
      }
    }
    throw new Error(`Vite did not produce a Javy entry chunk for ${entryPath}`);
  } finally {
    await rm(temporaryDirectory, { recursive: true, force: true });
  }
}

async function compileJavyModule(input, output, options) {
  const configuredExecutable = options.javy ?? process.env.JAVY_BIN ?? "javy";
  const executable =
    configuredExecutable.includes("/") || configuredExecutable.includes("\\")
      ? resolve(
          options.cwd ?? process.env.INIT_CWD ?? process.cwd(),
          configuredExecutable,
        )
      : configuredExecutable;
  const configuredPlugin = options.plugin ?? process.env.JAVY_PLUGIN;
  const args = ["build", input, "-o", output];
  if (configuredPlugin) {
    const plugin = resolve(
      options.cwd ?? process.env.INIT_CWD ?? process.cwd(),
      configuredPlugin,
    );
    args.push("-C", `plugin=${plugin}`);
  } else {
    args.push(
      "-J",
      "javy-stream-io=y",
      "-J",
      "text-encoding=y",
      "-J",
      "event-loop=y",
    );
  }
  args.push("-C", "source=compressed");
  await new Promise((accept, reject) => {
    const child = spawn(executable, args, {
      cwd: options.cwd ?? process.cwd(),
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.on("error", (error) => {
      reject(
        new Error(
          `could not start Javy executable ${JSON.stringify(executable)}: ${error.message}`,
          { cause: error },
        ),
      );
    });
    child.on("exit", (code, signal) => {
      if (code === 0) {
        accept();
        return;
      }
      reject(
        new Error(
          `Javy failed for ${pathToFileURL(input)} (${signal ?? `exit ${code}`}):\n` +
            `${stderr || stdout || "<no compiler output>"}`,
        ),
      );
    });
  });
}
