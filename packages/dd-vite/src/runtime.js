import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const DEFAULT_WORKER_NAME = "test-worker";
const DEFAULT_TIMEOUT_MS = 30_000;
const BODYLESS_METHODS = new Set(["GET", "HEAD"]);
const require = createRequire(import.meta.url);

export function createDdRuntime(options = {}) {
  return new DdRuntimeClient(options);
}

export class DdRuntimeClient {
  #child;
  #nextId = 1;
  #pending = new Map();
  #stdout = "";
  #stderr = "";
  #closed = false;

  constructor(options = {}) {
    this.options = {
      timeoutMs: DEFAULT_TIMEOUT_MS,
      allowCodeGeneration: true,
      ...options,
    };
  }

  async deploy(name, source, config = {}) {
    const result = await this.request({
      op: "deploy",
      name,
      source,
      config,
    });
    return result;
  }

  async invoke(name, request) {
    const body = request.body_base64 ?? "";
    return this.request({
      op: "invoke",
      name,
      method: request.method ?? "GET",
      url: request.url ?? "http://worker/",
      headers: request.headers ?? [],
      body_base64: body,
      request_id: request.request_id,
    });
  }

  async fetch(nameOrInput, inputOrInit, maybeInit) {
    const hasWorkerName = typeof nameOrInput === "string" && arguments.length > 1;
    const name = hasWorkerName ? nameOrInput : DEFAULT_WORKER_NAME;
    const input = hasWorkerName ? inputOrInit : nameOrInput;
    const init = hasWorkerName ? maybeInit : inputOrInit;
    const request = input instanceof Request ? input : new Request(input, init);
    const body_base64 = BODYLESS_METHODS.has(request.method.toUpperCase())
      ? ""
      : Buffer.from(await request.arrayBuffer()).toString("base64");
    const result = await this.invoke(name, {
      method: request.method,
      url: request.url,
      headers: [...request.headers.entries()],
      body_base64,
    });
    return new Response(Buffer.from(result.body_base64, "base64"), {
      status: result.status,
      headers: result.headers,
    });
  }

  async stats(name) {
    return this.request({ op: "stats", name });
  }

  async request(command) {
    if (this.#closed) {
      throw new Error("dd runtime client is closed");
    }
    const child = this.#ensureStarted();
    const id = String(this.#nextId++);
    const timeoutMs = this.options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    const payload = JSON.stringify({ id, ...command });
    return new Promise((resolveRequest, rejectRequest) => {
      const timeout = setTimeout(() => {
        this.#pending.delete(id);
        rejectRequest(new Error(`dd runtime command timed out after ${timeoutMs}ms: ${command.op}`));
      }, timeoutMs);
      this.#pending.set(id, {
        resolve: resolveRequest,
        reject: rejectRequest,
        timeout,
      });
      child.stdin.write(`${payload}\n`, (error) => {
        if (!error) {
          return;
        }
        clearTimeout(timeout);
        this.#pending.delete(id);
        if (this.#child === child) {
          this.#child = undefined;
        }
        rejectRequest(error);
      });
    });
  }

  async close() {
    if (this.#closed) {
      return;
    }
    const child = this.#liveChild();
    if (!child) {
      this.#closed = true;
      return;
    }
    try {
      await this.request({ op: "shutdown" });
    } catch {
      // The process may already be gone; terminate below.
    }
    this.#closed = true;
    child.kill("SIGTERM");
  }

  #ensureStarted() {
    const existing = this.#liveChild();
    if (existing) {
      return existing;
    }
    const command = runtimeCommand(this.options);
    const child = spawn(command.command, command.args, {
      cwd: command.cwd,
      env: { ...process.env, ...this.options.env },
      stdio: ["pipe", "pipe", "pipe"],
    });
    this.#child = child;
    this.#stdout = "";
    this.#stderr = "";
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk) => this.#onStdout(chunk));
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk) => {
      this.#stderr = `${this.#stderr}${chunk}`.slice(-16_384);
    });
    child.on("error", (error) => {
      if (this.#child === child) {
        this.#child = undefined;
        this.#rejectAll(error);
      }
    });
    child.on("exit", (code, signal) => {
      if (this.#child !== child) {
        return;
      }
      this.#child = undefined;
      if (this.#closed) {
        return;
      }
      this.#rejectAll(
        new Error(
          `dd runtime exited with ${signal ?? code}; stderr: ${this.#stderr.trim()}`,
        ),
      );
    });
    return child;
  }

  #liveChild() {
    const child = this.#child;
    if (!child) {
      return undefined;
    }
    if (
      child.exitCode != null ||
      child.signalCode != null ||
      child.stdin.destroyed ||
      child.stdin.writableEnded
    ) {
      this.#child = undefined;
      return undefined;
    }
    return child;
  }

  #onStdout(chunk) {
    this.#stdout += chunk;
    for (;;) {
      const newline = this.#stdout.indexOf("\n");
      if (newline === -1) {
        break;
      }
      const line = this.#stdout.slice(0, newline).trim();
      this.#stdout = this.#stdout.slice(newline + 1);
      if (line) {
        this.#handleLine(line);
      }
    }
  }

  #handleLine(line) {
    let message;
    try {
      message = JSON.parse(line);
    } catch (error) {
      this.#rejectAll(new Error(`invalid dd runtime response: ${error.message}: ${line}`));
      return;
    }
    const pending = this.#pending.get(message.id);
    if (!pending) {
      return;
    }
    this.#pending.delete(message.id);
    clearTimeout(pending.timeout);
    if (message.ok) {
      pending.resolve(message.result);
    } else {
      const error = new Error(message.error?.message ?? "dd runtime command failed");
      error.kind = message.error?.kind;
      pending.reject(error);
    }
  }

  #rejectAll(error) {
    for (const [id, pending] of this.#pending) {
      this.#pending.delete(id);
      clearTimeout(pending.timeout);
      pending.reject(error);
    }
  }
}

export async function bundleWorkerEntry(entry, options = {}) {
  const { build, mergeConfig } = await import("vite");
  const entryPath = normalizePath(entry);
  const baseConfig = {
    configFile: false,
    envFile: true,
    logLevel: options.logLevel ?? "warn",
    ssr: {
      noExternal: true,
    },
    build: {
      write: false,
      ssr: entryPath,
      target: options.target ?? "es2022",
      sourcemap: options.sourcemap ?? "inline",
      minify: options.minify ?? false,
      emptyOutDir: false,
      rollupOptions: {
        input: entryPath,
        output: {
          format: "es",
          codeSplitting: false,
          entryFileNames: "worker.js",
        },
      },
    },
  };
  const output = await build(mergeConfig(baseConfig, options.viteConfig ?? {}));
  const outputs = Array.isArray(output) ? output : [output];
  for (const rollupOutput of outputs) {
    for (const chunk of rollupOutput.output) {
      if (chunk.type === "chunk" && (chunk.isEntry || chunk.fileName === "worker.js")) {
        return chunk.code;
      }
    }
  }
  throw new Error(`Vite did not produce an entry chunk for ${entryPath}`);
}

function runtimeCommand(options) {
  const args = ["--stdio"];
  if (options.allowCodeGeneration !== false) {
    args.push("--allow-code-generation");
  }
  if (options.binary) {
    return { command: options.binary, args, cwd: options.cwd ?? process.cwd() };
  }
  if (process.env.DD_DEV_RUNTIME_BIN) {
    return {
      command: process.env.DD_DEV_RUNTIME_BIN,
      args,
      cwd: options.cwd ?? process.cwd(),
    };
  }
  const packagedBinary = packagedRuntimeBinary();
  if (packagedBinary) {
    return {
      command: packagedBinary,
      args,
      cwd: options.cwd ?? process.cwd(),
    };
  }
  const repoRoot = findRepoRoot(options.cwd ?? process.cwd());
  if (!repoRoot) {
    throw new Error(
      "No dd dev runtime binary found. Install @dd/runtime, set DD_DEV_RUNTIME_BIN, or run inside a dd source checkout.",
    );
  }
  return {
    command: "cargo",
    args: ["run", "--quiet", "-p", "runtime", "--bin", "dd_dev_runtime", "--", ...args],
    cwd: repoRoot,
  };
}

function packagedRuntimeBinary() {
  try {
    const runtime = require("@dd/runtime");
    return runtime.runtimeBinaryPath();
  } catch {
    return undefined;
  }
}

function findRepoRoot(start) {
  let current = resolve(start);
  for (;;) {
    if (existsSync(resolve(current, "Cargo.toml")) && existsSync(resolve(current, "crates/runtime"))) {
      return current;
    }
    const parent = dirname(current);
    if (parent === current) {
      return undefined;
    }
    current = parent;
  }
}

function normalizePath(value) {
  if (value instanceof URL) {
    return fileURLToPath(value);
  }
  return resolve(String(value));
}
