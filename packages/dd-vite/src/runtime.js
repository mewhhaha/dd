import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { normalizeRuntimeConfig } from "./vite/config.js";

const DEFAULT_WORKER_NAME = "test-worker";
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_CLOSE_TIMEOUT_MS = 1_000;
const TERMINATE_TIMEOUT_MS = 1_000;
const require = createRequire(import.meta.url);

export function createDdRuntime(options = {}) {
  return new DdRuntimeClient(options);
}

export class DdRuntimeClient {
  #child;
  #generation = 0;
  #nextId = 1;
  #pending = new Map();
  #workers = new Map();
  #stdout = "";
  #stderr = "";
  #closed = false;
  #closing;
  #terminating = new Map();

  constructor(options = {}) {
    this.options = {
      timeoutMs: DEFAULT_TIMEOUT_MS,
      closeTimeoutMs: DEFAULT_CLOSE_TIMEOUT_MS,
      allowCodeGeneration: true,
      onConsole: printWorkerConsole,
      onInspector: printInspectorEvent,
      ...options,
    };
  }

  get generation() {
    return this.#generation;
  }

  async deploy(name, source, config = {}) {
    const result = await this.request({
      op: "deploy",
      name,
      source,
      config: normalizeRuntimeConfig(config, `worker ${name} config`),
    });
    this.#workers.set(name, result.url);
    return result;
  }

  workerUrl(name) {
    this.#liveChild();
    const url = this.#workers.get(name);
    if (!url) {
      throw new Error(`dd worker ${name} is not deployed in this runtime`);
    }
    return url;
  }

  async fetch(nameOrInput, inputOrInit, maybeInit) {
    const hasWorkerName = typeof nameOrInput === "string" && (
      typeof inputOrInit === "string" || inputOrInit instanceof URL || inputOrInit instanceof Request
    );
    const name = hasWorkerName ? nameOrInput : DEFAULT_WORKER_NAME;
    const input = hasWorkerName ? inputOrInit : nameOrInput;
    const init = hasWorkerName ? maybeInit : inputOrInit;
    const request = new Request(input, init);
    const originalUrl = new URL(request.url);
    const target = new URL(this.workerUrl(name));
    target.pathname = originalUrl.pathname;
    target.search = originalUrl.search;
    const headers = new Headers(request.headers);
    headers.set("x-dd-dev-request-url", request.url);
    headers.set("x-dd-dev-request-host", headers.get("host") ?? originalUrl.host);
    headers.delete("host");
    headers.delete("transfer-encoding");
    return fetch(target, {
      method: request.method,
      headers,
      body: request.body,
      duplex: "half",
      signal: request.signal,
      redirect: "manual",
    });
  }

  async stats(name) {
    return this.request({ op: "stats", name });
  }

  async request(command, options = {}) {
    if (this.#closed) {
      throw new Error("dd runtime client is closed");
    }
    return this.#sendRequest(this.#ensureStarted(), command, options);
  }

  #sendRequest(child, command, options = {}) {
    const id = String(this.#nextId++);
    const timeoutMs = options.timeoutMs ?? this.options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    const useTimeout = Number.isFinite(timeoutMs) && timeoutMs > 0;
    const payload = JSON.stringify({ ...command, id });
    return new Promise((resolveRequest, rejectRequest) => {
      const timeout = useTimeout
        ? setTimeout(() => {
          this.#pending.delete(id);
          if (this.#child === child) {
            this.#failChild(
              child,
              new Error(`dd runtime restarted after command timed out: ${command.op}`),
            );
          }
          rejectRequest(new Error(`dd runtime command timed out after ${timeoutMs}ms: ${command.op}`));
        }, timeoutMs)
        : undefined;
      this.#pending.set(id, {
        resolve: resolveRequest,
        reject: rejectRequest,
        timeout,
      });
      child.stdin.write(`${payload}\n`, (error) => {
        if (!error) {
          return;
        }
        if (this.#child === child) {
          this.#failChild(child, error);
        } else {
          clearTimeout(timeout);
          this.#pending.delete(id);
          rejectRequest(error);
        }
      });
    });
  }

  close() {
    if (this.#closing) return this.#closing;
    this.#closed = true;
    this.#closing = this.#close();
    return this.#closing;
  }

  async #close() {
    const child = this.#liveChild();
    if (child) {
      this.#sendRequest(child, { op: "shutdown" }, { timeoutMs: 0 }).catch(() => {});
      await waitForChildExit(child, this.options.closeTimeoutMs ?? DEFAULT_CLOSE_TIMEOUT_MS);
      this.#failChild(child, new Error("dd runtime client is closed"));
    }
    this.#workers.clear();
    this.#rejectAll(new Error("dd runtime client is closed"));
    await Promise.all(this.#terminating.values());
  }

  #failChild(child, error) {
    if (this.#child === child) {
      this.#discardChild(child);
      this.#rejectAll(error);
    }
    if (!this.#terminating.has(child)) {
      const terminating = terminateChild(child);
      this.#terminating.set(child, terminating);
      // Retain failures for close() to report; background command failures
      // must not leave an unhandled rejection or an untracked subprocess.
      terminating.then(() => this.#terminating.delete(child), () => {});
    }
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
    this.#generation += 1;
    this.#stdout = "";
    this.#stderr = "";
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      if (this.#child === child) this.#onStdout(chunk);
    });
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk) => {
      if (this.#child === child) this.#stderr = `${this.#stderr}${chunk}`.slice(-16_384);
    });
    child.stdin.on("error", (error) => {
      if (this.#child === child) this.#failChild(child, error);
    });
    child.on("error", (error) => {
      if (this.#child === child) {
        this.#failChild(child, error);
      }
    });
    child.on("exit", (code, signal) => {
      if (this.#child !== child) {
        return;
      }
      this.#discardChild(child);
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
      this.#failChild(child, new Error("dd runtime subprocess is no longer writable"));
      return undefined;
    }
    return child;
  }

  #discardChild(child) {
    if (this.#child !== child) {
      return;
    }
    this.#child = undefined;
    this.#workers.clear();
    this.#generation += 1;
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
    if (isConsoleEventLine(line)) {
      let event;
      try {
        event = JSON.parse(line);
      } catch {
        this.#stderr = `${this.#stderr}${line}\n`.slice(-16_384);
        return;
      }
      this.options.onConsole?.(event);
      return;
    }
    if (isInspectorEventLine(line)) {
      let event;
      try {
        event = JSON.parse(line);
      } catch {
        this.#stderr = `${this.#stderr}${line}\n`.slice(-16_384);
        return;
      }
      this.options.onInspector?.(event);
      return;
    }
    if (!isRuntimeProtocolLine(line)) {
      this.#stderr = `${this.#stderr}${line}\n`.slice(-16_384);
      return;
    }
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

function hasExited(child) {
  return child.exitCode != null || child.signalCode != null;
}

function waitForChildExit(child, ms) {
  if (hasExited(child) || !child.pid) return Promise.resolve(true);
  return new Promise((resolve) => {
    const finish = (exited) => {
      clearTimeout(timeout);
      child.off("exit", onExit);
      resolve(exited);
    };
    const onExit = () => finish(true);
    const timeout = setTimeout(() => finish(false), ms);
    child.once("exit", onExit);
  });
}

async function terminateChild(child) {
  if (hasExited(child) || !child.pid) return;
  child.kill("SIGTERM");
  if (await waitForChildExit(child, TERMINATE_TIMEOUT_MS)) return;
  child.kill("SIGKILL");
  if (!(await waitForChildExit(child, TERMINATE_TIMEOUT_MS))) {
    throw new Error(`dd runtime subprocess ${child.pid} did not exit after SIGKILL`);
  }
}

function isRuntimeProtocolLine(line) {
  return line.startsWith('{"id":');
}

function isConsoleEventLine(line) {
  return line.startsWith('{"event":"console"');
}

function isInspectorEventLine(line) {
  return line.startsWith('{"event":"inspector"');
}

/** The default destination for worker console output: this process's
 * stdout, or stderr for warnings and errors, each line tagged with the worker. */
export function printWorkerConsole({ worker, level, message }) {
  const stream = level === "error" || level === "warn" ? process.stderr : process.stdout;
  stream.write(`${formatWorkerConsole(worker, message)}\n`);
}

function printInspectorEvent(event) {
  process.stderr.write(`${formatInspectorEvent(event)}\n`);
}

/** How to attach Chrome DevTools: for the listener, where to find it; for a
 * worker isolate, the DevTools URL that opens it. */
export function formatInspectorEvent({ address, worker, isolate, devtools }) {
  if (!worker) {
    const configure = address === "127.0.0.1:9229" ? "" : ` (add ${address} under "Discover network targets" first)`;
    return `[dd] Debugger listening on ${address}. Open chrome://inspect in Chrome${configure} to debug worker isolates.`;
  }
  return `[dd:${worker}] DevTools for isolate ${isolate}: ${devtools}`;
}

export function formatWorkerConsole(worker, message) {
  const tag = worker ? `[dd:${worker}]` : "[dd]";
  return `${tag} ${String(message).replaceAll("\n", `\n${" ".repeat(tag.length + 1)}`)}`;
}

export async function bundleWorkerEntry(entry, options = {}) {
  const { build, mergeConfig } = await import("vite");
  const entryPath = normalizePath(entry);
  const baseConfig = {
    configFile: false,
    envFile: true,
    logLevel: options.logLevel ?? "warn",
    mode: "production",
    esbuild: {
      jsxDev: false,
    },
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
  const inspect = inspectArgument(options.inspect);
  if (inspect) {
    args.push(inspect);
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
  const repoRoot = findRepoRoot(options.cwd ?? process.cwd());
  if (repoRoot) {
    const debugBinary = resolve(repoRoot, "target/debug/dd_dev_runtime");
    if (existsSync(debugBinary)) {
      return {
        command: debugBinary,
        args,
        cwd: repoRoot,
      };
    }
    return {
      command: "cargo",
      args: ["run", "--quiet", "-p", "dd_server", "--no-default-features", "--features", "websocket", "--bin", "dd_dev_runtime", "--", ...args],
      cwd: repoRoot,
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
  throw new Error(
    "No dd dev runtime binary found. Install @mewhhaha/dd, set DD_DEV_RUNTIME_BIN, or run inside a dd source checkout.",
  );
}

function inspectArgument(inspect) {
  if (inspect == null || inspect === false) {
    return undefined;
  }
  if (inspect === true) {
    return "--inspect";
  }
  if (typeof inspect === "string" && inspect.trim() !== "") {
    return `--inspect=${inspect.trim()}`;
  }
  throw new TypeError('dd runtime option inspect must be true or a "host:port" string');
}

function packagedRuntimeBinary() {
  try {
    const runtime = require("@mewhhaha/dd");
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
