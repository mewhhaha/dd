import { bundleWorkerEntry, createDdRuntime } from "./runtime.js";

export async function createWorkerTestRuntime(options = {}) {
  const name = options.name ?? "test-worker";
  const ownsRuntime = options.runtime == null;
  const runtime = options.runtime ?? createDdRuntime(options.runtimeOptions);
  let deployment;

  async function source() {
    if (typeof options.source === "function") {
      return options.source();
    }
    if (typeof options.source === "string") {
      return options.source;
    }
    if (!options.entry) {
      throw new Error("createWorkerTestRuntime requires either source or entry");
    }
    return bundleWorkerEntry(options.entry, {
      viteConfig: options.viteConfig,
      target: options.target,
      sourcemap: options.sourcemap,
      minify: options.minify,
      logLevel: options.logLevel,
    });
  }

  async function deploy() {
    deployment = await runtime.deploy(name, await source(), options.config ?? {});
    return deployment;
  }

  if (options.autoDeploy !== false) {
    try {
      await deploy();
    } catch (error) {
      if (ownsRuntime) await runtime.close();
      throw error;
    }
  }

  return {
    name,
    runtime,
    get deployment() {
      return deployment;
    },
    deploy,
    reload: deploy,
    fetch(input, init) {
      return runtime.fetch(name, input, init);
    },
    stats() {
      return runtime.stats(name);
    },
    async close() {
      if (ownsRuntime) await runtime.close();
    },
  };
}
