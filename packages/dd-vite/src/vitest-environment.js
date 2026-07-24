import { createWorkerTestRuntime } from "./vitest.js";

export default {
  name: "dd",
  viteEnvironment: "ssr",
  async setup(globalThis, options = {}) {
    const runtime = await createWorkerTestRuntime(options.dd ?? {});
    globalThis.dd = runtime;
    return {
      async teardown() {
        await runtime.close();
        delete globalThis.dd;
      },
    };
  },
};
