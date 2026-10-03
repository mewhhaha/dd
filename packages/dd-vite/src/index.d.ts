import type { EnvironmentOptions, Plugin } from "vite";
import type { DdAuxiliaryWorkerRecord, DdViteEnvironmentOptions, DdVitePluginOptions, DdWorkerRuntimeOptions } from "./types.js";

export type * from "./types.js";
export { DdRuntimeClient, bundleWorkerEntry, createDdRuntime } from "./runtime.js";
export { createWorkerTestRuntime } from "./vitest.js";

export const DD_CONFIG_SCHEMA_VERSION: 1;

export function ddEnvironment(
  options?: DdWorkerRuntimeOptions & {
    viteEnvironment?: DdViteEnvironmentOptions;
    environmentOptions?: EnvironmentOptions;
  },
): EnvironmentOptions;
export function ddVitePlugin(options?: DdVitePluginOptions): Plugin;
export default ddVitePlugin;

declare module "virtual:dd-auxiliary-workers" {
  export const workers: Record<string, DdAuxiliaryWorkerRecord>;
  export default workers;
}
