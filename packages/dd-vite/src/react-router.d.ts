import type { Plugin } from "vite";
import type { DdVitePluginOptions } from "./index.js";

export interface DdReactRouterPluginOptions extends DdVitePluginOptions {
  buildDirectory?: string;
  workerEntry?: string | URL;
  serverEntry?: string | URL;
}

export function reactRouter(options?: DdReactRouterPluginOptions): Plugin;
export default reactRouter;
