import type { Plugin } from "vite";
import type { DdVitePluginOptions } from "./index.js";

export interface DdReactRouterRscPluginOptions extends DdVitePluginOptions {
  buildDirectory?: string;
  workerEntry?: string | URL;
  serverEntry?: string | URL;
  rscEntry?: string | URL;
  asyncHooksShim?: string | URL | false;
}

export function reactRouterRsc(options?: DdReactRouterRscPluginOptions): Plugin;
export default reactRouterRsc;
