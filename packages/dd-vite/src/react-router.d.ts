import type { PluginOption } from "vite";
import type { DdVitePluginOptions } from "./index.js";

export interface DdReactRouterPluginOptions extends DdVitePluginOptions {
  buildDirectory?: string;
  workerEntry?: string | URL;
  serverEntry?: string | URL;
  reactRouter?: false;
}

export function reactRouter(options?: DdReactRouterPluginOptions): PluginOption[];
export default reactRouter;
