import type { PluginOption } from "vite";
import type { RscPluginOptions } from "@vitejs/plugin-rsc";
import type { DdVitePluginOptions } from "./index.js";

export interface DdReactRouterRscPluginOptions extends DdVitePluginOptions {
  buildDirectory?: string;
  workerEntry?: string | URL;
  serverEntry?: string | URL;
  rscEntry?: string | URL;
  asyncHooksShim?: string | URL | false;
  reactRouter?: false;
  viteRsc?: false | Omit<RscPluginOptions, "serverHandler">;
}

export function reactRouterRsc(options?: DdReactRouterRscPluginOptions): PluginOption[];
export default reactRouterRsc;
