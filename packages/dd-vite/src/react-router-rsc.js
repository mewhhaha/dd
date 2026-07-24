import { unstable_reactRouterRSC as reactRouterRSCVitePlugin } from "@react-router/dev/vite";
import rsc from "@vitejs/plugin-rsc";
import { ddVitePlugin } from "./vite.js";

export function reactRouterRsc(options = {}) {
  const {
    asyncHooksShim,
    buildDirectory,
    framework: _framework,
    reactRouter: reactRouterPlugin,
    rscEntry,
    serverEntry,
    viteRsc,
    workerEntry,
    ...pluginOptions
  } = options;
  const plugins = [
    ddVitePlugin({
      ...pluginOptions,
      framework: {
        name: "react-router-rsc",
        buildDirectory,
        workerEntry,
        serverEntry,
        rscEntry,
        asyncHooksShim,
      },
    }),
  ];
  if (reactRouterPlugin !== false) {
    plugins.push(...reactRouterRSCVitePlugin());
  }
  if (viteRsc !== false) {
    plugins.push(...rsc({
      ...(typeof viteRsc === "object" && viteRsc ? viteRsc : {}),
      serverHandler: false,
    }));
  }
  return plugins;
}

export default reactRouterRsc;
