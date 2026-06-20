import { reactRouter as reactRouterVitePlugin } from "@react-router/dev/vite";
import { ddVitePlugin } from "./vite.js";

export function reactRouter(options = {}) {
  const {
    buildDirectory,
    framework: _framework,
    reactRouter: reactRouterPlugin,
    serverEntry,
    workerEntry,
    ...pluginOptions
  } = options;
  const plugins = [
    ddVitePlugin({
      ...pluginOptions,
      framework: {
        name: "react-router",
        buildDirectory,
        workerEntry,
        serverEntry,
      },
    }),
  ];
  if (reactRouterPlugin !== false) {
    plugins.push(...reactRouterVitePlugin());
  }
  return plugins;
}

export default reactRouter;
