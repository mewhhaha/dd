import { ddVitePlugin } from "./vite.js";

export function reactRouter(options = {}) {
  const {
    buildDirectory,
    framework: _framework,
    serverEntry,
    workerEntry,
    ...pluginOptions
  } = options;
  return ddVitePlugin({
    ...pluginOptions,
    framework: {
      name: "react-router",
      buildDirectory,
      workerEntry,
      serverEntry,
    },
  });
}

export default reactRouter;
