import { ddVitePlugin } from "./vite.js";

export function reactRouterRsc(options = {}) {
  const {
    asyncHooksShim,
    buildDirectory,
    framework: _framework,
    rscEntry,
    serverEntry,
    workerEntry,
    ...pluginOptions
  } = options;
  return ddVitePlugin({
    ...pluginOptions,
    framework: {
      name: "react-router-rsc",
      buildDirectory,
      workerEntry,
      serverEntry,
      rscEntry,
      asyncHooksShim,
    },
  });
}

export default reactRouterRsc;
