import dd from "@dd/vite";
import { unstable_reactRouterRSC as reactRouterRSC } from "@react-router/dev/vite";
import rsc from "@vitejs/plugin-rsc";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";
import {
  buildReactRouterRsc,
  bundleDdRscWorker,
  hasReactRouterRscServerOutput,
  readDdRscWorker,
  rscClientEntry,
  serveReactRouterRscClientAssets,
  writeDdRscBuildArtifacts,
} from "./rsc-dd-build.mjs";

export default defineConfig(({ command }) => {
  const buildOnly = process.env.DD_RSC_BUILD_ONLY === "1";
  const buildStartedAt = command === "build" ? Date.now() - 1_000 : 0;
  const ddRuntimePlugins = buildOnly
    ? []
    : [
        serveReactRouterRscClientAssets(),
        dd({
          source: async () => {
            if (command === "serve") {
              await buildReactRouterRsc();
              await writeDdRscBuildArtifacts();
              return readDdRscWorker();
            }
            return bundleDdRscWorker();
          },
          deploymentConfig: false,
        }),
      ];
  const ddBuildPlugins = buildOnly
    ? []
    : [
        {
          name: "dd-react-router-rsc-build-artifacts",
          buildApp: {
            order: "post",
            async handler() {
              if (!(await hasReactRouterRscServerOutput(buildStartedAt))) {
                throw new Error("React Router RSC server output was not ready for dd bundling");
              }
              await writeDdRscBuildArtifacts();
            }
          },
        },
      ];

  return {
    resolve: {
      alias: {
        "@react-router/dev/config/default-rsc-entries/entry.client": rscClientEntry,
      },
    },
    plugins: [
      tailwindcss(),
      ...ddRuntimePlugins,
      reactRouterRSC(),
      rsc({ serverHandler: false }),
      ...ddBuildPlugins,
    ],
  };
});
