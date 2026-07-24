import { defineConfig } from "vite";
import dd from "@mewhhaha/vite-plugin-dd";

export default defineConfig({
  build: {
    rollupOptions: {
      input: "src/client.ts",
      output: {
        entryFileNames: "assets/client.js",
      },
    },
  },
  plugins: [
    dd({
      viteEnvironment: {
        name: "vite_effect",
      },
      auxiliaryWorkers: [
        {
          name: "auth",
          kind: "service",
          binding: "AUTH",
          service: "vite-effect-auth",
          viteEnvironment: {
            name: "auth",
          },
          entry: "src/auth-worker.ts",
          config: {
            public: false,
            bindings: [
              { type: "kv", binding: "AUTH_DB" },
              { type: "memory", binding: "AUTH_STATE" },
            ],
          },
        },
      ],
    }),
  ],
});
