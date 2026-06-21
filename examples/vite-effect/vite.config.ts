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
      auxiliaryWorkers: [
        {
          name: "auth",
          binding: "AUTH_WORKER",
          entry: "src/auth-worker.ts",
          config: {
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
