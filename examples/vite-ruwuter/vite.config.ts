import { defineConfig } from "vite";
import dd from "@mewhhaha/vite-plugin-dd";
import tailwindcss from "@tailwindcss/vite";
import { fileURLToPath } from "node:url";

const asyncHooksShim = fileURLToPath(new URL("./src/node-async-hooks.ts", import.meta.url));

export default defineConfig({
  resolve: {
    alias: {
      "node:async_hooks": asyncHooksShim,
      async_hooks: asyncHooksShim,
    },
  },
  build: {
    rollupOptions: {
      input: "src/client.ts",
    },
  },
  plugins: [
    tailwindcss(),
    dd(),
  ],
});
