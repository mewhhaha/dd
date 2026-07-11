import dd from "@mewhhaha/vite-plugin-dd";
import { defineConfig } from "vite";

export default defineConfig({
  build: {
    rollupOptions: {
      input: "src/client.ts",
      output: {
        entryFileNames: "assets/client.js",
      },
    },
  },
  plugins: [dd()],
});
