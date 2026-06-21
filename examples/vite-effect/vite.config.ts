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
    dd(),
  ],
});
