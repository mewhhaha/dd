import { defineConfig } from "vite";
import dd from "@mewhhaha/vite-plugin-dd";
import tailwindcss from "@tailwindcss/vite";

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
    tailwindcss(),
    dd(),
  ],
});
