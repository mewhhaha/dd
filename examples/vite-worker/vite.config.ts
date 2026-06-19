import { defineConfig } from "vite";
import dd from "@dd/vite";

export default defineConfig({
  build: {
    rollupOptions: {
      input: "src/client.ts",
    },
  },
  plugins: [dd()],
});
