import { defineConfig } from "vite";
import ddReactRouter from "@mewhhaha/vite-plugin-dd/react-router";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [
    ddReactRouter(),
    tailwindcss(),
  ],
});
