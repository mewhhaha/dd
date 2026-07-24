import ddReactRouterRsc from "@mewhhaha/vite-plugin-dd/react-router-rsc";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [
    ddReactRouterRsc(),
    tailwindcss(),
  ],
});
