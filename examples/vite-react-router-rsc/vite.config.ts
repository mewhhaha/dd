import ddReactRouterRsc from "@mewhhaha/vite-plugin-dd/react-router-rsc";
import { unstable_reactRouterRSC as reactRouterRSC } from "@react-router/dev/vite";
import rsc from "@vitejs/plugin-rsc";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [
    ddReactRouterRsc(),
    tailwindcss(),
    reactRouterRSC(),
    rsc({ serverHandler: false }),
  ],
});
