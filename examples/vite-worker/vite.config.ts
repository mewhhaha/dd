import { defineConfig } from "vite";
import dd from "@dd/vite";

export default defineConfig({
  plugins: [dd()],
});
