import { reactRouter } from "@react-router/dev/vite";
import { defineConfig } from "vite";
import dd from "@dd/vite";
import tailwindcss from "@tailwindcss/vite";

const workerDefine = {
  "process.env.NODE_ENV": JSON.stringify("production"),
};

export default defineConfig({
  define: workerDefine,
  plugins: [
    tailwindcss(),
    reactRouter(),
    dd({
      entry: new URL("./src/worker.ts", import.meta.url),
      viteConfig: {
        define: workerDefine,
        plugins: [tailwindcss()],
      },
    }),
  ],
});
