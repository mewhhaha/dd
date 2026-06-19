import "./tailwind.css";

export const marker = "vite-ruwuter-client-module";

if (import.meta.hot) {
  import.meta.hot.accept();
}

document.documentElement.dataset.ddExample = "vite-ruwuter";

document.documentElement.dataset.client = marker;
