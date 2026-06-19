export default {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    return new Response(
      `<!doctype html>
        <title>Vite worker example</title>
        <main data-path="${url.pathname}">worker-root</main>`,
      { headers: { "content-type": "text/html; charset=utf-8" } },
    );
  },
};
