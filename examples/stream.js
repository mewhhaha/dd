export default {
  async fetch() {
    return new Response(
      new ReadableStream({
        start(controller) {
          (async () => {
            controller.enqueue("hel");
            await new Promise((resolve) => setTimeout(resolve, 15));
            controller.enqueue("lo");
            await new Promise((resolve) => setTimeout(resolve, 15));
            controller.enqueue(" world");
            controller.close();
          })().catch((error) => controller.error(error));
        },
      }),
      {
        headers: {
          "content-type": "text/plain; charset=utf-8",
        },
      },
    );
  },
};
