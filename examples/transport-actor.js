function text(message, status = 200) {
  return new Response(message, {
    status,
    headers: { "content-type": "text/plain; charset=utf-8" },
  });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/") {
      return text("transport actor: CONNECT /session over public h3");
    }
    if (url.pathname !== "/session") {
      return text("not found", 404);
    }
    const media = env.MEDIA.get(env.MEDIA.idFromName("global"));
    return await media.atomic((state) => {
      const { response } = state.accept(request);
      return response;
    });
  },
  async wake(event, env) {
    const _ = env;
    if (!event?.stub || !event.handle) {
      return;
    }
    if (event.type === "transportstream") {
      await event.stub.apply([{
        type: "transport.stream",
        handle: event.handle,
        payload: event.data,
      }]);
      return;
    }
    if (event.type === "transportdatagram") {
      await event.stub.apply([{
        type: "transport.datagram",
        handle: event.handle,
        payload: event.data,
      }]);
    }
  },
};
