function text(body, status = 200) {
  return new Response(body, {
    status,
    headers: { "content-type": "text/plain; charset=utf-8" },
  });
}

function room(env) {
  return env.ROOMS.get(env.ROOMS.idFromName("deadlock-repro"));
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname !== "/ws") {
      return text("connect a websocket to /ws");
    }

    const stub = room(env);
    return await stub.atomic((tx) => {
      const { response } = tx.accept(request);
      return response;
    });
  },

  async wake(event) {
    if (!event?.stub || event.type !== "socketmessage") {
      return;
    }

    const handles = await event.stub.sockets.values();
    await event.stub.atomic((tx) => {
      tx.sockets.send(event.handle, `ready:${handles.length}`);
    });
  },
};
