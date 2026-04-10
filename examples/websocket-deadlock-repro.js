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

    return await room(env).atomic((state) => {
      const { response } = state.accept(request);
      return response;
    });
  },

  async wake(event) {
    if (!event?.stub || event.type !== "socketopen") {
      return;
    }

    // This is the tiny shape of the old deadlock:
    //
    // socketopen wake is running on the isolate lane
    //   -> sockets.values() asks Rust for the active handles
    //     -> Rust replies through isolate event delivery
    //       -> isolate cannot deliver it because this wake is awaiting values()
    //
    // A healthy runtime sends "ready:1" immediately. The broken runtime hangs.
    const handles = await event.stub.sockets.values();
    const socket = new WebSocket(event.handle);
    socket.send(`ready:${handles.length}`, "text");
  },
};
