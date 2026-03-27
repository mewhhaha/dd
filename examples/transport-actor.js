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
    const actor = env.MEDIA.get(env.MEDIA.idFromName("global"));
    return actor.fetch(request);
  },
};

export class MediaActor {
  constructor(state) {
    this.state = state;
  }

  async fetch(request) {
    const { handle, response } = await this.state.transports.accept(request);
    const session = new WebTransportSession(handle);

    void this.echoReliable(session);
    void this.echoDatagrams(session);

    return response;
  }

  async echoReliable(session) {
    const reader = session.stream.readable.getReader();
    const writer = session.stream.writable.getWriter();
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      await writer.write(value);
    }
  }

  async echoDatagrams(session) {
    const reader = session.datagrams.readable.getReader();
    const writer = session.datagrams.writable.getWriter();
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      await writer.write(value);
    }
  }
}
