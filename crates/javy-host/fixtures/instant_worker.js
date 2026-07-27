let completedRequests = 0;

export default {
  async fetch(request) {
    completedRequests += 1;
    const path = new URL(request.url).pathname;
    if (path === "/instance-requests") {
      return Response.json({ completedRequests });
    }
    if (path === "/infinite") {
      while (true) {}
    }
    return new Response("ok");
  },
};
