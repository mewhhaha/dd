import server from "virtual:dd-react-router-rsc-server";

export default {
  fetch(request: Request, env: unknown): Promise<Response> {
    return server.fetch(request, env);
  },
};
