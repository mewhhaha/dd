import type { Env } from "../app/dd-context";
import server from "virtual:dd-react-router-rsc-server";

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    return server.fetch(request, env);
  },
};
