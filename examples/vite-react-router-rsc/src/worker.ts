import type { Env } from "../app/dd-context";
import {
  acceptStorefrontLiveSocket,
  handleStorefrontLiveSocketWake,
} from "../app/storefront";
import server from "virtual:dd-react-router-rsc-server";

const WORKER_NAME = "vite-react-router-rsc";

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/live") {
      return await acceptStorefrontLiveSocket(request, env, WORKER_NAME);
    }
    return server.fetch(request, env);
  },
  async wake(event: unknown): Promise<void> {
    await handleStorefrontLiveSocketWake(
      event as Parameters<typeof handleStorefrontLiveSocketWake>[0],
      WORKER_NAME,
    );
  },
};
