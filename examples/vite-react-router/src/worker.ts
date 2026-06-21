import { createRequestHandler, RouterContextProvider } from "react-router";
import {
  createDdRequestContext,
  ddRequestContext,
  registerDdRequestContext,
  requestWithDdRequestContext,
  unregisterDdRequestContext,
  type Env,
  type DdRequestContext,
} from "../app/dd-context";
import {
  acceptStorefrontCartSocket,
  acceptStorefrontLiveSocket,
  handleStorefrontLiveSocketWake,
} from "../app/storefront";

const requestHandler = createRequestHandler(
  () => import("virtual:react-router/server-build"),
  import.meta.env.MODE,
);
const WORKER_NAME = "vite-react-router";

function createRouterContext(env: Env, request: Request): {
  ddContext: DdRequestContext;
  routerContext: RouterContextProvider;
} {
  const ddContext = createDdRequestContext(env, WORKER_NAME, request);
  const context = new RouterContextProvider();
  context.set(ddRequestContext, ddContext);
  return { ddContext, routerContext: context };
}

function withStmHeader(response: Response, context: DdRequestContext): Response {
  const count = context.lastStmCount;
  if (count === undefined && !context.sessionCookie) {
    return response;
  }
  const headers = new Headers(response.headers);
  if (count !== undefined) {
    headers.set("x-dd-stm-count", String(count));
  }
  if (context.sessionCookie) {
    headers.append("set-cookie", context.sessionCookie);
  }
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

export default {
  async fetch(request: Request, env: Env) {
    const url = new URL(request.url);
    if (url.pathname === "/api/storefront/live") {
      return await acceptStorefrontLiveSocket(request, env, WORKER_NAME);
    }
    if (url.pathname === "/api/cart/live") {
      return await acceptStorefrontCartSocket(request, env, WORKER_NAME);
    }
    const { ddContext, routerContext } = createRouterContext(env, request);
    const contextId = registerDdRequestContext(ddContext);
    try {
      const response = await requestHandler(
        requestWithDdRequestContext(request, contextId),
        routerContext,
      );
      return withStmHeader(response, ddContext);
    } finally {
      unregisterDdRequestContext(contextId);
    }
  },
  async wake(event: unknown): Promise<void> {
    await handleStorefrontLiveSocketWake(
      event as Parameters<typeof handleStorefrontLiveSocketWake>[0],
      WORKER_NAME,
    );
  },
};
