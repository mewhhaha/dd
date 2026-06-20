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

const requestHandler = createRequestHandler(
  () => import("virtual:react-router/server-build"),
  import.meta.env.MODE,
);

function createRouterContext(env: Env): {
  ddContext: DdRequestContext;
  routerContext: RouterContextProvider;
} {
  const ddContext = createDdRequestContext(env, "vite-react-router");
  const context = new RouterContextProvider();
  context.set(ddRequestContext, ddContext);
  return { ddContext, routerContext: context };
}

function withStmHeader(response: Response, context: DdRequestContext): Response {
  const count = context.lastStmCount;
  if (count === undefined) {
    return response;
  }
  const headers = new Headers(response.headers);
  headers.set("x-dd-stm-count", String(count));
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

export default {
  async fetch(request: Request, env: Env) {
    const { ddContext, routerContext } = createRouterContext(env);
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
};
