import defaultEntry from "@react-router/dev/config/default-rsc-entries/entry.rsc";
import { RouterContextProvider } from "react-router";
import {
  createDdRequestContext,
  ddRequestContext,
  runWithDdRequestContext,
  type DdRequestContext,
  type Env,
} from "./dd-context";

function withDdHeaders(response: Response, context: DdRequestContext): Response {
  if (context.lastStmCount === undefined && !context.sessionCookie) {
    return response;
  }
  const headers = new Headers(response.headers);
  if (context.lastStmCount !== undefined) {
    headers.set("x-dd-stm-count", String(context.lastStmCount));
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
  async fetch(request: Request, env?: Env): Promise<Response> {
    if (!env) {
      return defaultEntry.fetch(request);
    }

    const dd = createDdRequestContext(env, "vite-react-router-rsc", request);
    const context = new RouterContextProvider();
    context.set(ddRequestContext, dd);
    return await runWithDdRequestContext(dd, async () => {
      const response = await defaultEntry.fetch(request, context);
      return withDdHeaders(response, dd);
    });
  },
};

if (import.meta.hot) {
  import.meta.hot.accept();
}
