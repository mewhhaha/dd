import defaultEntry from "@react-router/dev/config/default-rsc-entries/entry.rsc";
import { RouterContextProvider } from "react-router";
import {
  createDdRequestContext,
  ddRequestContext,
  type Env,
} from "./dd-context";

function withStmHeader(response: Response, count: number | undefined): Response {
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
  async fetch(request: Request, env?: Env): Promise<Response> {
    if (!env) {
      return defaultEntry.fetch(request);
    }

    const dd = createDdRequestContext(env, "vite-react-router-rsc");
    const context = new RouterContextProvider();
    context.set(ddRequestContext, dd);
    const response = await defaultEntry.fetch(request, context);
    return withStmHeader(response, dd.lastStmCount);
  },
};

if (import.meta.hot) {
  import.meta.hot.accept();
}
