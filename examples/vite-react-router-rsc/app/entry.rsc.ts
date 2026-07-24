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

const rscPayloadScriptPattern =
  /<script>\(self\.__FLIGHT_DATA\|\|=\[\]\)\.push\([\s\S]*?\)<\/script>/g;

function moveRscPayloadScripts(html: string): string {
  const payloadScripts = html.match(rscPayloadScriptPattern);
  if (!payloadScripts?.length) {
    return html;
  }

  const htmlWithoutPayloadScripts = html.replace(rscPayloadScriptPattern, "");
  const payload = payloadScripts.join("");
  const bootstrapIndex = htmlWithoutPayloadScripts.indexOf('<script id="_R_">');
  if (bootstrapIndex >= 0) {
    return `${htmlWithoutPayloadScripts.slice(0, bootstrapIndex)}${payload}${htmlWithoutPayloadScripts.slice(bootstrapIndex)}`;
  }

  const bodyEndIndex = htmlWithoutPayloadScripts.lastIndexOf("</body>");
  if (bodyEndIndex >= 0) {
    return `${htmlWithoutPayloadScripts.slice(0, bodyEndIndex)}${payload}${htmlWithoutPayloadScripts.slice(bodyEndIndex)}`;
  }

  return `${htmlWithoutPayloadScripts}${payload}`;
}

async function repairHtmlFlightPayload(response: Response): Promise<Response> {
  const contentType = response.headers.get("content-type") ?? "";
  if (!contentType.toLowerCase().includes("text/html")) {
    return response;
  }

  const headers = new Headers(response.headers);
  headers.delete("content-length");
  const html = await response.text();
  return new Response(moveRscPayloadScripts(html), {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

export default {
  async fetch(request: Request, env?: Env): Promise<Response> {
    if (!env) {
      return repairHtmlFlightPayload(await defaultEntry.fetch(request));
    }

    const dd = createDdRequestContext(env, "vite-react-router-rsc", request);
    const context = new RouterContextProvider();
    context.set(ddRequestContext, dd);
    return await runWithDdRequestContext(dd, async () => {
      const response = await defaultEntry.fetch(request, context);
      return withDdHeaders(await repairHtmlFlightPayload(response), dd);
    });
  },
};

if (import.meta.hot) {
  import.meta.hot.accept();
}
