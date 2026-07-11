import defaultEntry from "@react-router/dev/config/default-rsc-entries/entry.rsc";

const flightPayloadScript =
  /<script>\(self\.__FLIGHT_DATA\|\|=\[\]\)\.push\([\s\S]*?\)<\/script>/g;

function moveFlightPayloadBeforeBootstrap(html: string): string {
  const payloadScripts = html.match(flightPayloadScript);
  if (!payloadScripts?.length) {
    return html;
  }

  const withoutPayload = html.replace(flightPayloadScript, "");
  const bootstrapIndex = withoutPayload.indexOf('<script id="_R_">');
  if (bootstrapIndex < 0) {
    return `${withoutPayload}${payloadScripts.join("")}`;
  }

  return `${withoutPayload.slice(0, bootstrapIndex)}${payloadScripts.join("")}${withoutPayload.slice(bootstrapIndex)}`;
}

async function repairFlightPayloadOrder(response: Response): Promise<Response> {
  if (!response.headers.get("content-type")?.toLowerCase().includes("text/html")) {
    return response;
  }

  const headers = new Headers(response.headers);
  headers.delete("content-length");
  return new Response(moveFlightPayloadBeforeBootstrap(await response.text()), {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

export default {
  async fetch(request: Request): Promise<Response> {
    return repairFlightPayloadOrder(await defaultEntry.fetch(request));
  },
};

if (import.meta.hot) {
  import.meta.hot.accept();
}
