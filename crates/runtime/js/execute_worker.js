(() => {
  const requestId = __REQUEST_ID__;
  const worker = globalThis.__grugd_worker;
  if (worker === undefined) {
    throw new Error("Worker is not installed");
  }

  const input = __REQUEST_JSON__;
  const request = new Request(input.url, {
    method: input.method,
    headers: input.headers,
    body: input.body.length === 0 ? undefined : new Uint8Array(input.body),
  });
  const ctx = { requestId: input.request_id };

  (async () => {
    const response = await worker.fetch(request, {}, ctx);
    if (!(response instanceof Response)) {
      throw new Error("Worker fetch() must return a Response");
    }

    const body = Array.from(new Uint8Array(await response.arrayBuffer()));
    return {
      status: response.status,
      headers: Array.from(response.headers.entries()),
      body,
    };
  })()
    .then((result) => {
      Deno.core.ops.op_emit_completion(
        JSON.stringify({ request_id: requestId, ok: true, result }),
      );
    })
    .catch((error) => {
      const message = String((error && (error.stack || error.message)) || error);
      Deno.core.ops.op_emit_completion(
        JSON.stringify({ request_id: requestId, ok: false, error: message }),
      );
    });
})();
