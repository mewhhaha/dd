(() => {
  const requestId = __REQUEST_ID__;
  const inflightRequests = globalThis.__dd_inflight_requests;
  const controller = inflightRequests?.get(requestId);
  if (controller) {
    controller.abort(new Error("Request aborted by caller"));
  }
})();
