(function () {
  const { internals } = __bootstrap;
  function updateSpanFromClientResponse(_span, _response) {}

  function updateSpanFromError(_span, _error) {}

  function updateSpanFromRequest(_span, _request) {}

  const telemetryUtil = { updateSpanFromClientResponse, updateSpanFromError, updateSpanFromRequest };
  internals.__telemetryUtil = telemetryUtil;
  return telemetryUtil;
})();
