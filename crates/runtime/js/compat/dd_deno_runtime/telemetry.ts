(function () {
  const { internals } = __bootstrap;
  const TRACING_ENABLED = false;
  const PROPAGATORS = [];

  const ContextManager = {
    active() {
      return undefined;
    },
  };

  function builtinTracer() {
    return {
      startSpan() {
        return {
          end() {},
        };
      },
    };
  }

  function enterSpan(_span) {
    return undefined;
  }

  function restoreSnapshot(_snapshot) {}

  const DID_NOT_ENTER = Symbol("dd.noopTracing");
  function exitSpan(_snapshot) {}
  const telemetry = { TRACING_ENABLED, PROPAGATORS, ContextManager, builtinTracer, enterSpan, exitSpan, DID_NOT_ENTER, restoreSnapshot };
  internals.__telemetry = telemetry;
  return telemetry;
})();
