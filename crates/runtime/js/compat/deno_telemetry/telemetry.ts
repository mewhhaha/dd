class NoopSpan {
  end() {}
}

const TRACING_ENABLED = false;
const PROPAGATORS = [];

function builtinTracer() {
  return {
    startSpan() {
      return new NoopSpan();
    },
  };
}

const ContextManager = {
  active() {
    return undefined;
  },
};

function enterSpan(_span) {
  return undefined;
}

function restoreSnapshot(_snapshot) {}

export {
  builtinTracer,
  ContextManager,
  enterSpan,
  PROPAGATORS,
  restoreSnapshot,
  TRACING_ENABLED,
};
