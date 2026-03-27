export const TRACING_ENABLED = false;
export const PROPAGATORS = [];

export const ContextManager = {
  active() {
    return undefined;
  },
};

export function builtinTracer() {
  return {
    startSpan() {
      return {
        end() {},
      };
    },
  };
}

export function enterSpan(_span) {
  return undefined;
}

export function restoreSnapshot(_snapshot) {}
