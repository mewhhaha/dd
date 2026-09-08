export function memoryCommand(memory, transition) {
  if (typeof transition !== "function" || transition.constructor?.name === "AsyncFunction") {
    throw new Error("memory command requires a synchronous transition");
  }
  return (input, options) => memory.atomic((tx) => {
    const snapshot = Object.freeze({
      get: (key) => tx.get(key),
      list: (options) => tx.list(options),
    });
    const changes = transition(snapshot, input);
    if (typeof changes.then === "function") {
      throw new Error("memory command transition must be synchronous");
    }
    const { writes, deletes = [], effects = [], result } = changes;
    for (const key of deletes) tx.delete(key);
    for (const { key, value } of writes) tx.put(key, value);
    for (const { kind, payload } of effects) tx.emit(kind, payload);
    return result;
  }, options);
}
