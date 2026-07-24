import * as workerModule from "__WORKER_SPECIFIER__";

const worker = workerModule.default;

if (worker === undefined) {
  throw new Error("Worker must export default");
}

if (typeof worker !== "object" || worker === null) {
  throw new Error("Default export must be an object");
}

if (typeof worker.fetch !== "function") {
  throw new Error("Default export must define fetch(request, env, ctx)");
}

globalThis.__dd_worker_module = workerModule;
globalThis.__dd_worker = worker;
export const ok = true;
