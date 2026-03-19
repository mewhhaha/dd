import worker from "__WORKER_SPECIFIER__";

if (worker === undefined) {
  throw new Error("Worker must export default");
}

if (typeof worker !== "object" || worker === null) {
  throw new Error("Default export must be an object");
}

if (typeof worker.fetch !== "function") {
  throw new Error("Default export must define fetch(request, env, ctx)");
}

globalThis.__grugd_worker = worker;
export const ok = true;
