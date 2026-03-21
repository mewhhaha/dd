import * as workerModule from "__WORKER_SPECIFIER__";

const actorClasses = __ACTOR_CLASSES_JSON__;
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

if (!Array.isArray(actorClasses)) {
  throw new Error("actor class declaration must be an array");
}

const actorClassMap = Object.create(null);
for (const className of actorClasses) {
  const name = String(className ?? "").trim();
  if (!name) {
    throw new Error("actor class name must not be empty");
  }
  const value = workerModule[name];
  if (typeof value !== "function" || typeof value.prototype !== "object") {
    throw new Error(`actor class export is missing or not constructible: ${name}`);
  }
  actorClassMap[name] = value;
}

globalThis.__dd_worker_module = workerModule;
globalThis.__dd_worker = worker;
globalThis.__dd_actor_classes = actorClassMap;
export const ok = true;
