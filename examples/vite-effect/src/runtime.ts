import { Context, Effect, Layer } from "effect";
import type { Env } from "./types";

export class WorkerEnv extends Context.Service<WorkerEnv, Env>()("vite-effect/WorkerEnv") {}

export class RequestContext extends Context.Service<
  RequestContext,
  { readonly request: Request }
>()("vite-effect/RequestContext") {}

export function requestLayer(env: Env, request: Request) {
  return Layer.mergeAll(
    Layer.succeed(WorkerEnv, env),
    requestContextLayer(request),
  );
}

export function requestContextLayer(request: Request) {
  return Layer.succeed(RequestContext, { request });
}

export const currentRequest = RequestContext.pipe(
  Effect.map(({ request }) => request),
);
