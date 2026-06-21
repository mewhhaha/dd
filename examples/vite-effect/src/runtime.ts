import { Context, Effect, Layer } from "effect";
import type { Env } from "./types";

export class WorkerEnv extends Context.Tag("vite-effect/WorkerEnv")<WorkerEnv, Env>() {}

export class RequestContext extends Context.Tag("vite-effect/RequestContext")<
  RequestContext,
  { readonly request: Request }
>() {}

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
