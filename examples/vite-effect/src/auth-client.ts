import { Context, Effect, Layer } from "effect";
import { internalError, type AppError } from "./errors";
import { currentRequest, type RequestContext } from "./runtime";
import type { AppEnv, ServiceBinding } from "./types";

export type AuthClientService = {
  readonly fetch: (input?: Request | string, init?: RequestInit) => Effect.Effect<Response, AppError, RequestContext>;
  readonly proxy: Effect.Effect<Response, AppError, RequestContext>;
};

export class FrontendEnv extends Context.Tag("vite-effect/FrontendEnv")<FrontendEnv, AppEnv>() {}

export class AuthClient extends Context.Tag("vite-effect/AuthClient")<AuthClient, AuthClientService>() {}

export function frontendLayer(env: AppEnv) {
  return AuthClientLive.pipe(
    Layer.provide(Layer.succeed(FrontendEnv, env)),
  );
}

export const AuthClientLive = Layer.effect(AuthClient, Effect.gen(function* () {
  const env = yield* FrontendEnv;

  function fetch(input?: Request | string, init?: RequestInit): Effect.Effect<Response, AppError, RequestContext> {
    return currentRequest.pipe(
      Effect.flatMap((request) => Effect.tryPromise({
        try: async () => {
          return authService(env).fetch(authRequest(request, input, init));
        },
        catch: (error) => internalError(`Auth worker call failed: ${String(error)}`),
      })),
    );
  }

  return {
    fetch,
    proxy: fetch(),
  };
}));

function authService(env: AppEnv): ServiceBinding {
  const service = env.AUTH;
  if (!service || typeof service !== "object" || typeof service.fetch !== "function") {
    throw new Error("AUTH service binding is missing");
  }
  return service;
}

function authRequest(parent: Request, input?: Request | string, init?: RequestInit): Request {
  if (input instanceof Request) {
    return input;
  }
  if (typeof input !== "string") {
    return parent;
  }
  const headers = new Headers(parent.headers);
  new Headers(init?.headers).forEach((value, name) => headers.set(name, value));
  return new Request(new URL(input, parent.url), {
    ...init,
    method: init?.method ?? "GET",
    headers,
  });
}
