import { Effect, Schema } from "effect";
import { Router, type IRequest } from "itty-router";
import { AuthClient, frontendLayer } from "./auth-client";
import { internalError, notFound, type AppError } from "./errors";
import { errorResponse, textResponse } from "./http";
import { requestContextLayer, type RequestContext } from "./runtime";
import type { AppEnv, AuditEvent, AuthSessionView } from "./types";
import { renderAccountSummary, renderAudit, renderHome } from "./views";

type FrontendRequirements = RequestContext | AuthClient;

const AuthSessionViewSchema = Schema.Struct({
  authenticated: Schema.Literal(true),
  user: Schema.Struct({
    username: Schema.NonEmptyString,
    displayName: Schema.NonEmptyString,
    role: Schema.Literal("admin", "viewer"),
    passkeys: Schema.Number.pipe(Schema.nonNegative()),
  }),
  session: Schema.Struct({
    id: Schema.NonEmptyString,
    username: Schema.NonEmptyString,
    createdAt: Schema.NonEmptyString,
  }),
});

const AuditEventSchema = Schema.Struct({
  id: Schema.NonEmptyString,
  type: Schema.NonEmptyString,
  username: Schema.String,
  at: Schema.NonEmptyString,
  detail: Schema.String,
});

const router = Router<IRequest, [AppEnv], Response>()
  .get("/", effectHandler(accountOrHome()))
  .get("/me", effectHandler(accountOrHome()))
  .get("/audit", effectHandler(auditPage()))
  .all("/api/*", effectHandler(proxyAuth()))
  .post("/logout", effectHandler(proxyAuth()))
  .all("/__auth/*", effectHandler(proxyAuth()))
  .get("/src/status", () => textResponse("frontend worker src namespace"))
  .all("*", () => errorResponse(notFound("Route not found")));

const app = {
  fetch(request: Request, env: AppEnv): Promise<Response> {
    return router.fetch(request, env);
  },
};

export default app;

function accountOrHome(): Effect.Effect<Response, AppError, FrontendRequirements> {
  return sessionView().pipe(
    Effect.map((session) => session ? renderAccountSummary(session) : renderHome()),
  );
}

function auditPage(): Effect.Effect<Response, AppError, FrontendRequirements> {
  return AuthClient.pipe(
    Effect.flatMap((auth) => auth.fetch("/api/audit")),
    Effect.flatMap((response) => response.ok
      ? responseJson(response, Schema.Array(AuditEventSchema)).pipe(
        Effect.map((events) => renderAudit([...events] as AuditEvent[])),
      )
      : Effect.succeed(response)),
  );
}

function proxyAuth(): Effect.Effect<Response, AppError, FrontendRequirements> {
  return AuthClient.pipe(
    Effect.flatMap((auth) => auth.proxy),
  );
}

function sessionView(): Effect.Effect<AuthSessionView | null, AppError, FrontendRequirements> {
  return AuthClient.pipe(
    Effect.flatMap((auth) => auth.fetch("/api/session")),
    Effect.flatMap((response) => {
      if (response.status === 401) {
        return Effect.succeed(null);
      }
      if (!response.ok) {
        return Effect.fail(responseError(response, "Session lookup failed"));
      }
      return responseJson(response, AuthSessionViewSchema);
    }),
  );
}

function responseJson<A, I>(response: Response, schema: Schema.Schema<A, I, never>): Effect.Effect<A, AppError> {
  return Effect.tryPromise({
    try: () => response.json(),
    catch: (error) => internalParseError(error),
  }).pipe(
    Effect.flatMap((body) => Schema.decodeUnknown(schema)(body)),
    Effect.mapError((error) => internalParseError(error)),
  );
}

function responseError(response: Response, fallback: string): AppError {
  return internalError(`${fallback}: ${response.status}`);
}

function internalParseError(error: unknown): AppError {
  return internalError(`Auth worker response was invalid: ${String(error)}`);
}

function effectHandler(
  program: Effect.Effect<Response, AppError, FrontendRequirements>,
): (request: IRequest, env: AppEnv) => Promise<Response> {
  return (request, env) => Effect.runPromise(
    program.pipe(
      Effect.catchAll((error) => Effect.succeed(errorResponse(error))),
      Effect.provide(frontendLayer(env)),
      Effect.provide(requestContextLayer(request)),
    ),
  );
}
