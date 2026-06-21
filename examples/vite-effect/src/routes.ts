import { Effect, Layer } from "effect";
import { Router, type IRequest } from "itty-router";
import {
  account,
  apiSession,
  audit,
  beginAuthentication,
  beginRegistration,
  finishAuthentication,
  finishRegistration,
  home,
  logout,
} from "./auth";
import { notFound, type AppError } from "./errors";
import { errorResponse, textResponse } from "./http";
import { requestLayer, type RequestContext } from "./runtime";
import { Storage, StorageLive } from "./storage";
import type { Env } from "./types";
import { Passkeys, PasskeysLive } from "./webauthn";

type AppRequirements = RequestContext | Storage | Passkeys;

const services = Layer.mergeAll(StorageLive, PasskeysLive);

const router = Router<IRequest, [Env], Response>()
  .get("/", effectHandler(home()))
  .get("/me", effectHandler(account()))
  .get("/audit", effectHandler(audit()))
  .get("/api/session", effectHandler(apiSession()))
  .post("/api/passkeys/register/options", effectHandler(beginRegistration()))
  .post("/api/passkeys/register/verify", effectHandler(finishRegistration()))
  .post("/api/passkeys/authenticate/options", effectHandler(beginAuthentication()))
  .post("/api/passkeys/authenticate/verify", effectHandler(finishAuthentication()))
  .post("/logout", effectHandler(logout()))
  .get("/src/status", () => textResponse("worker src namespace"))
  .all("*", () => errorResponse(notFound("Route not found")));

const app = {
  fetch(request: Request, env: Env): Promise<Response> {
    return router.fetch(request, env);
  },
};

export default app;

function effectHandler(
  program: Effect.Effect<Response, AppError, AppRequirements>,
): (request: IRequest, env: Env) => Promise<Response> {
  return (request, env) => Effect.runPromise(
    program.pipe(
      Effect.catchAll((error) => Effect.succeed(errorResponse(error))),
      Effect.provide(services),
      Effect.provide(requestLayer(env, request)),
    ),
  );
}
