import {
  type AuthenticationResponseJSON,
  type RegistrationResponseJSON,
} from "@simplewebauthn/server";
import { Effect, Schema } from "effect";
import { badRequest, forbidden, unauthorized, type AppError } from "./errors";
import { expireSessionCookie, jsonResponse, redirectResponse, requestJson, serializeSessionCookie } from "./http";
import { type RequestContext } from "./runtime";
import { replaceCredential, Storage, type StorageService, updateCredential } from "./storage";
import type { RegistrationInput, Session, User } from "./types";
import { renderAccount, renderAudit, renderHome } from "./views";
import { Passkeys } from "./webauthn";

const RegistrationInputSchema = Schema.Struct({
  username: Schema.Trim.pipe(Schema.minLength(1), Schema.maxLength(64)),
  displayName: Schema.Trim.pipe(Schema.minLength(1), Schema.maxLength(80)),
});

export function home(): Effect.Effect<Response, AppError, Storage> {
  return Effect.gen(function* () {
    const storage = yield* Storage;
    const session = yield* storage.currentSession;
    if (!session) {
      return renderHome();
    }
    const { user } = yield* userForSession(storage, session);
    return renderAccount(session, user);
  });
}

export function account(): Effect.Effect<Response, AppError, Storage> {
  return Effect.gen(function* () {
    const storage = yield* Storage;
    const session = yield* requireSession(storage);
    const { user } = yield* userForSession(storage, session);
    return renderAccount(session, user);
  });
}

export function audit(): Effect.Effect<Response, AppError, Storage> {
  return Effect.gen(function* () {
    const storage = yield* Storage;
    yield* requireAdmin(storage);
    const events = yield* storage.recentAudit;
    return renderAudit(events);
  });
}

export function apiSession(): Effect.Effect<Response, AppError, Storage> {
  return Effect.gen(function* () {
    const storage = yield* Storage;
    const session = yield* requireSession(storage);
    const { user } = yield* userForSession(storage, session);
    return jsonResponse({
      authenticated: true,
      user: {
        username: user.username,
        displayName: user.displayName,
        role: user.role,
        passkeys: user.credentials.length,
      },
      session: {
        id: session.id,
        createdAt: session.createdAt,
      },
    });
  });
}

export function beginRegistration(): Effect.Effect<Response, AppError, RequestContext | Storage | Passkeys> {
  return Effect.gen(function* () {
    const storage = yield* Storage;
    const passkeys = yield* Passkeys;
    yield* storage.checkRateLimit("register-options");
    const input = yield* registrationInput();
    const existing = yield* storage.userByName(input.username);
    if (existing) {
      yield* requireOwnSession(storage, existing.username);
    }
    const user = existing ?? (yield* newUser(storage, input));
    const options = yield* passkeys.registrationOptions(user);
    yield* storage.saveChallenge({
      kind: "registration",
      challenge: options.challenge,
      username: user.username,
      userId: user.id,
      displayName: user.displayName,
    });
    return jsonResponse(options);
  });
}

export function finishRegistration(): Effect.Effect<Response, AppError, RequestContext | Storage | Passkeys> {
  return Effect.gen(function* () {
    const storage = yield* Storage;
    const passkeys = yield* Passkeys;
    yield* storage.checkRateLimit("register-verify");
    const response = yield* requestJson<RegistrationResponseJSON>();
    const challenge = yield* storage.takeChallenge("registration");
    const existing = yield* storage.userByName(challenge.username ?? "");
    if (existing) {
      yield* requireOwnSession(storage, existing.username);
    }
    const user = existing ?? (yield* newUser(storage, {
      username: challenge.username ?? "",
      displayName: challenge.displayName ?? challenge.username ?? "",
    }, challenge.userId));
    const registration = yield* passkeys.verifyRegistration(response, challenge.challenge);
    const credential = passkeys.credentialFromRegistration(registration, response);
    const nextUser = replaceCredential(user, credential);
    yield* storage.saveUser(nextUser);
    const session = yield* storage.createSession(nextUser);
    yield* storage.appendAudit("passkey.register", nextUser.username, `credential ${credential.id.slice(0, 8)}`).pipe(
      Effect.catchAll(() => Effect.void),
    );
    return jsonResponse({
      verified: true,
      redirect: "/me",
    }, {
      headers: {
        "set-cookie": serializeSessionCookie(session.id),
      },
    });
  }).pipe(
    Effect.catchAll((error) => recordFailure("passkey.register.failure", error)),
  );
}

export function beginAuthentication(): Effect.Effect<Response, AppError, Storage | Passkeys> {
  return Effect.gen(function* () {
    const storage = yield* Storage;
    const passkeys = yield* Passkeys;
    yield* storage.checkRateLimit("auth-options");
    const options = yield* passkeys.authenticationOptions;
    yield* storage.saveChallenge({
      kind: "authentication",
      challenge: options.challenge,
    });
    return jsonResponse(options);
  });
}

export function finishAuthentication(): Effect.Effect<Response, AppError, RequestContext | Storage | Passkeys> {
  return Effect.gen(function* () {
    const storage = yield* Storage;
    const passkeys = yield* Passkeys;
    yield* storage.checkRateLimit("auth-verify");
    const response = yield* requestJson<AuthenticationResponseJSON>();
    const challenge = yield* storage.takeChallenge("authentication");
    const user = yield* storage.userForCredential(response.id).pipe(
      Effect.flatMap((candidate) => candidate ? Effect.succeed(candidate) : Effect.fail(unauthorized("Passkey is not registered"))),
    );
    const credential = user.credentials.find((candidate) => candidate.id === response.id);
    if (!credential) {
      return yield* Effect.fail(unauthorized("Passkey is not registered"));
    }
    const authentication = yield* passkeys.verifyAuthentication(response, challenge.challenge, credential);
    const nextCredential = passkeys.credentialFromAuthentication(credential, authentication);
    const nextUser = updateCredential(user, nextCredential);
    yield* storage.saveUser(nextUser);
    const session = yield* storage.createSession(nextUser);
    yield* storage.appendAudit("passkey.login", nextUser.username, `credential ${nextCredential.id.slice(0, 8)}`).pipe(
      Effect.catchAll(() => Effect.void),
    );
    return jsonResponse({
      verified: true,
      redirect: "/me",
    }, {
      headers: {
        "set-cookie": serializeSessionCookie(session.id),
      },
    });
  }).pipe(
    Effect.catchAll((error) => recordFailure("passkey.login.failure", error)),
  );
}

export function logout(): Effect.Effect<Response, AppError, Storage> {
  return Effect.gen(function* () {
    const storage = yield* Storage;
    const session = yield* storage.currentSession;
    if (session) {
      yield* storage.deleteSession(session.id);
      yield* storage.appendAudit("logout", session.username, `session ${session.id.slice(0, 8)}`).pipe(
        Effect.catchAll(() => Effect.void),
      );
    }
    return redirectResponse("/", {
      "set-cookie": expireSessionCookie(),
    });
  });
}

function recordFailure(type: string, error: AppError): Effect.Effect<never, AppError, Storage> {
  return Storage.pipe(
    Effect.flatMap((storage) => storage.appendAudit(type, "anonymous", error.message).pipe(
      Effect.catchAll(() => Effect.void),
      Effect.flatMap(() => Effect.fail(error)),
    )),
  );
}

function requireAdmin(storage: StorageService): Effect.Effect<User, AppError> {
  return requireSession(storage).pipe(
    Effect.flatMap((session) => userForSession(storage, session)),
    Effect.flatMap(({ user }) => user.role === "admin"
      ? Effect.succeed(user)
      : Effect.fail(forbidden("Admin access required"))),
  );
}

function requireSession(storage: StorageService): Effect.Effect<Session, AppError> {
  return storage.currentSession.pipe(
    Effect.flatMap((session) => session
      ? Effect.succeed(session)
      : Effect.fail(unauthorized("Sign in required"))),
  );
}

function requireOwnSession(storage: StorageService, username: string): Effect.Effect<Session, AppError> {
  return requireSession(storage).pipe(
    Effect.flatMap((session) => session.username === username
      ? Effect.succeed(session)
      : Effect.fail(forbidden("Cannot register a passkey for another user"))),
  );
}

function userForSession(storage: StorageService, session: Session): Effect.Effect<{ session: Session; user: User }, AppError> {
  return storage.userByName(session.username).pipe(
    Effect.flatMap((user) => user
      ? Effect.succeed({ session, user })
      : Effect.fail(unauthorized("Session user was not found"))),
  );
}

function registrationInput(): Effect.Effect<RegistrationInput, AppError, RequestContext> {
  return requestJson<unknown>().pipe(
    Effect.flatMap((body) => Schema.decodeUnknown(RegistrationInputSchema)(body)),
    Effect.mapError(() => badRequest("Username and display name are required")),
    Effect.flatMap((input) => {
      const username = normalizeUsername(input.username);
      const displayName = input.displayName;
      if (!username) {
        return Effect.fail(badRequest("Username is required"));
      }
      if (!displayName) {
        return Effect.fail(badRequest("Display name is required"));
      }
      return Effect.succeed({ username, displayName });
    }),
  );
}

function newUser(storage: StorageService, input: RegistrationInput, id: string = crypto.randomUUID()): Effect.Effect<User, AppError> {
  return storage.userIndex.pipe(
    Effect.map((users) => {
      const now = new Date().toISOString();
      return {
        id,
        username: input.username,
        displayName: input.displayName,
        role: users.length === 0 ? "admin" : "viewer",
        credentials: [],
        createdAt: now,
        updatedAt: now,
      };
    }),
  );
}

function normalizeUsername(value: unknown): string {
  return String(value ?? "").trim().toLowerCase().replace(/[^a-z0-9._-]/g, "").slice(0, 64);
}
