import { Context, Effect, Either, Layer, Schedule, Schema } from "effect";
import { internalError, tooManyRequests, unauthorized, type AppError } from "./errors";
import { sessionCookie } from "./http";
import { RequestContext, WorkerEnv } from "./runtime";
import type { AuditEvent, ChallengeKind, PasskeyCredential, PendingChallenge, RateState, Session, User } from "./types";

const USER_INDEX_KEY = "auth:user-index";
const AUDIT_INDEX_KEY = "auth:audit-index";
const RATE_LIMIT = 5;
const RATE_WINDOW_MS = 60_000;
const CHALLENGE_TTL_MS = 300_000;
const STORAGE_RETRY = Schedule.spaced("10 millis").pipe(Schedule.intersect(Schedule.recurs(2)));
const TransportSchema = Schema.Literal("ble", "cable", "hybrid", "internal", "nfc", "smart-card", "usb");
const PasskeyCredentialSchema = Schema.Struct({
  id: Schema.NonEmptyString,
  publicKey: Schema.NonEmptyString,
  counter: Schema.Number.pipe(Schema.nonNegative()),
  transports: Schema.optional(Schema.Array(TransportSchema)),
  deviceType: Schema.Literal("singleDevice", "multiDevice"),
  backedUp: Schema.Boolean,
  createdAt: Schema.NonEmptyString,
  lastUsedAt: Schema.optional(Schema.String),
});
const UserSchema = Schema.Struct({
  id: Schema.NonEmptyString,
  username: Schema.NonEmptyString,
  displayName: Schema.NonEmptyString,
  role: Schema.Literal("admin", "viewer"),
  credentials: Schema.Array(PasskeyCredentialSchema),
  createdAt: Schema.NonEmptyString,
  updatedAt: Schema.NonEmptyString,
});
const SessionSchema = Schema.Struct({
  id: Schema.NonEmptyString,
  username: Schema.NonEmptyString,
  createdAt: Schema.NonEmptyString,
});
const AuditEventSchema = Schema.Struct({
  id: Schema.NonEmptyString,
  type: Schema.NonEmptyString,
  username: Schema.String,
  at: Schema.NonEmptyString,
  detail: Schema.String,
});
const StringArraySchema = Schema.Array(Schema.String);

export type StorageService = {
  readonly userByName: (username: string) => Effect.Effect<User | null, AppError>;
  readonly userForCredential: (credentialId: string) => Effect.Effect<User | null, AppError>;
  readonly userIndex: Effect.Effect<string[], AppError>;
  readonly saveUser: (user: User) => Effect.Effect<void, AppError>;
  readonly currentSession: Effect.Effect<Session | null, AppError>;
  readonly createSession: (user: User) => Effect.Effect<Session, AppError>;
  readonly deleteSession: (sessionId: string) => Effect.Effect<void, AppError>;
  readonly checkRateLimit: (label: string) => Effect.Effect<void, AppError>;
  readonly saveChallenge: (challenge: Omit<PendingChallenge, "createdAt" | "expiresAt">) => Effect.Effect<void, AppError>;
  readonly takeChallenge: (kind: ChallengeKind) => Effect.Effect<PendingChallenge, AppError>;
  readonly appendAudit: (type: string, username: string, detail: string) => Effect.Effect<void, AppError>;
  readonly recentAudit: Effect.Effect<AuditEvent[], AppError>;
};

export class Storage extends Context.Tag("vite-effect/Storage")<Storage, StorageService>() {}

export const StorageLive = Layer.effect(Storage, Effect.gen(function* () {
  const env = yield* WorkerEnv;
  const { request } = yield* RequestContext;

  function userByName(username: string): Effect.Effect<User | null, AppError> {
    return kvGet(env.AUTH_DB, userKey(username)).pipe(
      Effect.map(parseUser),
    );
  }

  function userForCredential(credentialId: string): Effect.Effect<User | null, AppError> {
    return kvGet(env.AUTH_DB, credentialKey(credentialId)).pipe(
      Effect.flatMap((username) => {
        if (typeof username !== "string" || !username) {
          return Effect.succeed(null);
        }
        return userByName(username);
      }),
    );
  }

  const userIndex = kvGet(env.AUTH_DB, USER_INDEX_KEY).pipe(
    Effect.map(parseStringArray),
  );

  function saveUser(user: User): Effect.Effect<void, AppError> {
    return userIndex.pipe(
      Effect.flatMap((index) => Effect.all([
        kvPut(env.AUTH_DB, userKey(user.username), JSON.stringify(user)),
        kvPut(env.AUTH_DB, USER_INDEX_KEY, JSON.stringify([...new Set([...index, user.username])])),
        ...user.credentials.map((credential) => kvPut(env.AUTH_DB, credentialKey(credential.id), user.username)),
      ], { concurrency: "unbounded", discard: true })),
    );
  }

  const currentSession = Effect.suspend(() => {
    const sessionId = sessionCookie(request);
    if (!sessionId) {
      return Effect.succeed(null);
    }
    return kvGet(env.AUTH_DB, sessionKey(sessionId)).pipe(
      Effect.map(parseSession),
    );
  });

  function createSession(user: User): Effect.Effect<Session, AppError> {
    const session: Session = {
      id: crypto.randomUUID(),
      username: user.username,
      createdAt: new Date().toISOString(),
    };
    return kvPut(env.AUTH_DB, sessionKey(session.id), JSON.stringify(session)).pipe(
      Effect.as(session),
    );
  }

  function deleteSession(sessionId: string): Effect.Effect<void, AppError> {
    return kvDelete(env.AUTH_DB, sessionKey(sessionId));
  }

  function checkRateLimit(label: string): Effect.Effect<void, AppError> {
    return Effect.tryPromise({
      try: async () => {
        const shard = env.AUTH_STATE.get(env.AUTH_STATE.idFromName(`rate:${label}:${clientFingerprint(request)}`));
        return await shard.atomic(() => {
          const now = Date.now();
          const state = shard.tvar<RateState>("attempts", { count: 0, resetAt: now + RATE_WINDOW_MS });
          const current = normalizeRateState(state.read(), now);
          if (current.count >= RATE_LIMIT && now < current.resetAt) {
            return false;
          }
          state.write({
            count: now >= current.resetAt ? 1 : current.count + 1,
            resetAt: now >= current.resetAt ? now + RATE_WINDOW_MS : current.resetAt,
          });
          return true;
        });
      },
      catch: (error) => internalError(`Rate limit check failed: ${String(error)}`),
    }).pipe(
      Effect.flatMap((allowed) => allowed ? Effect.void : Effect.fail(tooManyRequests("Too many passkey attempts"))),
    );
  }

  function saveChallenge(challenge: Omit<PendingChallenge, "createdAt" | "expiresAt">): Effect.Effect<void, AppError> {
    return Effect.tryPromise({
      try: async () => {
        const now = Date.now();
        const shard = challengeShard(env.AUTH_STATE, request);
        await shard.atomic(() => {
          shard.tvar<PendingChallenge | null>("challenge", null).write({
            ...challenge,
            createdAt: now,
            expiresAt: now + CHALLENGE_TTL_MS,
          });
        });
      },
      catch: (error) => internalError(`Challenge write failed: ${String(error)}`),
    });
  }

  function takeChallenge(kind: ChallengeKind): Effect.Effect<PendingChallenge, AppError> {
    return Effect.tryPromise({
      try: async () => {
        const shard = challengeShard(env.AUTH_STATE, request);
        return await shard.atomic(() => {
          const state = shard.tvar<PendingChallenge | null>("challenge", null);
          const challenge = state.read();
          state.write(null);
          return challenge;
        });
      },
      catch: (error) => internalError(`Challenge read failed: ${String(error)}`),
    }).pipe(
      Effect.flatMap((challenge) => {
        if (!challenge || challenge.kind !== kind || Date.now() > challenge.expiresAt) {
          return Effect.fail(unauthorized("Passkey challenge expired"));
        }
        return Effect.succeed(challenge);
      }),
    );
  }

  function appendAudit(type: string, username: string, detail: string): Effect.Effect<void, AppError> {
    const event: AuditEvent = {
      id: crypto.randomUUID().slice(0, 12),
      type,
      username,
      detail,
      at: new Date().toISOString(),
    };
    return kvGet(env.AUTH_DB, AUDIT_INDEX_KEY).pipe(
      Effect.flatMap((rawIndex) => {
        const ids = parseStringArray(rawIndex);
        return Effect.all([
          kvPut(env.AUTH_DB, auditKey(event.id), JSON.stringify(event)),
          kvPut(env.AUTH_DB, AUDIT_INDEX_KEY, JSON.stringify([event.id, ...ids].slice(0, 20))),
        ], { concurrency: "unbounded", discard: true });
      }),
    );
  }

  const recentAudit = kvGet(env.AUTH_DB, AUDIT_INDEX_KEY).pipe(
    Effect.flatMap((rawIndex) => {
      const ids = parseStringArray(rawIndex).slice(0, 12);
      return Effect.all(ids.map((id) => kvGet(env.AUTH_DB, auditKey(id))), {
        concurrency: "unbounded",
      });
    }),
    Effect.map((events) => events.map(parseAuditEvent).filter((event): event is AuditEvent => event !== null)),
  );

  return {
    userByName,
    userForCredential,
    userIndex,
    saveUser,
    currentSession,
    createSession,
    deleteSession,
    checkRateLimit,
    saveChallenge,
    takeChallenge,
    appendAudit,
    recentAudit,
  };
}));

export function replaceCredential(user: User, credential: PasskeyCredential): User {
  const credentials = [
    credential,
    ...user.credentials.filter((existing) => existing.id !== credential.id),
  ];
  return {
    ...user,
    credentials,
    updatedAt: new Date().toISOString(),
  };
}

export function updateCredential(user: User, credential: PasskeyCredential): User {
  return {
    ...user,
    credentials: user.credentials.map((existing) => existing.id === credential.id ? credential : existing),
    updatedAt: new Date().toISOString(),
  };
}

function kvGet(db: KvNamespace, key: string): Effect.Effect<unknown | null, AppError> {
  return retryStorage(Effect.tryPromise({
    try: () => db.get(key),
    catch: (error) => internalError(`KV get failed for ${key}: ${String(error)}`),
  }));
}

function kvPut(db: KvNamespace, key: string, value: string): Effect.Effect<void, AppError> {
  return retryStorage(Effect.tryPromise({
    try: () => Promise.resolve(db.put(key, value)),
    catch: (error) => internalError(`KV put failed for ${key}: ${String(error)}`),
  }));
}

function kvDelete(db: KvNamespace, key: string): Effect.Effect<void, AppError> {
  return retryStorage(Effect.tryPromise({
    try: () => Promise.resolve(db.delete(key)),
    catch: (error) => internalError(`KV delete failed for ${key}: ${String(error)}`),
  }));
}

function retryStorage<A>(effect: Effect.Effect<A, AppError>): Effect.Effect<A, AppError> {
  return effect.pipe(Effect.retry(STORAGE_RETRY));
}

function challengeShard(memory: MemoryNamespace, request: Request): MemoryShard {
  return memory.get(memory.idFromName(`challenge:${clientFingerprint(request)}`));
}

function normalizeRateState(value: unknown, now: number): RateState {
  if (!value || typeof value !== "object") {
    return { count: 0, resetAt: now + RATE_WINDOW_MS };
  }
  const state = value as Partial<RateState>;
  const resetAt = Number(state.resetAt ?? 0);
  if (!Number.isFinite(resetAt) || now >= resetAt) {
    return { count: 0, resetAt: now + RATE_WINDOW_MS };
  }
  return {
    count: Math.max(0, Math.trunc(Number(state.count ?? 0))),
    resetAt,
  };
}

function parseUser(value: unknown): User | null {
  const user = decodeStored(UserSchema, value);
  return user
    ? {
      ...user,
      credentials: user.credentials.map((credential) => ({
        ...credential,
        transports: credential.transports ? [...credential.transports] : undefined,
      })),
    }
    : null;
}

function parseSession(value: unknown): Session | null {
  return decodeStored(SessionSchema, value);
}

function parseAuditEvent(value: unknown): AuditEvent | null {
  return decodeStored(AuditEventSchema, value);
}

function parseStringArray(value: unknown): string[] {
  return [...(decodeStored(StringArraySchema, value) ?? [])];
}

function decodeStored<A, I>(schema: Schema.Schema<A, I, never>, value: unknown): A | null {
  const parsed = parseStored(value);
  if (parsed === null) {
    return null;
  }
  return Either.getOrNull(Schema.decodeUnknownEither(schema)(parsed));
}

function parseStored(value: unknown): unknown | null {
  try {
    return typeof value === "string" ? JSON.parse(value || "null") : value;
  } catch {
    return null;
  }
}

function clientFingerprint(request: Request): string {
  return request.headers.get("x-forwarded-for")
    ?? request.headers.get("cf-connecting-ip")
    ?? request.headers.get("user-agent")
    ?? "local";
}

function userKey(username: string): string {
  return `auth:user:${username}`;
}

function credentialKey(credentialId: string): string {
  return `auth:credential:${credentialId}`;
}

function sessionKey(sessionId: string): string {
  return `auth:session:${sessionId}`;
}

function auditKey(id: string): string {
  return `auth:audit:${id}`;
}
