import type { AuthenticatorTransportFuture } from "@simplewebauthn/server";

export type ServiceBinding = {
  readonly worker?: string;
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
};

export type AppEnv = {
  AUTH: ServiceBinding;
  [binding: string]: unknown;
};

export type AuthWorkerEnv = {
  AUTH_DB: KvNamespace;
  AUTH_STATE: MemoryNamespace;
};

export type Env = AuthWorkerEnv;

export type Role = "admin" | "viewer";

export type PasskeyCredential = {
  id: string;
  publicKey: string;
  counter: number;
  transports?: AuthenticatorTransportFuture[];
  deviceType: "singleDevice" | "multiDevice";
  backedUp: boolean;
  createdAt: string;
  lastUsedAt?: string;
};

export type User = {
  id: string;
  username: string;
  displayName: string;
  role: Role;
  credentials: PasskeyCredential[];
  createdAt: string;
  updatedAt: string;
};

export type Session = {
  id: string;
  username: string;
  createdAt: string;
};

export type AuthSessionView = {
  authenticated: true;
  user: {
    username: string;
    displayName: string;
    role: Role;
    passkeys: number;
  };
  session: {
    id: string;
    username: string;
    createdAt: string;
  };
};

export type AuditEvent = {
  id: string;
  type: string;
  username: string;
  at: string;
  detail: string;
};

export type RateState = {
  count: number;
  resetAt: number;
};

export type ChallengeKind = "registration" | "authentication";

export type PendingChallenge = {
  kind: ChallengeKind;
  challenge: string;
  username?: string;
  userId?: string;
  displayName?: string;
  createdAt: number;
  expiresAt: number;
};

export type RegistrationInput = {
  username: string;
  displayName: string;
};
