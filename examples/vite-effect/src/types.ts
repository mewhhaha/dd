import type { AuthenticatorTransportFuture } from "@simplewebauthn/server";

export type DynamicWorkerConfig = {
  source?: string;
  entrypoint?: string;
  modules?: Record<string, string>;
  bindings?: Array<{ type: "kv" | "memory" | "dynamic"; binding: string }>;
  env?: Record<string, unknown>;
  timeout?: number;
  egress_allow_hosts?: string[];
  allow_host_rpc?: boolean;
  allow_websocket?: boolean;
  allow_transport?: boolean;
  allow_state_bindings?: boolean;
  max_request_bytes?: number;
  max_response_bytes?: number;
  max_outbound_requests?: number;
  max_concurrency?: number;
};

export type DynamicWorkerStub = {
  worker: string;
  fetch(input: Request | string, init?: RequestInit): Promise<Response>;
};

export type DynamicWorkerNamespace = {
  get(id: string, factory: () => DynamicWorkerConfig | Promise<DynamicWorkerConfig>): Promise<DynamicWorkerStub>;
  list(): Promise<string[]>;
  delete(id: string): Promise<unknown>;
};

export type AppEnv = {
  AUTH_WORKER: DynamicWorkerNamespace;
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
