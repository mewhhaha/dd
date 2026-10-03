import type { EnvironmentOptions, InlineConfig } from "vite";
import type { DdRuntimeClient } from "./runtime.js";
import type { DdRuntimeWorkerStats, DdRuntimeAdminSnapshot } from "./runtime-contract.js";

export type * from "./runtime-contract.js";

export interface DdCacheConfig {
  enabled?: boolean;
}

export interface DdTraceDestination {
  worker: string;
  path?: `/${string}`;
}

export interface DdInternalConfig {
  trace?: DdTraceDestination | null;
}

export type DdBinding =
  | { type: "kv"; binding: string }
  | { type: "memory"; binding: string }
  | { type: "service"; binding: string; service: string };

export interface DdDeployConfig {
  egress_allow_hosts?: string[];
  public?: boolean;
  cache?: DdCacheConfig;
  bindings?: DdBinding[];
  internal?: DdInternalConfig;
}

export type DdServerModuleKind = "ESModule" | "CompiledWasm" | "Text" | "Data" | "Json";

export type DdServerModuleConfig = {
  path: string;
  file?: string | null;
} & (
  | { type: DdServerModuleKind; kind?: never }
  | { kind: DdServerModuleKind; type?: never }
);

export interface DdProjectConfig {
  $schema?: string;
  schema_version: 1;
  name: string;
  entrypoint: string;
  base_url?: string;
  baseUrl?: string;
  assets_dir?: string | null;
  temporary?: boolean;
  asset_excludes?: string[];
  server_modules?: DdServerModuleConfig[];
  config?: DdDeployConfig;
  egress_allow_hosts?: string[];
  public?: boolean;
  cache?: DdCacheConfig;
  bindings?: DdBinding[];
  internal?: DdInternalConfig;
}

export interface DdKvNamespace {
  get<T = unknown>(key: string): Promise<T | null>;
  put(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<void>;
  list<T = unknown>(options?: { prefix?: string; limit?: number }): Promise<Array<{ key: string; value: T }>>;
}

export interface DdCacheNamespace {
  match(request: RequestInfo | URL): Promise<Response | undefined>;
  put(request: RequestInfo | URL, response: Response): Promise<void>;
  delete(request: RequestInfo | URL): Promise<boolean>;
}

export interface DdMemoryId {
  readonly __dd_memory_binding: string;
  readonly __dd_memory_key: string;
  toString(): string;
}

export interface DdMemoryListEntry<T = unknown> {
  key: string;
  value: T;
}

export interface DdMemorySnapshot {
  get<T = unknown>(key: string): T | null;
  list<T = unknown>(options?: { prefix?: string; limit?: number }): Array<DdMemoryListEntry<T>>;
}

export interface DdMemoryTransaction extends DdMemorySnapshot {
  readonly id: DdMemoryId;
  put(key: string, value: unknown): void;
  delete(key: string): boolean;
  emit(kind: string, payload?: unknown): void;
  accept(request: Request): { handle: string; response: Response };
  readonly sockets: {
    values(): string[];
    send(handle: string, payload: string | Uint8Array): void;
    close(handle: string, code?: number, reason?: string): void;
  };
}

export interface DdMemoryStub {
  readonly id: DdMemoryId;
  readonly binding: string;
  readonly sockets: { values(): Promise<string[]> };
  read<T>(
    callback: (snapshot: DdMemorySnapshot) => T & (T extends { then: (...args: never[]) => unknown } ? never : unknown),
  ): Promise<T>;
  atomic<T>(
    callback: (tx: DdMemoryTransaction) => T & (T extends { then: (...args: never[]) => unknown } ? never : unknown),
    options?: { idempotencyKey?: string },
  ): Promise<T>;
}

export interface DdMemoryNamespace {
  idFromName(name: string): DdMemoryId;
  get(id: string | DdMemoryId): DdMemoryStub;
}

export interface DdServiceBinding {
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
}

export interface DdRuntimeDeployResult {
  type: "deploy";
  worker: string;
  deployment_id: string;
  url: string;
}

export interface DdRuntimeStatsResult {
  type: "stats";
  stats: DdRuntimeWorkerStats | null;
}

export interface DdAdminStatusResponse {
  ok: boolean;
  ready: boolean;
  draining: boolean;
  shutting_down: boolean;
  active_requests: number;
  active_control_operations: number;
  active_deployments: number;
  restoration_failures: string[];
  runtime: DdRuntimeAdminSnapshot;
  trace_exporter: {
    compiled: boolean;
    configured: boolean;
    enabled: boolean;
    state: "disabled" | "unverified" | "pending" | "healthy" | "error";
    verified: boolean;
    export_successes: number;
    export_failures: number;
  };
}

export interface DdApiError {
  ok: false;
  error: string;
  code: string;
  trace_id: string | null;
  retryable: boolean;
}

export type DdRuntimeCommandResult =
  | DdRuntimeDeployResult
  | DdRuntimeStatsResult
  | { type: "shutdown" };

export interface DdRuntimeOptions {
  binary?: string;
  cwd?: string;
  env?: Record<string, string>;
  timeoutMs?: number;
  closeTimeoutMs?: number;
  allowCodeGeneration?: boolean;
}

export interface DdWorkerBundleOptions {
  viteConfig?: InlineConfig;
  target?: string;
  sourcemap?: boolean | "inline" | "hidden";
  minify?: boolean;
  logLevel?: "silent" | "error" | "warn" | "info";
}

export interface DdWorkerRuntimeOptions extends DdWorkerBundleOptions {
  name?: string;
  entry?: string | URL;
  source?: string | (() => string | Promise<string>);
  config?: DdDeployConfig;
  runtime?: DdRuntimeClient;
  runtimeOptions?: DdRuntimeOptions;
  autoDeploy?: boolean;
}

export interface DdAuxiliaryWorkerOptions extends DdWorkerBundleOptions {
  name: string;
  kind?: "service";
  binding?: string;
  service?: string;
  viteEnvironment?: DdViteEnvironmentOptions;
  entry?: string | URL;
  source?: string | (() => string | Promise<string>);
  config?: DdDeployConfig;
  deployment?: {
    entrypoint?: string;
    output?: string;
  };
}

export interface DdAuxiliaryWorkerRecord {
  name: string;
  kind: "service";
  binding: string;
  service?: string;
  config: DdDeployConfig;
}

export interface DdViteEnvironmentOptions {
  name?: string;
  childEnvironments?: string[];
  options?: EnvironmentOptions;
}

export interface DdStaticRoutesOptions {
  version?: number;
  include?: string[];
  exclude?: string[];
}

export type DdFrameworkName = "react-router" | "react-router-rsc";

export interface DdFrameworkOptions {
  name: DdFrameworkName;
  buildDirectory?: string;
  workerEntry?: string | URL;
  serverEntry?: string | URL;
  rscEntry?: string | URL;
  asyncHooksShim?: string | URL | false;
}

export interface DdGeneratedDeploymentConfigOptions {
  enabled?: boolean;
  input?: string | URL | Partial<DdProjectConfig> | (() => Partial<DdProjectConfig> | Promise<Partial<DdProjectConfig>>);
  output?: string;
  entrypoint?: string;
  assetsDir?: string | false;
  assetExcludes?: string[];
  serverModules?: DdServerModuleConfig[];
  staticRoutes?: DdStaticRoutesOptions | false;
}

export interface DdVitePluginOptions extends DdWorkerRuntimeOptions {
  mount?: string;
  middleware?: boolean;
  environment?: boolean;
  viteEnvironment?: DdViteEnvironmentOptions;
  environmentOptions?: EnvironmentOptions;
  reloadOnHotUpdate?: boolean | "all" | "entry";
  deploymentConfig?: false | DdGeneratedDeploymentConfigOptions;
  auxiliaryWorkers?: DdAuxiliaryWorkerOptions[];
  eager?: boolean;
}
