import type { EnvironmentOptions, InlineConfig, Plugin } from "vite";

export const DD_CONFIG_SCHEMA_VERSION: 1;

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

export interface DdMemoryTransaction {
  readonly id: DdMemoryId;
  get<T = unknown>(key: string): T | null;
  put(key: string, value: unknown): void;
  delete(key: string): boolean;
  list<T = unknown>(options?: { prefix?: string; limit?: number }): Array<DdMemoryListEntry<T>>;
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

export interface DdRuntimeWorkerStats {
  generation: number;
  public: boolean;
  queued: number;
  busy: number;
  inflight_total: number;
  wait_until_total: number;
  isolates_total: number;
  spawn_count: number;
  reuse_count: number;
  scale_down_count: number;
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
  active_deployments: number;
  restoration_failures: string[];
  runtime: {
    worker_schedulers: number;
    active_deployments: number;
    workers: Array<DdRuntimeWorkerStats & { name: string; outbox_lag_shards: number }>;
    restore_failures: Array<{ worker: string | null; source: string; error: string }>;
    readiness: {
      ready: boolean;
      runtime_ready: boolean;
      migrations_ready: boolean;
      storage_ready: boolean;
      worker_restoration_ready: boolean;
      restore_failure_count: number;
      failed_components: string[];
    };
    storage_retry_count: number;
    state_storage: {
      committed_groups: number;
      committed_commands: number;
      rollbacks: number;
      discarded_connections: number;
      busy_retries: number;
      pending_commands: number;
      pending_bytes: number;
    };
    memory_snapshot_cache_hits: number;
    memory_snapshot_cache_misses: number;
    memory_snapshot_cache_evictions: number;
  };
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
  input?: string | URL | Record<string, unknown> | (() => Record<string, unknown> | Promise<Record<string, unknown>>);
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

export class DdRuntimeClient {
  constructor(options?: DdRuntimeOptions);
  readonly generation: number;
  deploy(name: string, source: string, config?: DdDeployConfig): Promise<DdRuntimeDeployResult>;
  workerUrl(name: string): string;
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
  fetch(name: string, input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
  stats(name: string): Promise<DdRuntimeStatsResult>;
  request<T extends DdRuntimeCommandResult = DdRuntimeCommandResult>(
    command: Record<string, unknown>,
    options?: { timeoutMs?: number },
  ): Promise<T>;
  close(): Promise<void>;
}

export function createDdRuntime(options?: DdRuntimeOptions): DdRuntimeClient;
export function bundleWorkerEntry(
  entry: string | URL,
  options?: DdWorkerBundleOptions,
): Promise<string>;
export function createWorkerTestRuntime(options?: DdWorkerRuntimeOptions): Promise<{
  name: string;
  runtime: DdRuntimeClient;
  readonly deployment: DdRuntimeDeployResult | undefined;
  deploy(): Promise<DdRuntimeDeployResult>;
  reload(): Promise<DdRuntimeDeployResult>;
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
  stats(): Promise<DdRuntimeStatsResult>;
  close(): Promise<void>;
}>;
export function ddEnvironment(
  options?: DdWorkerRuntimeOptions & {
    viteEnvironment?: DdViteEnvironmentOptions;
    environmentOptions?: EnvironmentOptions;
  },
): EnvironmentOptions;
export function ddVitePlugin(options?: DdVitePluginOptions): Plugin;
export default ddVitePlugin;

declare module "virtual:dd-auxiliary-workers" {
  export const workers: Record<string, DdAuxiliaryWorkerRecord>;
  export default workers;
}
