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
  | { type: "dynamic"; binding: string }
  | { type: "service"; binding: string; service: string };

export interface DdDeployConfig {
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
  public?: boolean;
  cache?: DdCacheConfig;
  bindings?: DdBinding[];
  internal?: DdInternalConfig;
}

export interface DdKvNamespace {
  get(key: string, options?: { type?: "text" }): Promise<string | null>;
  get<T>(key: string, options: { type: "json" }): Promise<T | null>;
  get(key: string, options: { type: "arrayBuffer" }): Promise<ArrayBuffer | null>;
  put(
    key: string,
    value: string | ArrayBuffer | ArrayBufferView,
    options?: { durability?: "queued" | "committed" },
  ): Promise<void>;
  delete(key: string): Promise<void>;
  list(options?: { prefix?: string; limit?: number }): Promise<{ keys: Array<{ name: string }> }>;
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

export interface DdMemoryVersionedValue<T> {
  value: T;
  version: number;
  encoding: string;
}

export interface DdMemoryVariable<T> {
  readonly key: string;
  read(): T;
  read(options: { withVersion: true }): T | DdMemoryVersionedValue<T>;
  write(value: T): T;
  modify(update: (value: T) => T): T;
  delete(): boolean;
}

export interface DdMemoryListEntry<T = string> {
  key: string;
  value: T;
  version: number;
}

export interface DdMemoryStub {
  readonly id: DdMemoryId;
  readonly binding: string;
  read<T = string>(key: string): Promise<T | null>;
  write<T>(key: string, value: T): Promise<T>;
  delete(key: string): Promise<boolean>;
  writeMany<T>(entries: Array<readonly [string, T] | { key: string; value: T }>): Promise<number>;
  list<T = string>(options?: { prefix?: string; limit?: number }): Promise<Array<DdMemoryListEntry<T>>>;
  var<T = string>(key: string): DdMemoryVariable<T | null>;
  tvar<T>(key: string, defaultValue: T): DdMemoryVariable<T>;
  emit(kind: string, payload?: object | string | number | boolean | null): void;
  atomic<T, Args extends readonly unknown[]>(
    callback: (...args: Args) => T | Promise<T>,
    ...args: Args
  ): Promise<T>;
  atomic<T, Args extends readonly unknown[]>(
    options: { idempotencyKey?: string; idempotency_key?: string },
    callback: (...args: Args) => T | Promise<T>,
    ...args: Args
  ): Promise<T>;
}

export interface DdMemoryNamespace {
  idFromName(name: string): DdMemoryId;
  get(id: DdMemoryId): DdMemoryStub;
}

export interface DdDynamicWorkerConfig {
  source?: string;
  entrypoint?: string;
  modules?: Record<string, string>;
  bindings?: DdBinding[];
  env?: Record<string, string | object>;
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
}

export interface DdDynamicWorkerNamespace {
  get(
    id: string,
    factory: () => DdDynamicWorkerConfig | Promise<DdDynamicWorkerConfig>,
  ): Promise<DdDynamicWorkerStub>;
  list(): Promise<string[]>;
  delete(id: string): Promise<boolean>;
}

export interface DdDynamicWorkerStub {
  readonly worker: string;
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
}

export interface DdServiceBinding {
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
}

export interface DdRuntimeDeployResult {
  type: "deploy";
  worker: string;
  deployment_id: string;
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
    cache_flush_failure_count: number;
    cache_pending_recency_touches: number;
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
  | { type: "invoke"; status: number; headers: Array<[string, string]>; body_base64: string }
  | { type: "websocket_open"; session_id: string; status: number; headers: Array<[string, string]>; body_base64: string }
  | { type: "websocket_frame"; status: number; headers: Array<[string, string]>; body_base64: string }
  | { type: "websocket_drain_frame"; frame: null | { status: number; headers: Array<[string, string]>; body_base64: string } }
  | { type: "websocket_wait_frame" }
  | { type: "websocket_close" }
  | { type: "shutdown" };

export interface DdRuntimeOptions {
  binary?: string;
  cwd?: string;
  env?: Record<string, string>;
  timeoutMs?: number;
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
  kind?: "dynamic" | "service";
  binding?: string;
  id?: string;
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
  kind: "dynamic" | "service";
  binding: string;
  id: string;
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
  deploy(name: string, source: string, config?: DdDeployConfig): Promise<DdRuntimeDeployResult>;
  invoke(
    name: string,
    request: {
      method?: string;
      url?: string;
      headers?: Array<[string, string]>;
      body_base64?: string;
      request_id?: string;
    },
  ): Promise<{ status: number; headers: Array<[string, string]>; body_base64: string }>;
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
  fetch(name: string, input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
  openWebSocket(
    name: string,
    request: {
      method?: string;
      url?: string;
      headers?: Array<[string, string]>;
      body_base64?: string;
      request_id?: string;
    },
  ): Promise<{ session_id: string; status: number; headers: Array<[string, string]>; body_base64: string }>;
  sendWebSocketFrame(
    name: string,
    sessionId: string,
    body: ArrayBuffer | ArrayBufferView | string,
    options?: { binary?: boolean },
  ): Promise<{ status: number; headers: Array<[string, string]>; body_base64: string }>;
  drainWebSocketFrame(
    name: string,
    sessionId: string,
  ): Promise<{
    frame: null | { status: number; headers: Array<[string, string]>; body_base64: string };
  }>;
  closeWebSocket(
    name: string,
    sessionId: string,
    options?: { code?: number; reason?: string },
  ): Promise<{ type: "websocket_close" }>;
  stats(name: string): Promise<DdRuntimeStatsResult>;
  request<T extends DdRuntimeCommandResult = DdRuntimeCommandResult>(command: Record<string, unknown>): Promise<T>;
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
  invoke(request: Parameters<DdRuntimeClient["invoke"]>[1]): ReturnType<DdRuntimeClient["invoke"]>;
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
