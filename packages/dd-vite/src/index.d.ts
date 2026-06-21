import type { EnvironmentOptions, InlineConfig, Plugin } from "vite";

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
  config?: unknown;
  runtime?: DdRuntimeClient;
  runtimeOptions?: DdRuntimeOptions;
  autoDeploy?: boolean;
}

export interface DdAuxiliaryWorkerOptions extends DdWorkerBundleOptions {
  name: string;
  binding?: string;
  id?: string;
  entry?: string | URL;
  source?: string | (() => string | Promise<string>);
  config?: Record<string, unknown>;
}

export interface DdAuxiliaryWorkerRecord {
  name: string;
  binding: string;
  id: string;
  config: Record<string, unknown>;
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
  environmentName?: string;
  environmentOptions?: EnvironmentOptions;
  reloadOnHotUpdate?: boolean | "all" | "entry";
  devModuleRunner?: boolean;
  deploymentConfig?: false | DdGeneratedDeploymentConfigOptions;
  auxiliaryWorkers?: DdAuxiliaryWorkerOptions[];
  eager?: boolean;
}

export class DdRuntimeClient {
  constructor(options?: DdRuntimeOptions);
  deploy(name: string, source: string, config?: unknown): Promise<unknown>;
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
  ): Promise<unknown>;
  stats(name: string): Promise<unknown>;
  request(command: Record<string, unknown>): Promise<unknown>;
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
  readonly deployment: unknown;
  deploy(): Promise<unknown>;
  reload(): Promise<unknown>;
  invoke(request: Parameters<DdRuntimeClient["invoke"]>[1]): ReturnType<DdRuntimeClient["invoke"]>;
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
  stats(): Promise<unknown>;
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
