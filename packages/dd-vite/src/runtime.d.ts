import type { DdDeployConfig, DdRuntimeCommandResult, DdRuntimeDeployResult, DdRuntimeOptions, DdRuntimeStatsResult, DdWorkerBundleOptions, DdWorkerConsoleEvent } from "./types.js";

export type * from "./types.js";

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
export function printWorkerConsole(event: DdWorkerConsoleEvent): void;
export function formatWorkerConsole(worker: string, message: string): string;
export function bundleWorkerEntry(
  entry: string | URL,
  options?: DdWorkerBundleOptions,
): Promise<string>;
