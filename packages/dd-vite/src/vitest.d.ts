import type { DdRuntimeClient } from "./runtime.js";
import type { DdRuntimeDeployResult, DdRuntimeStatsResult, DdWorkerRuntimeOptions } from "./types.js";

export type { DdWorkerRuntimeOptions } from "./types.js";

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
