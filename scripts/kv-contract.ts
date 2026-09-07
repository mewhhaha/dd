import type {
  DdAdminStatusResponse,
  DdGeneratedDeploymentConfigOptions,
  DdKvNamespace,
  DdProjectConfig,
  DdRuntimeClient,
  DdRuntimeOptions,
} from "../packages/dd-vite/src/index.js";

export const projectConfig: DdProjectConfig = {
  schema_version: 1,
  name: "contract",
  entrypoint: "worker.js",
  egress_allow_hosts: ["api.example.com"],
};

export const deploymentConfig: DdGeneratedDeploymentConfigOptions = {
  serverModules: [{ path: "message.txt", file: "message.txt", type: "Text" }],
};

export const runtimeOptions: DdRuntimeOptions = { closeTimeoutMs: 1_000 };

export async function exerciseRuntimeContract(runtime: DdRuntimeClient, status: DdAdminStatusResponse) {
  const generation: number = runtime.generation;
  const workerSchedulers: number = status.runtime.worker_schedulers;
  const committedCommands: number = status.runtime.state_storage.committed_commands;
  const snapshotCacheHits: number = status.runtime.memory_snapshot_cache_hits;
  await runtime.request({ op: "stats", name: "contract" }, { timeoutMs: 1_000 });
  // @ts-expect-error Recency updates stay in RAM and have no persistent flush queue.
  status.runtime.cache_pending_recency_touches;
  return { generation, workerSchedulers, committedCommands, snapshotCacheHits };
}

export async function exerciseKvContract(kv: DdKvNamespace) {
  const committed: void = await kv.put("record", { count: 42 });
  const record: { count: number } | null = await kv.get<{ count: number }>("record");
  const entries: Array<{ key: string; value: { count: number } }> =
    await kv.list<{ count: number }>({ prefix: "record", limit: 10 });
  const deleted: void = await kv.delete("record");

  // @ts-expect-error KV writes have one durability contract.
  await kv.put("record", "value", { durability: "queued" });
  // @ts-expect-error Retrieval returns the stored value without conversion options.
  await kv.get("record", { type: "text" });
  // @ts-expect-error The caller must specify or narrow the stored value type.
  const text: string = await kv.get("record");
  return { committed, record, entries, deleted, text };
}

export async function exerciseMemoryContract(memory: import("../packages/dd-vite/src/index.js").DdMemoryNamespace) {
  const counter = memory.get("user-1");
  const count: number = await counter.atomic((tx) => {
    const next = (tx.get<number>("count") ?? 0) + 1;
    tx.put("count", next);
    tx.emit("audit.count", { next });
    return next;
  }, { idempotencyKey: "command-1" });
  // @ts-expect-error Transactions cannot suspend.
  await counter.atomic(async (tx) => tx.get("count"));
  // @ts-expect-error Promise-like results cannot suspend a transaction either.
  await counter.atomic(() => ({ then(resolve: (value: number) => void) { resolve(1); } }));
  // @ts-expect-error Memory variables and implicit transaction scopes are removed.
  counter.tvar("count", 0);
  // @ts-expect-error Read and write operations require an explicit transaction.
  counter.write("count", count);
  return count;
}
