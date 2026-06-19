import { createContext } from "react-router";

export type MemoryNamespace = {
  idFromName(name: string): unknown;
  get(id: unknown): MemoryShard;
};

type MemoryShard = {
  atomic<T>(callback: () => T): Promise<T>;
  tvar<T>(key: string, defaultValue: T): {
    read(): T;
    write(value: T): void;
  };
};

export type Env = {
  EXAMPLE_MEMORY: MemoryNamespace;
};

export type DdRequestContext = {
  readonly workerName: string;
  lastStmCount?: number;
  incrementStmRequestCount(): Promise<number>;
};

export const ddRequestContext = createContext<DdRequestContext>();

export function createDdRequestContext(env: Env, workerName: string): DdRequestContext {
  const requestContext: DdRequestContext = {
    workerName,
    async incrementStmRequestCount() {
      const memory = env.EXAMPLE_MEMORY.get(env.EXAMPLE_MEMORY.idFromName(workerName));
      const requests = memory.tvar("requests", 0);
      const count = await memory.atomic(() => {
        const next = Number(requests.read()) + 1;
        requests.write(next);
        return next;
      });
      requestContext.lastStmCount = count;
      return count;
    },
  };
  return requestContext;
}
