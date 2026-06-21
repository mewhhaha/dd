import { AsyncLocalStorage } from "node:async_hooks";
import { createContext } from "react-router";
import {
  createStorefront,
  storefrontSessionFromRequest,
  type StorefrontEnv,
} from "./storefront";

export type Env = StorefrontEnv;

export type DdRequestContext = {
  readonly workerName: string;
  readonly sessionCookie: string | null;
  readonly storefront: ReturnType<typeof createStorefront>;
  lastStmCount?: number;
  incrementStmRequestCount(): Promise<number>;
  sampleTimerTick(): Promise<string>;
};

export const ddRequestContext = createContext<DdRequestContext>();
const ddRequestStorage = new AsyncLocalStorage<DdRequestContext>();

export function runWithDdRequestContext<T>(
  context: DdRequestContext,
  callback: () => T,
): T {
  return ddRequestStorage.run(context, callback);
}

export function getCurrentDdRequestContext(): DdRequestContext {
  const context = ddRequestStorage.getStore();
  if (!context) {
    throw new Error("dd request context is not available");
  }
  return context;
}

export function createDdRequestContext(
  env: Env,
  workerName: string,
  request: Request,
): DdRequestContext {
  const session = storefrontSessionFromRequest(request);
  const requestContext: DdRequestContext = {
    workerName,
    sessionCookie: session.setCookie,
    storefront: createStorefront(env, session.id, workerName),
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
    async sampleTimerTick() {
      const started = Date.now();
      await new Promise((resolve) => setTimeout(resolve, 1));
      return `${Math.max(0, Date.now() - started)} ms`;
    },
  };
  return requestContext;
}
