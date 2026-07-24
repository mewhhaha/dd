import { createContext } from "react-router";
import {
  createStorefront,
  storefrontSessionFromRequest,
  type StorefrontEnv,
} from "./storefront";

const DD_REQUEST_CONTEXTS = Symbol.for("dd.examples.vite-react-router.request-contexts");
const DD_REQUEST_CONTEXT_HEADER = "x-dd-request-context";

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

export function getDdRequestContext(
  context: { get<T>(context: { defaultValue?: T }): T },
  request: Request,
): DdRequestContext {
  try {
    return context.get(ddRequestContext);
  } catch (error) {
    const bridged = requestContextFromRequest(request);
    if (bridged) {
      return bridged;
    }
    throw error;
  }
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

export function registerDdRequestContext(context: DdRequestContext): string {
  const id = crypto.randomUUID();
  requestContexts().set(id, context);
  return id;
}

export function unregisterDdRequestContext(id: string): void {
  requestContexts().delete(id);
}

export function requestWithDdRequestContext(request: Request, id: string): Request {
  const headers = new Headers(request.headers);
  headers.set(DD_REQUEST_CONTEXT_HEADER, id);
  return new Request(request, { headers });
}

function requestContextFromRequest(request: Request): DdRequestContext | undefined {
  const id = request.headers.get(DD_REQUEST_CONTEXT_HEADER);
  return id ? requestContexts().get(id) : undefined;
}

function requestContexts(): Map<string, DdRequestContext> {
  const global = globalThis as typeof globalThis & {
    [DD_REQUEST_CONTEXTS]?: Map<string, DdRequestContext>;
  };
  global[DD_REQUEST_CONTEXTS] ??= new Map();
  return global[DD_REQUEST_CONTEXTS];
}
