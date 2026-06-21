import { Effect } from "effect";
import { badRequest, type AppError } from "./errors";
import { currentRequest, type RequestContext } from "./runtime";

const SESSION_COOKIE = "vite_effect_session";

export function htmlResponse(body: string, init?: ResponseInit): Response {
  return new Response(body, {
    ...init,
    headers: {
      "content-type": "text/html; charset=utf-8",
      ...init?.headers,
    },
  });
}

export function jsonResponse(value: unknown, init?: ResponseInit): Response {
  return new Response(JSON.stringify(value, null, 2), {
    ...init,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...init?.headers,
    },
  });
}

export function textResponse(body: string, init?: ResponseInit): Response {
  return new Response(body, init);
}

export function redirectResponse(location: string, headers: Record<string, string> = {}): Response {
  return new Response(null, {
    status: 303,
    headers: {
      location,
      ...headers,
    },
  });
}

export function errorResponse(error: AppError): Response {
  return jsonResponse({
    error: error.code,
    message: error.message,
  }, { status: error.status });
}

export function requestJson<T>(): Effect.Effect<T, AppError, RequestContext> {
  return currentRequest.pipe(
    Effect.flatMap((request) => Effect.tryPromise({
      try: () => request.json() as Promise<T>,
      catch: (error) => badRequest(`Invalid JSON body: ${String(error)}`),
    })),
  );
}

export function sessionCookie(request: Request): string | null {
  return parseCookieHeader(request.headers.get("cookie") ?? "").get(SESSION_COOKIE) ?? null;
}

export function serializeSessionCookie(sessionId: string): string {
  return `${SESSION_COOKIE}=${encodeURIComponent(sessionId)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=3600`;
}

export function expireSessionCookie(): string {
  return `${SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`;
}

function parseCookieHeader(header: string): Map<string, string> {
  const cookies = new Map<string, string>();
  for (const part of header.split(";")) {
    const [name, ...valueParts] = part.trim().split("=");
    if (!name || valueParts.length === 0) {
      continue;
    }
    cookies.set(name, decodeURIComponent(valueParts.join("=")));
  }
  return cookies;
}
