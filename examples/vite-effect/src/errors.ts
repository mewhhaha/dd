import { Data } from "effect";

export class BadRequest extends Data.TaggedError("BadRequest")<{ readonly message: string }> {
  readonly status = 400;
  readonly code = "bad_request";
}

export class Unauthorized extends Data.TaggedError("Unauthorized")<{ readonly message: string }> {
  readonly status = 401;
  readonly code = "unauthorized";
}

export class Forbidden extends Data.TaggedError("Forbidden")<{ readonly message: string }> {
  readonly status = 403;
  readonly code = "forbidden";
}

export class NotFound extends Data.TaggedError("NotFound")<{ readonly message: string }> {
  readonly status = 404;
  readonly code = "not_found";
}

export class RateLimited extends Data.TaggedError("RateLimited")<{ readonly message: string }> {
  readonly status = 429;
  readonly code = "rate_limited";
}

export class InternalError extends Data.TaggedError("InternalError")<{ readonly message: string }> {
  readonly status = 500;
  readonly code = "internal_error";
}

export type AppError =
  | BadRequest
  | Unauthorized
  | Forbidden
  | NotFound
  | RateLimited
  | InternalError;

export function badRequest(message: string): AppError {
  return new BadRequest({ message });
}

export function unauthorized(message: string): AppError {
  return new Unauthorized({ message });
}

export function forbidden(message: string): AppError {
  return new Forbidden({ message });
}

export function notFound(message: string): AppError {
  return new NotFound({ message });
}

export function tooManyRequests(message: string): AppError {
  return new RateLimited({ message });
}

export function internalError(message: string): AppError {
  return new InternalError({ message });
}
