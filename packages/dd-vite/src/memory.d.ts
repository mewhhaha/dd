import type { DdMemorySnapshot, DdMemoryStub } from "./index.js";

export interface DdMemoryChanges<Result> {
  writes: ReadonlyArray<{ key: string; value: unknown }>;
  deletes?: ReadonlyArray<string>;
  effects?: ReadonlyArray<{ kind: string; payload?: unknown }>;
  result: Result & (Result extends { then: (...args: never[]) => unknown } ? never : unknown);
}

export function memoryCommand<Input, Result>(
  memory: DdMemoryStub,
  transition: (snapshot: DdMemorySnapshot, input: Input) => DdMemoryChanges<Result>,
): (input: Input, options?: { idempotencyKey?: string }) => Promise<Result>;
