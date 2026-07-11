declare module "node:async_hooks" {
  export class AsyncLocalStorage<T> {
    run<R>(store: T, callback: (...args: never[]) => R, ...args: never[]): R;
    getStore(): T | undefined;
    enterWith(store: T): void;
    disable(): void;
  }
}
