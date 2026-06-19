export class AsyncLocalStorage<T = unknown> {
  #store: T | undefined;

  run<R>(store: T, callback: (...args: never[]) => R, ...args: never[]): R {
    const previous = this.#store;
    this.#store = store;
    try {
      return callback(...args);
    } finally {
      this.#store = previous;
    }
  }

  getStore(): T | undefined {
    return this.#store;
  }

  enterWith(store: T): void {
    this.#store = store;
  }

  disable(): void {
    this.#store = undefined;
  }
}
