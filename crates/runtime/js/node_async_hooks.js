const asyncContext = globalThis.__dd_async_context;

export class AsyncLocalStorage {
  #storage;

  run(store, callback, ...args) {
    this.#storage ??= {};
    return asyncContext.runWithAsyncLocalStore(this.#storage, store, callback, ...args);
  }

  getStore() {
    if (!this.#storage) {
      return undefined;
    }
    return asyncContext.getAsyncLocalStore(this.#storage);
  }

  enterWith(store) {
    this.#storage ??= {};
    asyncContext.enterWithAsyncLocalStore(this.#storage, store);
  }

  disable() {
    if (!this.#storage) {
      return;
    }
    asyncContext.disableAsyncLocalStore(this.#storage);
    this.#storage = undefined;
  }
}
