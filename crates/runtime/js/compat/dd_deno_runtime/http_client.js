import { core, primordials } from "ext:core/mod.js";

const { internalRidSymbol } = core;
const { ObjectDefineProperty } = primordials;

const HttpClient = class HttpClient {
  #rid;

  constructor(rid) {
    ObjectDefineProperty(this, internalRidSymbol, {
      enumerable: false,
      value: rid,
    });
    this.#rid = rid;
  }

  close() {
    core.tryClose(this.#rid);
  }
};

export { HttpClient };
export const HttpClientPrototype = HttpClient.prototype;
