(function () {
  const { core, primordials } = __bootstrap;

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

  return { HttpClient, HttpClientPrototype: HttpClient.prototype };
})();
