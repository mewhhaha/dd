// The WPT runner's side of a test isolate. The runner evaluates this file as
// a function expression and calls it with the test's settings before it
// loads testharness.js, then drives the test through the hooks it leaves on
// `globalThis.__wpt_runner` (not enumerable).
//
// It prepares dd's worker global scope the way wptserve's classic worker
// wrapper (`.any.worker.js`) and a dedicated worker would: `GLOBAL`,
// `META_TITLE`, a `location`, `importScripts` for `.worker.js` tests, and the
// test data files tests load with fetch. Everything else is dd's own.
(function (config) {
  "use strict";

  const global = globalThis;
  // Captured before any test can replace them.
  const { Request, Response, URL } = global;
  const stringify = JSON.stringify;
  const define = (name, value) => {
    Object.defineProperty(global, name, {
      value,
      writable: true,
      configurable: true,
      enumerable: false,
    });
  };

  global.GLOBAL = {
    isWindow() {
      return false;
    },
    isWorker() {
      return true;
    },
    isShadowRealm() {
      return false;
    },
  };
  if (config.title !== null) {
    global.META_TITLE = config.title;
  }

  // A worker's location is the URL its script was served from.
  const testUrl = new URL(config.url);
  if (!("location" in global)) {
    const location = {};
    for (const key of [
      "href",
      "origin",
      "protocol",
      "host",
      "hostname",
      "port",
      "pathname",
      "search",
      "hash",
    ]) {
      location[key] = testUrl[key];
    }
    location.toString = () => testUrl.href;
    define("location", Object.freeze(location));
  }

  // testharness.js turns uncaught errors and unhandled rejections into
  // harness errors through listeners it adds on the global scope when it
  // loads. dd's global scope is not an EventTarget, so this collects those
  // listeners while testharness.js loads and `attach` removes the stand-in.
  const listeners = { error: [], unhandledrejection: [] };
  const standInAddEventListener = !("addEventListener" in global);
  if (standInAddEventListener) {
    define("addEventListener", (type, listener) => {
      if (type in listeners) {
        listeners[type].push(listener);
      }
    });
  }

  // `.worker.js` tests import their helpers with importScripts. The runner
  // has already run every script such a test names, in order, before the
  // test itself, so a call only checks that it was one of them.
  if (config.importedScripts !== null) {
    const imported = new Set(config.importedScripts);
    define("importScripts", (...urls) => {
      for (const url of urls) {
        const path = new URL(String(url), testUrl).pathname;
        if (!imported.has(path)) {
          throw new DOMException(`importScripts: ${path} was not preloaded`, "NetworkError");
        }
      }
    });
  }

  // Tests load their data (urltestdata.json, the IDL fragments idlharness
  // reads, ...) with fetch from the server that serves them. There is no
  // server here: a GET for a .json or .idl file next to the tests is read
  // from the checkout by the runner. Every other fetch is dd's own, and so
  // is every fetch of fetch's own tests.
  const dataFile = /\.(json|idl)$/;
  const runtimeFetch = global.fetch;
  const pendingFetches = new Map();
  const requestedFetches = [];
  let nextFetchId = 1;
  if (config.serveDataFiles) {
    define("fetch", function fetch(input, init = undefined) {
      let url;
      try {
        url = new URL(input instanceof Request ? input.url : String(input), testUrl);
      } catch {
        return runtimeFetch.call(this, input, init);
      }
      const method = String(init?.method ?? (input instanceof Request ? input.method : "GET"));
      if (
        url.origin !== testUrl.origin ||
        method.toUpperCase() !== "GET" ||
        !dataFile.test(url.pathname)
      ) {
        return runtimeFetch.call(this, input, init);
      }
      return new Promise((resolve, reject) => {
        const id = nextFetchId++;
        pendingFetches.set(id, { resolve, reject });
        requestedFetches.push({ id, path: decodeURIComponent(url.pathname) });
      });
    });
  }

  const decodeBase64 = (text) => {
    const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    const values = new Uint8Array(128);
    for (let index = 0; index < alphabet.length; index++) {
      values[alphabet.charCodeAt(index)] = index;
    }
    const clean = text.replace(/=+$/, "");
    const bytes = new Uint8Array(Math.floor((clean.length * 3) / 4));
    let buffer = 0;
    let bits = 0;
    let offset = 0;
    for (let index = 0; index < clean.length; index++) {
      buffer = (buffer << 6) | values[clean.charCodeAt(index)];
      bits += 6;
      if (bits >= 8) {
        bits -= 8;
        bytes[offset++] = (buffer >> bits) & 0xff;
      }
    }
    return bytes;
  };

  let complete = null;
  const finished = [];
  const testResult = (test) => ({
    name: String(test.name),
    status: test.status,
    message: test.message === null || test.message === undefined ? null : String(test.message),
  });
  let harnessTimeout = null;

  const runner = {
    // Called once testharness.js has loaded.
    attach() {
      if (standInAddEventListener) {
        delete global.addEventListener;
      }
      harnessTimeout = global.timeout;
      global.add_result_callback((test) => {
        finished.push(testResult(test));
      });
      global.add_completion_callback((tests, status) => {
        complete = {
          harness: {
            status: status.status,
            message: status.message === null || status.message === undefined
              ? null
              : String(status.message),
          },
          tests: Array.prototype.map.call(tests, testResult),
        };
      });
    },

    // What the runner has to do next: "done" once the harness completed,
    // otherwise a JSON list of data files to read (empty when none).
    poll() {
      if (complete !== null) {
        return "done";
      }
      if (requestedFetches.length === 0) {
        return "";
      }
      return stringify(requestedFetches.splice(0));
    },

    // Answers a fetch `poll` asked for: the file's bytes as base64, or null
    // when it does not exist.
    respond(id, base64) {
      const pending = pendingFetches.get(id);
      pendingFetches.delete(id);
      if (base64 === null) {
        pending.resolve(new Response("not found", { status: 404, statusText: "Not Found" }));
        return;
      }
      pending.resolve(new Response(decodeBase64(base64), { status: 200 }));
    },

    // Reports an exception a script threw as an `error` event would.
    uncaughtError(message) {
      const event = { type: "error", message, error: { message, stack: message } };
      for (const listener of listeners.error) {
        listener(event);
      }
    },

    // Reports a rejection nothing handled as an `unhandledrejection` event
    // would.
    unhandledRejection(message) {
      const event = { type: "unhandledrejection", reason: { message, stack: message } };
      for (const listener of listeners.unhandledrejection) {
        listener(event);
      }
    },

    // The per-file timeout passed: time the harness out so its incomplete
    // tests report TIMEOUT.
    timeout() {
      if (typeof harnessTimeout === "function") {
        harnessTimeout();
      }
    },

    // The results as JSON: the harness's own once it completed, otherwise
    // the subtests that finished so far.
    results() {
      return stringify(complete ?? { harness: null, tests: finished });
    },
  };
  define("__wpt_runner", runner);
})
