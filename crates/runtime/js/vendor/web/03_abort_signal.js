// Copyright 2018-2026 the Deno authors. MIT license.

return (function () {
const { core, primordials } = __bootstrap;
const {
  ArrayPrototypePush,
  FunctionPrototypeApply,
  ObjectPrototypeIsPrototypeOf,
  SafeFinalizationRegistry,
  SafeSet,
  SafeSetIterator,
  SafeWeakRef,
  SetPrototypeAdd,
  SetPrototypeDelete,
  Symbol,
  SymbolFor,
  TypeError,
  WeakRefPrototypeDeref,
} = primordials;

const webidl = core.loadExtScript("ext:vendor/webidl/00_webidl.js");
const { assert } = core.loadExtScript("ext:vendor/web/00_infra.js");
const { createFilteredInspectProxy } = core.loadExtScript(
  "ext:web/console.js",
);
const {
  defineEventHandler,
  Event,
  EventTarget,
  listenerCount,
  setIsTrusted,
} = core.loadExtScript("ext:vendor/web/02_event.js");

const add = Symbol("[[add]]");
const signalAbort = Symbol("[[signalAbort]]");
const remove = Symbol("[[remove]]");
const runAbortSteps = Symbol("[[runAbortSteps]]");
const abortReason = Symbol("[[abortReason]]");
const abortAlgos = Symbol("[[abortAlgos]]");
const dependent = Symbol("[[dependent]]");
const sourceSignals = Symbol("[[sourceSignals]]");
const dependentSignals = Symbol("[[dependentSignals]]");
const signal = Symbol("[[signal]]");
const activeDependents = Symbol("[[activeDependents]]");

const illegalConstructorKey = Symbol("illegalConstructorKey");

// When a dependent signal created by AbortSignal.any() is GC'd, clean up
// its WeakRef from each source signal's dependentSignals set.
// This prevents unbounded growth of the set when .any() is called in a loop.
const dependentSignalCleanupRegistry = new SafeFinalizationRegistry(
  (prevent) => {
    const sourceSignal = WeakRefPrototypeDeref(prevent.ref);
    if (sourceSignal === undefined) return;
    const deps = sourceSignal[dependentSignals];
    if (deps === null) return;
    SetPrototypeDelete(deps, prevent.weak);
  },
);

class AbortSignal extends EventTarget {
  [abortReason] = undefined;
  [abortAlgos] = null;
  [dependent] = false;
  [sourceSignals] = null;
  [dependentSignals] = null;
  [webidl.brand] = webidl.brand;

  static any(signals) {
    const prefix = "Failed to execute 'AbortSignal.any'";
    webidl.requiredArguments(arguments.length, 1, prefix);
    return createDependentAbortSignal(signals, prefix);
  }

  static abort(reason = undefined) {
    if (reason !== undefined) {
      reason = webidl.converters.any(reason);
    }
    const signal = new AbortSignal(illegalConstructorKey);
    signal[signalAbort](reason);
    return signal;
  }

  static timeout(millis) {
    const prefix = "Failed to execute 'AbortSignal.timeout'";
    webidl.requiredArguments(arguments.length, 1, prefix);
    millis = webidl.converters["unsigned long long"](
      millis,
      prefix,
      "Argument 1",
      {
        enforceRange: true,
      },
    );

    const signal = new AbortSignal(illegalConstructorKey);
    core.createSystemTimer(
      () => {
        signal[signalAbort](
          new DOMException(
            "The operation was aborted due to timeout",
            "TimeoutError",
          ),
        );
      },
      millis,
    );
    return signal;
  }

  [add](algorithm) {
    if (this.aborted) {
      return;
    }
    this[abortAlgos] ??= new SafeSet();
    SetPrototypeAdd(this[abortAlgos], algorithm);
  }

  [signalAbort](
    reason = new DOMException("The signal has been aborted", "AbortError"),
  ) {
    if (this.aborted) {
      return;
    }
    this[abortReason] = reason;

    const dependentSignalsToAbort = [];
    if (this[dependentSignals] !== null) {
      for (const weakRef of new SafeSetIterator(this[dependentSignals])) {
        const dependentSignal = WeakRefPrototypeDeref(weakRef);
        if (
          dependentSignal !== undefined &&
          dependentSignal[abortReason] === undefined
        ) {
          dependentSignal[abortReason] = this[abortReason];
          ArrayPrototypePush(dependentSignalsToAbort, dependentSignal);
        }
      }
    }

    this[runAbortSteps]();

    if (dependentSignalsToAbort.length !== 0) {
      for (let i = 0; i < dependentSignalsToAbort.length; ++i) {
        const dependentSignal = dependentSignalsToAbort[i];
        dependentSignal[runAbortSteps]();
      }
    }
  }

  [runAbortSteps]() {
    const algos = this[abortAlgos];
    this[abortAlgos] = null;

    if (algos !== null) {
      for (const algorithm of new SafeSetIterator(algos)) {
        algorithm();
      }
    }

    if (listenerCount(this, "abort") > 0) {
      const event = new Event("abort");
      setIsTrusted(event, true);
      super.dispatchEvent(event);
    }

    // release strong references from source signals now that abort has been delivered
    if (this[sourceSignals] !== null) {
      for (const weakRef of new SafeSetIterator(this[sourceSignals])) {
        const sourceSignal = WeakRefPrototypeDeref(weakRef);
        if (sourceSignal !== undefined && sourceSignal[activeDependents]) {
          SetPrototypeDelete(sourceSignal[activeDependents], this);
        }
      }
    }
  }

  [remove](algorithm) {
    this[abortAlgos] && SetPrototypeDelete(this[abortAlgos], algorithm);
  }

  constructor(key = null) {
    if (key !== illegalConstructorKey) {
      throw new TypeError("Illegal constructor");
    }
    super();
  }

  get aborted() {
    webidl.assertBranded(this, AbortSignalPrototype);
    return this[abortReason] !== undefined;
  }

  get reason() {
    webidl.assertBranded(this, AbortSignalPrototype);
    return this[abortReason];
  }

  throwIfAborted() {
    webidl.assertBranded(this, AbortSignalPrototype);
    if (this[abortReason] !== undefined) {
      throw this[abortReason];
    }
  }

  // While a dependent signal (one from AbortSignal.any) has 'abort'
  // listeners, it is kept strongly reachable from each source signal so that
  // an upstream abort can still reach it. The source-to-dependent edge stored
  // in `dependentSignals` is a WeakRef; without this strong backref, a GC
  // between `AbortSignal.any(...)` and `source.abort()` would collect the
  // dependent and the abort would silently fail to propagate.
  addEventListener() {
    FunctionPrototypeApply(super.addEventListener, this, arguments);
    if (listenerCount(this, "abort") > 0 && this[sourceSignals] !== null) {
      for (const weakRef of new SafeSetIterator(this[sourceSignals])) {
        const sourceSignal = WeakRefPrototypeDeref(weakRef);
        if (sourceSignal === undefined) continue;
        sourceSignal[activeDependents] ??= new SafeSet();
        SetPrototypeAdd(sourceSignal[activeDependents], this);
      }
    }
  }

  removeEventListener() {
    FunctionPrototypeApply(super.removeEventListener, this, arguments);
    if (listenerCount(this, "abort") === 0 && this[sourceSignals] !== null) {
      for (const weakRef of new SafeSetIterator(this[sourceSignals])) {
        const sourceSignal = WeakRefPrototypeDeref(weakRef);
        if (sourceSignal === undefined) continue;
        // Release the strong backref that addEventListener installed; the
        // dependent is now free to be GC'd if nothing else holds it.
        if (sourceSignal[activeDependents]) {
          SetPrototypeDelete(sourceSignal[activeDependents], this);
        }
      }
    }
  }

  [SymbolFor("Deno.privateCustomInspect")](inspect, inspectOptions) {
    return inspect(
      createFilteredInspectProxy({
        object: this,
        evaluate: ObjectPrototypeIsPrototypeOf(AbortSignalPrototype, this),
        keys: [
          "aborted",
          "reason",
          "onabort",
        ],
      }),
      inspectOptions,
    );
  }
}
defineEventHandler(AbortSignal.prototype, "abort");

webidl.configureInterface(AbortSignal);
const AbortSignalPrototype = AbortSignal.prototype;

class AbortController {
  [signal] = new AbortSignal(illegalConstructorKey);

  constructor() {
    this[webidl.brand] = webidl.brand;
  }

  get signal() {
    webidl.assertBranded(this, AbortControllerPrototype);
    return this[signal];
  }

  abort(reason) {
    webidl.assertBranded(this, AbortControllerPrototype);
    this[signal][signalAbort](reason);
  }

  [SymbolFor("Deno.privateCustomInspect")](inspect, inspectOptions) {
    return inspect(
      createFilteredInspectProxy({
        object: this,
        evaluate: ObjectPrototypeIsPrototypeOf(AbortControllerPrototype, this),
        keys: [
          "signal",
        ],
      }),
      inspectOptions,
    );
  }
}

webidl.configureInterface(AbortController);
const AbortControllerPrototype = AbortController.prototype;

webidl.converters.AbortSignal = webidl.createInterfaceConverter(
  "AbortSignal",
  AbortSignal.prototype,
);
webidl.converters["sequence<AbortSignal>"] = webidl.createSequenceConverter(
  webidl.converters.AbortSignal,
);

function newSignal() {
  return new AbortSignal(illegalConstructorKey);
}

function createDependentAbortSignal(signals, prefix) {
  signals = webidl.converters["sequence<AbortSignal>"](
    signals,
    prefix,
    "Argument 1",
  );

  const resultSignal = new AbortSignal(illegalConstructorKey);
  for (let i = 0; i < signals.length; ++i) {
    const signal = signals[i];
    if (signal[abortReason] !== undefined) {
      resultSignal[abortReason] = signal[abortReason];
      return resultSignal;
    }
  }

  resultSignal[dependent] = true;
  resultSignal[sourceSignals] = new SafeSet();
  for (let i = 0; i < signals.length; ++i) {
    const signal = signals[i];
    if (!signal[dependent]) {
      signal[dependentSignals] ??= new SafeSet();
      const signalRef = new SafeWeakRef(signal);
      const resultRef = new SafeWeakRef(resultSignal);
      SetPrototypeAdd(resultSignal[sourceSignals], signalRef);
      SetPrototypeAdd(signal[dependentSignals], resultRef);
      // When resultSignal is GC'd, remove its WeakRef from signal's dependentSignals
      dependentSignalCleanupRegistry.register(resultSignal, {
        ref: signalRef,
        weak: resultRef,
      });
    } else {
      for (
        const sourceSignalRef of new SafeSetIterator(
          signal[sourceSignals],
        )
      ) {
        const sourceSignal = WeakRefPrototypeDeref(sourceSignalRef);
        if (sourceSignal === undefined) continue;
        assert(sourceSignal[abortReason] === undefined);
        assert(!sourceSignal[dependent]);

        // Check if already tracking this source
        let alreadyTracked = false;
        for (
          const existingRef of new SafeSetIterator(
            resultSignal[sourceSignals],
          )
        ) {
          if (WeakRefPrototypeDeref(existingRef) === sourceSignal) {
            alreadyTracked = true;
            break;
          }
        }
        if (alreadyTracked) continue;

        sourceSignal[dependentSignals] ??= new SafeSet();
        const newSourceRef = new SafeWeakRef(sourceSignal);
        const resultRef = new SafeWeakRef(resultSignal);
        SetPrototypeAdd(resultSignal[sourceSignals], newSourceRef);
        SetPrototypeAdd(sourceSignal[dependentSignals], resultRef);
        dependentSignalCleanupRegistry.register(resultSignal, {
          ref: newSourceRef,
          weak: resultRef,
        });
      }
    }
  }

  return resultSignal;
}

return {
  AbortController,
  AbortSignal,
  AbortSignalPrototype,
  add,
  createDependentAbortSignal,
  newSignal,
  remove,
  signalAbort,
};
})();
