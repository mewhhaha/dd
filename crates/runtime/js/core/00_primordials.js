// Copyright 2018-2026 the Deno authors. MIT license.

// Based on https://github.com/nodejs/node/blob/889ad35d3d41e376870f785b0c1b669cb732013d/lib/internal/per_context/primordials.js
// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.
// This file subclasses and stores the JS builtins that come from the VM
// so that Node.js's builtin modules do not need to later look these up from
// the global proxy, which can be mutated by users.

// Use of primordials have sometimes a dramatic impact on performance, please
// benchmark all changes made in performance-sensitive areas of the codebase.
// See: https://github.com/nodejs/node/pull/38248

// deno-lint-ignore-file deno-internal/prefer-primordials

"use strict";

(() => {
  const primordials = {};

  const {
    defineProperty: ReflectDefineProperty,
    getOwnPropertyDescriptor: ReflectGetOwnPropertyDescriptor,
    ownKeys: ReflectOwnKeys,
  } = Reflect;

  // `uncurryThis` is equivalent to `func => Function.prototype.call.bind(func)`.
  // It is using `bind.bind(call)` to avoid using `Function.prototype.bind`
  // and `Function.prototype.call` after it may have been mutated by users.
  const { apply, bind, call } = Function.prototype;
  const uncurryThis = bind.bind(call);
  primordials.uncurryThis = uncurryThis;

  // `applyBind` is equivalent to `func => Function.prototype.apply.bind(func)`.
  // It is using `bind.bind(apply)` to avoid using `Function.prototype.bind`
  // and `Function.prototype.apply` after it may have been mutated by users.
  const applyBind = bind.bind(apply);
  primordials.applyBind = applyBind;

  const getGetter = (proto, key) => {
    const desc = ReflectGetOwnPropertyDescriptor(proto, key);
    return desc && desc.get ? uncurryThis(desc.get) : undefined;
  };
  const maybeUncurry = (fn) => (typeof fn === "function" ? uncurryThis(fn) : undefined);

  const TypedArray = Reflect.getPrototypeOf(Uint8Array);
  const TypedArrayPrototype = TypedArray.prototype;
  const ArrayIteratorPrototype = Reflect.getPrototypeOf(Array.prototype[Symbol.iterator]());
  const SetIteratorPrototype = Reflect.getPrototypeOf(new Set()[Symbol.iterator]());
  const MapIteratorPrototype = Reflect.getPrototypeOf(new Map()[Symbol.iterator]());
  const StringIteratorPrototype = Reflect.getPrototypeOf(String.prototype[Symbol.iterator]());
  const AsyncGeneratorPrototype = Reflect.getPrototypeOf(async function* () {}).prototype;

  // Directly capture every built-in and uncurried prototype method used by the
  // runtime and vendored web scripts without reflecting ~1,000 unused keys and
  // allocating ~800 dead BoundFunctions on every isolate cold start.
  primordials.globalThis = globalThis;
  primordials.Proxy = Proxy;
  primordials.isNaN = isNaN;
  primordials.decodeURI = decodeURI;
  primordials.decodeURIComponent = decodeURIComponent;
  primordials.encodeURI = encodeURI;
  primordials.encodeURIComponent = encodeURIComponent;

  primordials.AggregateError = AggregateError;
  primordials.Array = Array;
  primordials.ArrayBuffer = ArrayBuffer;
  primordials.BigInt = BigInt;
  primordials.BigInt64Array = BigInt64Array;
  primordials.BigUint64Array = BigUint64Array;
  primordials.Boolean = Boolean;
  primordials.DataView = DataView;
  primordials.Date = Date;
  primordials.Error = Error;
  primordials.EvalError = EvalError;
  primordials.FinalizationRegistry = FinalizationRegistry;
  primordials.Float32Array = Float32Array;
  primordials.Float64Array = Float64Array;
  primordials.Function = Function;
  primordials.Int16Array = Int16Array;
  primordials.Int32Array = Int32Array;
  primordials.Int8Array = Int8Array;
  primordials.Map = Map;
  primordials.Number = Number;
  const _Promise = globalThis.Promise;
  primordials.Object = Object;
  primordials.Promise = _Promise;
  primordials.RangeError = RangeError;
  primordials.ReferenceError = ReferenceError;
  primordials.RegExp = RegExp;
  primordials.Set = Set;
  primordials.String = String;
  primordials.Symbol = Symbol;
  primordials.SyntaxError = SyntaxError;
  primordials.TypeError = TypeError;
  primordials.URIError = URIError;
  primordials.Uint16Array = Uint16Array;
  primordials.Uint32Array = Uint32Array;
  primordials.Uint8Array = Uint8Array;
  primordials.Uint8ClampedArray = Uint8ClampedArray;
  primordials.WeakMap = WeakMap;
  primordials.WeakRef = WeakRef;
  primordials.WeakSet = WeakSet;
  primordials.TypedArray = TypedArray;

  primordials.ArrayIteratorPrototype = ArrayIteratorPrototype;
  primordials.AsyncGeneratorPrototype = AsyncGeneratorPrototype;
  primordials.ErrorPrototype = Error.prototype;
  primordials.ObjectPrototype = Object.prototype;
  primordials.PromisePrototype = _Promise.prototype;
  primordials.TypeErrorPrototype = TypeError.prototype;

  // Static methods & constants
  primordials.ArrayBufferIsView = ArrayBuffer.isView;
  primordials.ArrayFrom = Array.from;
  primordials.ArrayIsArray = Array.isArray;
  primordials.BigIntAsIntN = BigInt.asIntN;
  primordials.BigIntAsUintN = BigInt.asUintN;
  primordials.DateNow = Date.now;
  primordials.ErrorCaptureStackTrace = Error.captureStackTrace;
  primordials.JSONParse = JSON.parse;
  primordials.JSONStringify = JSON.stringify;
  primordials.MathCeil = Math.ceil;
  primordials.MathFloor = Math.floor;
  primordials.MathFround = Math.fround;
  primordials.MathMax = Math.max;
  primordials.MathMin = Math.min;
  primordials.MathPow = Math.pow;
  primordials.MathRandom = Math.random;
  primordials.MathRound = Math.round;
  primordials.MathTrunc = Math.trunc;
  primordials.NumberIsFinite = Number.isFinite;
  primordials.NumberIsInteger = Number.isInteger;
  primordials.NumberIsNaN = Number.isNaN;
  primordials.NumberMAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER;
  primordials.NumberMIN_SAFE_INTEGER = Number.MIN_SAFE_INTEGER;
  primordials.NumberParseFloat = Number.parseFloat;
  primordials.NumberParseInt = Number.parseInt;
  primordials.ObjectAssign = Object.assign;
  primordials.ObjectCreate = Object.create;
  primordials.ObjectDefineProperties = Object.defineProperties;
  primordials.ObjectDefineProperty = Object.defineProperty;
  primordials.ObjectEntries = Object.entries;
  primordials.ObjectFreeze = Object.freeze;
  primordials.ObjectFromEntries = Object.fromEntries;
  primordials.ObjectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
  primordials.ObjectGetOwnPropertyDescriptors = Object.getOwnPropertyDescriptors;
  primordials.ObjectGetOwnPropertySymbols = Object.getOwnPropertySymbols;
  primordials.ObjectGetPrototypeOf = Object.getPrototypeOf;
  primordials.ObjectHasOwn = Object.hasOwn;
  primordials.ObjectIs = Object.is;
  primordials.ObjectKeys = Object.keys;
  primordials.ObjectSetPrototypeOf = Object.setPrototypeOf;
  primordials.PromiseReject = _Promise.reject.bind(_Promise);
  primordials.PromiseResolve = _Promise.resolve.bind(_Promise);
  primordials.ReflectApply = Reflect.apply;
  primordials.ReflectConstruct = Reflect.construct;
  primordials.ReflectDefineProperty = ReflectDefineProperty;
  primordials.ReflectGetOwnPropertyDescriptor = ReflectGetOwnPropertyDescriptor;
  primordials.ReflectGetPrototypeOf = Reflect.getPrototypeOf;
  primordials.ReflectHas = Reflect.has;
  primordials.ReflectOwnKeys = ReflectOwnKeys;
  primordials.StringFromCharCode = String.fromCharCode;
  primordials.SymbolAsyncIterator = Symbol.asyncIterator;
  primordials.SymbolFor = Symbol.for;
  primordials.SymbolIterator = Symbol.iterator;
  primordials.SymbolToStringTag = Symbol.toStringTag;

  // Uncurried prototype methods & accessors
  primordials.ArrayBufferPrototypeGetByteLength = getGetter(ArrayBuffer.prototype, "byteLength");
  primordials.ArrayBufferPrototypeGetDetached = getGetter(ArrayBuffer.prototype, "detached");
  primordials.ArrayBufferPrototypeSlice = uncurryThis(ArrayBuffer.prototype.slice);
  primordials.ArrayBufferPrototypeTransferToFixedLength = maybeUncurry(
    ArrayBuffer.prototype.transferToFixedLength,
  );

  primordials.ArrayPrototypeConcat = uncurryThis(Array.prototype.concat);
  primordials.ArrayPrototypeEvery = uncurryThis(Array.prototype.every);
  primordials.ArrayPrototypeFilter = uncurryThis(Array.prototype.filter);
  primordials.ArrayPrototypeFind = uncurryThis(Array.prototype.find);
  primordials.ArrayPrototypeForEach = uncurryThis(Array.prototype.forEach);
  primordials.ArrayPrototypeIncludes = uncurryThis(Array.prototype.includes);
  primordials.ArrayPrototypeIndexOf = uncurryThis(Array.prototype.indexOf);
  primordials.ArrayPrototypeJoin = uncurryThis(Array.prototype.join);
  primordials.ArrayPrototypeMap = uncurryThis(Array.prototype.map);
  primordials.ArrayPrototypePop = uncurryThis(Array.prototype.pop);
  primordials.ArrayPrototypePush = uncurryThis(Array.prototype.push);
  primordials.ArrayPrototypeSlice = uncurryThis(Array.prototype.slice);
  primordials.ArrayPrototypeSome = uncurryThis(Array.prototype.some);
  primordials.ArrayPrototypeSort = uncurryThis(Array.prototype.sort);
  primordials.ArrayPrototypeSplice = uncurryThis(Array.prototype.splice);
  primordials.ArrayPrototypeUnshift = uncurryThis(Array.prototype.unshift);

  primordials.AsyncGeneratorPrototypeNext = uncurryThis(AsyncGeneratorPrototype.next);

  primordials.BigIntPrototypeToString = uncurryThis(BigInt.prototype.toString);
  primordials.BigIntPrototypeValueOf = uncurryThis(BigInt.prototype.valueOf);
  primordials.BooleanPrototypeValueOf = uncurryThis(Boolean.prototype.valueOf);

  primordials.DataViewPrototypeGetBuffer = getGetter(DataView.prototype, "buffer");
  primordials.DataViewPrototypeGetByteLength = getGetter(DataView.prototype, "byteLength");
  primordials.DataViewPrototypeGetByteOffset = getGetter(DataView.prototype, "byteOffset");

  primordials.DatePrototypeGetTime = uncurryThis(Date.prototype.getTime);
  primordials.DatePrototypeToISOString = uncurryThis(Date.prototype.toISOString);

  primordials.FunctionPrototypeApply = uncurryThis(Function.prototype.apply);
  primordials.FunctionPrototypeBind = uncurryThis(Function.prototype.bind);
  primordials.FunctionPrototypeCall = uncurryThis(Function.prototype.call);
  primordials.FunctionPrototypeToString = uncurryThis(Function.prototype.toString);

  primordials.MapIteratorPrototypeNext = uncurryThis(MapIteratorPrototype.next);
  primordials.MapPrototypeDelete = uncurryThis(Map.prototype.delete);
  primordials.MapPrototypeForEach = uncurryThis(Map.prototype.forEach);
  primordials.MapPrototypeGet = uncurryThis(Map.prototype.get);
  primordials.MapPrototypeGetSize = getGetter(Map.prototype, "size");
  primordials.MapPrototypeHas = uncurryThis(Map.prototype.has);
  primordials.MapPrototypeSet = uncurryThis(Map.prototype.set);
  primordials.MapPrototypeSymbolIterator = uncurryThis(Map.prototype[Symbol.iterator]);

  primordials.NumberPrototypeToFixed = uncurryThis(Number.prototype.toFixed);
  primordials.NumberPrototypeToString = uncurryThis(Number.prototype.toString);
  primordials.NumberPrototypeValueOf = uncurryThis(Number.prototype.valueOf);

  primordials.ObjectPrototypeHasOwnProperty = uncurryThis(Object.prototype.hasOwnProperty);
  primordials.ObjectPrototypeIsPrototypeOf = uncurryThis(Object.prototype.isPrototypeOf);
  primordials.ObjectPrototypePropertyIsEnumerable = uncurryThis(
    Object.prototype.propertyIsEnumerable,
  );

  primordials.PromisePrototypeThen = uncurryThis(_Promise.prototype.then);

  primordials.RegExpPrototypeExec = uncurryThis(RegExp.prototype.exec);
  primordials.RegExpPrototypeTest = uncurryThis(RegExp.prototype.test);
  primordials.RegExpPrototypeToString = uncurryThis(RegExp.prototype.toString);

  primordials.SetIteratorPrototypeNext = uncurryThis(SetIteratorPrototype.next);
  primordials.SetPrototypeAdd = uncurryThis(Set.prototype.add);
  primordials.SetPrototypeDelete = uncurryThis(Set.prototype.delete);
  primordials.SetPrototypeForEach = uncurryThis(Set.prototype.forEach);
  primordials.SetPrototypeGetSize = getGetter(Set.prototype, "size");
  primordials.SetPrototypeSymbolIterator = uncurryThis(Set.prototype[Symbol.iterator]);

  primordials.StringIteratorPrototypeNext = uncurryThis(StringIteratorPrototype.next);
  primordials.StringPrototypeCharAt = uncurryThis(String.prototype.charAt);
  primordials.StringPrototypeCharCodeAt = uncurryThis(String.prototype.charCodeAt);
  primordials.StringPrototypeCodePointAt = uncurryThis(String.prototype.codePointAt);
  primordials.StringPrototypeIncludes = uncurryThis(String.prototype.includes);
  primordials.StringPrototypeIndexOf = uncurryThis(String.prototype.indexOf);
  primordials.StringPrototypeLocaleCompare = uncurryThis(String.prototype.localeCompare);
  primordials.StringPrototypeMatch = uncurryThis(String.prototype.match);
  primordials.StringPrototypePadEnd = uncurryThis(String.prototype.padEnd);
  primordials.StringPrototypePadStart = uncurryThis(String.prototype.padStart);
  primordials.StringPrototypeRepeat = uncurryThis(String.prototype.repeat);
  primordials.StringPrototypeReplace = uncurryThis(String.prototype.replace);
  primordials.StringPrototypeReplaceAll = uncurryThis(String.prototype.replaceAll);
  primordials.StringPrototypeSlice = uncurryThis(String.prototype.slice);
  primordials.StringPrototypeSplit = uncurryThis(String.prototype.split);
  primordials.StringPrototypeStartsWith = uncurryThis(String.prototype.startsWith);
  primordials.StringPrototypeSubstring = uncurryThis(String.prototype.substring);
  primordials.StringPrototypeSymbolIterator = uncurryThis(String.prototype[Symbol.iterator]);
  primordials.StringPrototypeToLowerCase = uncurryThis(String.prototype.toLowerCase);
  primordials.StringPrototypeToUpperCase = uncurryThis(String.prototype.toUpperCase);
  primordials.StringPrototypeToWellFormed = maybeUncurry(String.prototype.toWellFormed);
  primordials.StringPrototypeTrim = uncurryThis(String.prototype.trim);
  primordials.StringPrototypeValueOf = uncurryThis(String.prototype.valueOf);

  primordials.SymbolPrototypeGetDescription = getGetter(Symbol.prototype, "description");
  primordials.SymbolPrototypeToString = uncurryThis(Symbol.prototype.toString);
  primordials.SymbolPrototypeValueOf = uncurryThis(Symbol.prototype.valueOf);

  primordials.TypedArrayPrototypeGetBuffer = getGetter(TypedArrayPrototype, "buffer");
  primordials.TypedArrayPrototypeGetByteLength = getGetter(TypedArrayPrototype, "byteLength");
  primordials.TypedArrayPrototypeGetByteOffset = getGetter(TypedArrayPrototype, "byteOffset");
  primordials.TypedArrayPrototypeGetLength = getGetter(TypedArrayPrototype, "length");
  primordials.TypedArrayPrototypeGetSymbolToStringTag = getGetter(
    TypedArrayPrototype,
    Symbol.toStringTag,
  );
  primordials.TypedArrayPrototypeJoin = uncurryThis(TypedArrayPrototype.join);
  primordials.TypedArrayPrototypeSet = uncurryThis(TypedArrayPrototype.set);
  primordials.TypedArrayPrototypeSlice = uncurryThis(TypedArrayPrototype.slice);
  primordials.TypedArrayPrototypeSubarray = uncurryThis(TypedArrayPrototype.subarray);

  primordials.WeakMapPrototypeGet = uncurryThis(WeakMap.prototype.get);
  primordials.WeakMapPrototypeHas = uncurryThis(WeakMap.prototype.has);
  primordials.WeakMapPrototypeSet = uncurryThis(WeakMap.prototype.set);
  primordials.WeakRefPrototypeDeref = uncurryThis(WeakRef.prototype.deref);

  const {
    ArrayPrototypeForEach,
    ArrayPrototypeJoin,
    ArrayPrototypeMap,
    FunctionPrototypeCall,
    ObjectDefineProperty,
    ObjectFreeze,
    ObjectPrototypeIsPrototypeOf,
    ObjectSetPrototypeOf,
    Promise,
    PromisePrototype,
    PromisePrototypeThen,
    SymbolIterator,
    TypedArrayPrototypeJoin,
  } = primordials;

  // Because these functions are used by `makeSafe`, which is exposed
  // on the `primordials` object, it's important to use const references
  // to the primordials that they use:
  const createSafeIterator = (factory, next) => {
    class SafeIterator {
      constructor(iterable) {
        this._iterator = factory(iterable);
      }
      next() {
        return next(this._iterator);
      }
      [SymbolIterator]() {
        return this;
      }
    }
    ObjectSetPrototypeOf(SafeIterator.prototype, null);
    ObjectFreeze(SafeIterator.prototype);
    ObjectFreeze(SafeIterator);
    return SafeIterator;
  };

  class SafeArrayIterator {
    #array;
    #done = false;
    #index = 0;
    constructor(array) {
      this.#array = array;
    }
    next() {
      if (this.#done) {
        return { value: undefined, done: true };
      }
      const array = this.#array;
      const index = this.#index;
      if (index >= array.length) {
        this.#done = true;
        return { value: undefined, done: true };
      }
      this.#index = index + 1;
      return { value: array[index], done: false };
    }
    [SymbolIterator]() {
      return this;
    }
  }
  ObjectSetPrototypeOf(SafeArrayIterator.prototype, null);
  ObjectFreeze(SafeArrayIterator.prototype);
  ObjectFreeze(SafeArrayIterator);
  primordials.SafeArrayIterator = SafeArrayIterator;

  primordials.SafeSetIterator = createSafeIterator(
    primordials.SetPrototypeSymbolIterator,
    primordials.SetIteratorPrototypeNext,
  );
  primordials.SafeMapIterator = createSafeIterator(
    primordials.MapPrototypeSymbolIterator,
    primordials.MapIteratorPrototypeNext,
  );
  primordials.SafeStringIterator = createSafeIterator(
    primordials.StringPrototypeSymbolIterator,
    primordials.StringIteratorPrototypeNext,
  );

  const copyProps = (src, dest) => {
    ArrayPrototypeForEach(ReflectOwnKeys(src), (key) => {
      if (!ReflectGetOwnPropertyDescriptor(dest, key)) {
        ReflectDefineProperty(
          dest,
          key,
          {
            __proto__: null,
            ...ReflectGetOwnPropertyDescriptor(src, key),
          },
        );
      }
    });
  };

  /**
   * @type {typeof primordials.makeSafe}
   */
  const makeSafe = (unsafe, safe) => {
    if (SymbolIterator in unsafe.prototype) {
      const dummy = new unsafe();
      let next; // We can reuse the same `next` method.

      ArrayPrototypeForEach(ReflectOwnKeys(unsafe.prototype), (key) => {
        if (!ReflectGetOwnPropertyDescriptor(safe.prototype, key)) {
          const desc = ReflectGetOwnPropertyDescriptor(unsafe.prototype, key);
          if (
            typeof desc.value === "function" &&
            desc.value.length === 0 &&
            SymbolIterator in (FunctionPrototypeCall(desc.value, dummy) ?? {})
          ) {
            const createIterator = uncurryThis(desc.value);
            next ??= uncurryThis(createIterator(dummy).next);
            const SafeIterator = createSafeIterator(createIterator, next);
            desc.value = function () {
              return new SafeIterator(this);
            };
          }
          ReflectDefineProperty(safe.prototype, key, {
            __proto__: null,
            ...desc,
          });
        }
      });
    } else {
      copyProps(unsafe.prototype, safe.prototype);
    }
    copyProps(unsafe, safe);

    ObjectSetPrototypeOf(safe.prototype, null);
    ObjectFreeze(safe.prototype);
    ObjectFreeze(safe);
    return safe;
  };
  primordials.makeSafe = makeSafe;

  // Subclass the constructors because we need to use their prototype
  // methods later.
  // Defining the `constructor` is necessary here to avoid the default
  // constructor which uses the user-mutable `%ArrayIteratorPrototype%.next`.
  primordials.SafeMap = makeSafe(
    Map,
    class SafeMap extends Map {
      constructor(i) {
        if (i == null) {
          super();
          return;
        }
        super(new SafeArrayIterator(i));
      }
    },
  );
  primordials.SafeWeakMap = makeSafe(
    WeakMap,
    class SafeWeakMap extends WeakMap {
      constructor(i) {
        if (i == null) {
          super();
          return;
        }
        super(new SafeArrayIterator(i));
      }
    },
  );

  primordials.SafeSet = makeSafe(
    Set,
    class SafeSet extends Set {
      constructor(i) {
        if (i == null) {
          super();
          return;
        }
        super(new SafeArrayIterator(i));
      }
    },
  );
  primordials.SafeWeakSet = makeSafe(
    WeakSet,
    class SafeWeakSet extends WeakSet {
      constructor(i) {
        if (i == null) {
          super();
          return;
        }
        super(new SafeArrayIterator(i));
      }
    },
  );

  primordials.SafeRegExp = makeSafe(
    RegExp,
    class SafeRegExp extends RegExp {
      constructor(pattern, flags) {
        super(pattern, flags);
      }
    },
  );

  primordials.SafeFinalizationRegistry = makeSafe(
    FinalizationRegistry,
    class SafeFinalizationRegistry extends FinalizationRegistry {
      constructor(cleanupCallback) {
        super(cleanupCallback);
      }
    },
  );

  primordials.SafeWeakRef = makeSafe(
    WeakRef,
    class SafeWeakRef extends WeakRef {
      constructor(target) {
        super(target);
      }
    },
  );

  const SafePromise = makeSafe(
    Promise,
    class SafePromise extends Promise {
      constructor(executor) {
        super(executor);
      }
    },
  );

  primordials.ArrayPrototypeToString = (thisArray) =>
    ArrayPrototypeJoin(thisArray);

  primordials.TypedArrayPrototypeToString = (thisArray) =>
    TypedArrayPrototypeJoin(thisArray);

  primordials.PromisePrototypeCatch = (thisPromise, onRejected) =>
    PromisePrototypeThen(thisPromise, undefined, onRejected);

  const arrayToSafePromiseIterable = (array) =>
    new SafeArrayIterator(
      ArrayPrototypeMap(
        array,
        (p) => {
          if (ObjectPrototypeIsPrototypeOf(PromisePrototype, p)) {
            return new SafePromise((c, d) => PromisePrototypeThen(p, c, d));
          }
          return p;
        },
      ),
    );

  /**
   * Creates a Promise that is resolved with an array of results when all of the
   * provided Promises resolve, or rejected when any Promise is rejected.
   * @template T
   * @param {Array<T | PromiseLike<T>>} values
   * @returns {Promise<Awaited<T>[]>}
   */
  primordials.SafePromiseAll = (values) =>
    // Wrapping on a new Promise is necessary to not expose the SafePromise
    // prototype to user-land.
    new Promise((a, b) =>
      SafePromise.all(arrayToSafePromiseIterable(values)).then(a, b)
    );

  // /**
  //  * Creates a Promise that is resolved with an array of results when all
  //  * of the provided Promises resolve or reject.
  //  * @template T
  //  * @param {Array<T | PromiseLike<T>>} values
  //  * @returns {Promise<PromiseSettledResult<T>[]>}
  //  */
  primordials.SafePromiseAllSettled = (values) =>
    // Wrapping on a new Promise is necessary to not expose the SafePromise
    // prototype to user-land.
    new Promise((a, b) =>
      SafePromise.allSettled(arrayToSafePromiseIterable(values)).then(a, b)
    );

  // /**
  //  * The any function returns a promise that is fulfilled by the first given
  //  * promise to be fulfilled, or rejected with an AggregateError containing
  //  * an array of rejection reasons if all of the given promises are rejected.
  //  * It resolves all elements of the passed iterable to promises as it runs
  //  * this algorithm.
  //  * @template T
  //  * @param {T} values
  //  * @returns {Promise<Awaited<T[number]>>}
  //  */
  primordials.SafePromiseAny = (values) =>
    // Wrapping on a new Promise is necessary to not expose the SafePromise
    // prototype to user-land.
    new Promise((a, b) =>
      SafePromise.any(arrayToSafePromiseIterable(values)).then(a, b)
    );

  // /**
  //  * Creates a Promise that is resolved or rejected when any of the provided
  //  * Promises are resolved or rejected.
  //  * @template T
  //  * @param {T} values
  //  * @returns {Promise<Awaited<T[number]>>}
  //  */
  primordials.SafePromiseRace = (values) =>
    // Wrapping on a new Promise is necessary to not expose the SafePromise
    // prototype to user-land.
    new Promise((a, b) =>
      SafePromise.race(arrayToSafePromiseIterable(values)).then(a, b)
    );

  /**
   * Attaches a callback that is invoked when the Promise is settled (fulfilled or
   * rejected). The resolved value cannot be modified from the callback.
   * Prefer using async functions when possible.
   * @param {Promise<any>} thisPromise
   * @param {() => void) | undefined | null} onFinally The callback to execute
   *        when the Promise is settled (fulfilled or rejected).
   * @returns A Promise for the completion of the callback.
   */
  primordials.SafePromisePrototypeFinally = (thisPromise, onFinally) =>
    // Wrapping on a new Promise is necessary to not expose the SafePromise
    // prototype to user-land.
    new Promise((a, b) =>
      new SafePromise((a, b) => PromisePrototypeThen(thisPromise, a, b))
        .finally(onFinally)
        .then(a, b)
    );

  // Create getter and setter for `queueMicrotask`, it hasn't been bound yet.
  let queueMicrotask = undefined;
  ObjectDefineProperty(primordials, "queueMicrotask", {
    __proto__: null,
    get() {
      return queueMicrotask;
    },
  });
  primordials.setQueueMicrotask = (value) => {
    if (queueMicrotask !== undefined) {
      throw new Error("queueMicrotask is already defined");
    }
    queueMicrotask = value;
  };

  // Renaming from `eval` is necessary because otherwise it would perform direct
  // evaluation, allowing user-land access to local variables.
  // This is because the identifier `eval` is somewhat treated as a keyword
  primordials.indirectEval = eval;

  ObjectSetPrototypeOf(primordials, null);
  ObjectFreeze(primordials);

  // Provide bootstrap namespace
  globalThis.__bootstrap ??= {};
  globalThis.__bootstrap.primordials = primordials;
})();
