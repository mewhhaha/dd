// Copyright 2018-2026 the Deno authors. MIT license.

// dd's stand-in for Deno's console module. Workers print through V8's
// built-in console, so the web layer only needs the helper that decorates
// objects for inspection.
(function () {
const { primordials } = __bootstrap;
const {
  ObjectDefineProperty,
  ReflectGetOwnPropertyDescriptor,
  ReflectGetPrototypeOf,
} = primordials;

function createFilteredInspectProxy({ object, keys, evaluate }) {
  const cls = class {};
  if (object.constructor?.name) {
    ObjectDefineProperty(cls, "name", {
      __proto__: null,
      value: object.constructor.name,
    });
  }

  const result = new cls();
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    const descriptor = evaluate
      ? getEvaluatedDescriptor(object, key)
      : (getDescendantPropertyDescriptor(object, key) ??
        getEvaluatedDescriptor(object, key));
    ObjectDefineProperty(result, key, { __proto__: null, ...descriptor });
  }
  return result;

  function getDescendantPropertyDescriptor(object, key) {
    let propertyDescriptor = ReflectGetOwnPropertyDescriptor(object, key);
    if (!propertyDescriptor) {
      const prototype = ReflectGetPrototypeOf(object);
      if (prototype) {
        propertyDescriptor = getDescendantPropertyDescriptor(prototype, key);
      }
    }
    return propertyDescriptor;
  }

  function getEvaluatedDescriptor(object, key) {
    return {
      __proto__: null,
      configurable: true,
      enumerable: true,
      value: object[key],
    };
  }
}

return { createFilteredInspectProxy };
})();
