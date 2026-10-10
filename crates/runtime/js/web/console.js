// Copyright 2018-2026 the Deno authors. MIT license.

// dd's console and value inspector. Formats values the way Node and Deno do
// and hands each console call to the runtime as one message. The web layer
// loads this as Deno's console module: its classes inspect themselves through
// createFilteredInspectProxy and the `inspect` they are handed.
//
// Everything here runs on primordials and op-backed type checks, so worker
// code that replaces globals or prototype methods cannot change what is
// logged or reach the sink.
return (function () {
const { core, primordials } = __bootstrap;
const {
  ArrayIsArray,
  ArrayPrototypeConcat,
  ArrayPrototypeFilter,
  ArrayPrototypeIncludes,
  ArrayPrototypeIndexOf,
  ArrayPrototypeJoin,
  ArrayPrototypeMap,
  ArrayPrototypePop,
  ArrayPrototypePush,
  ArrayPrototypeSlice,
  ArrayBufferPrototypeGetByteLength,
  BigIntPrototypeToString,
  BigIntPrototypeValueOf,
  BooleanPrototypeValueOf,
  DatePrototypeGetTime,
  DatePrototypeToISOString,
  Error,
  ErrorCaptureStackTrace,
  FunctionPrototypeToString,
  JSONStringify,
  MapPrototypeDelete,
  MapPrototypeForEach,
  MapPrototypeGet,
  MapPrototypeGetSize,
  MapPrototypeHas,
  MapPrototypeSet,
  MathFloor,
  MathMax,
  NumberIsNaN,
  NumberParseFloat,
  NumberParseInt,
  Number,
  NumberPrototypeToFixed,
  NumberPrototypeToString,
  NumberPrototypeValueOf,
  ObjectDefineProperty,
  ObjectGetOwnPropertyDescriptor,
  ObjectGetPrototypeOf,
  ObjectIs,
  ObjectPrototypeHasOwnProperty,
  ObjectPrototypePropertyIsEnumerable,
  ReflectApply,
  ReflectGetOwnPropertyDescriptor,
  ReflectGetPrototypeOf,
  ReflectOwnKeys,
  RegExpPrototypeExec,
  RegExpPrototypeToString,
  SafeMap,
  SetPrototypeForEach,
  SetPrototypeGetSize,
  String,
  StringPrototypeCharCodeAt,
  StringPrototypeIncludes,
  StringPrototypeIndexOf,
  StringPrototypePadEnd,
  StringPrototypeRepeat,
  StringPrototypeReplaceAll,
  StringPrototypeSlice,
  StringPrototypeSplit,
  StringPrototypeStartsWith,
  StringPrototypeToString,
  StringPrototypeValueOf,
  SymbolFor,
  SymbolPrototypeToString,
  SymbolPrototypeValueOf,
  SymbolToStringTag,
  TypedArrayPrototypeGetLength,
  TypedArrayPrototypeGetSymbolToStringTag,
  Uint8Array,
} = primordials;
const { op_own_non_index_keys, op_promise_state, op_proxy_details } = core.ops;

// console.table prints at most this many rows of an array.
const TABLE_MAX_ROWS = 1000;

const privateCustomInspect = SymbolFor("Deno.privateCustomInspect");
const denoCustomInspect = SymbolFor("Deno.customInspect");
const nodeCustomInspect = SymbolFor("nodejs.util.inspect.custom");

const DEFAULT_OPTIONS = {
  __proto__: null,
  depth: 4,
  breakLength: 80,
  maxArrayLength: 100,
  maxStringLength: 10_000,
};

const IDENTIFIER = /^[a-zA-Z_$][a-zA-Z_$0-9]*$/;

/** Formats `value` for display, as Node's `util.inspect` does. */
function inspect(value, options = undefined) {
  const ctx = {
    __proto__: null,
    depth: options?.depth ?? DEFAULT_OPTIONS.depth,
    breakLength: options?.breakLength ?? DEFAULT_OPTIONS.breakLength,
    maxArrayLength: options?.maxArrayLength ?? DEFAULT_OPTIONS.maxArrayLength,
    maxStringLength: options?.maxStringLength ?? DEFAULT_OPTIONS.maxStringLength,
    seen: [],
    circular: new SafeMap(),
  };
  return formatValue(ctx, value, 0);
}

function inspectOptions(ctx, recurseTimes) {
  return {
    depth: ctx.depth === null ? null : MathMax(0, ctx.depth - recurseTimes),
    breakLength: ctx.breakLength,
    maxArrayLength: ctx.maxArrayLength,
    maxStringLength: ctx.maxStringLength,
  };
}

function quoteString(ctx, value) {
  let text = value;
  let trailer = "";
  if (ctx.maxStringLength !== null && text.length > ctx.maxStringLength) {
    const remaining = text.length - ctx.maxStringLength;
    text = StringPrototypeSlice(text, 0, ctx.maxStringLength);
    trailer = `... ${remaining} more character${remaining > 1 ? "s" : ""}`;
  }
  let quote = "'";
  if (StringPrototypeIncludes(text, "'")) {
    if (!StringPrototypeIncludes(text, '"')) {
      quote = '"';
    } else if (!StringPrototypeIncludes(text, "`") && !StringPrototypeIncludes(text, "${")) {
      quote = "`";
    }
  }
  let escaped = "";
  for (let i = 0; i < text.length; i++) {
    const code = StringPrototypeCharCodeAt(text, i);
    const char = text[i];
    if (char === quote || char === "\\") {
      escaped += `\\${char}`;
    } else if (code < 0x20 || code === 0x7f) {
      escaped += ESCAPES[code] ?? `\\x${code < 0x10 ? "0" : ""}${NumberPrototypeToString(code, 16)}`;
    } else {
      escaped += char;
    }
  }
  return `${quote}${escaped}${quote}${trailer}`;
}

const ESCAPES = {
  __proto__: null,
  8: "\\b",
  9: "\\t",
  10: "\\n",
  11: "\\v",
  12: "\\f",
  13: "\\r",
};

function formatNumber(value) {
  return ObjectIs(value, -0) ? "-0" : `${value}`;
}

function formatPrimitive(ctx, value) {
  switch (typeof value) {
    case "string":
      return quoteString(ctx, value);
    case "number":
      return formatNumber(value);
    case "bigint":
      return `${BigIntPrototypeToString(value)}n`;
    case "boolean":
      return value ? "true" : "false";
    case "undefined":
      return "undefined";
    case "symbol":
      return SymbolPrototypeToString(value);
  }
  return String(value);
}

function formatKey(key) {
  if (typeof key === "symbol") {
    return `[${SymbolPrototypeToString(key)}]`;
  }
  return RegExpPrototypeExec(IDENTIFIER, key) !== null ? key : quoteString(DEFAULT_OPTIONS, key);
}

function formatValue(ctx, value, recurseTimes) {
  if (value === null) {
    return "null";
  }
  if (typeof value !== "object" && typeof value !== "function") {
    return formatPrimitive(ctx, value);
  }
  if (core.isProxy(value)) {
    const details = op_proxy_details(value);
    if (details === null || details[0] === null) {
      return "<Revoked Proxy>";
    }
    return formatValue(ctx, details[0], recurseTimes);
  }
  const custom = customInspection(ctx, value, recurseTimes);
  if (custom !== undefined) {
    return custom;
  }
  const seenAt = ArrayPrototypeIndexOf(ctx.seen, value);
  if (seenAt !== -1) {
    let index = MapPrototypeGet(ctx.circular, value);
    if (index === undefined) {
      index = MapPrototypeGetSize(ctx.circular) + 1;
      MapPrototypeSet(ctx.circular, value, index);
    }
    return `[Circular *${index}]`;
  }
  return formatRaw(ctx, value, recurseTimes);
}

function customInspection(ctx, value, recurseTimes) {
  let inspector;
  try {
    inspector = value[privateCustomInspect];
    if (typeof inspector === "function") {
      return ReflectApply(inspector, value, [inspect, inspectOptions(ctx, recurseTimes)]);
    }
    inspector = value[denoCustomInspect];
    if (typeof inspector === "function") {
      return ReflectApply(inspector, value, [inspect, inspectOptions(ctx, recurseTimes)]);
    }
    inspector = value[nodeCustomInspect];
    if (typeof inspector === "function") {
      const depth = ctx.depth === null ? null : ctx.depth - recurseTimes;
      const result = ReflectApply(inspector, value, [depth, inspectOptions(ctx, recurseTimes), inspect]);
      if (result === value) {
        return undefined;
      }
      return typeof result === "string" ? result : formatValue(ctx, result, recurseTimes);
    }
  } catch (error) {
    return `[Inspection threw: ${safeMessage(error)}]`;
  }
  return undefined;
}

function safeMessage(error) {
  try {
    return core.isNativeError(error) ? String(error.message) : String(error);
  } catch {
    return "unprintable error";
  }
}

/** The name of the constructor whose prototype `value` inherits from. */
function constructorName(value) {
  let object = value;
  while (object !== null) {
    const descriptor = ObjectGetOwnPropertyDescriptor(object, "constructor");
    if (
      descriptor !== undefined &&
      typeof descriptor.value === "function" &&
      typeof descriptor.value.name === "string" &&
      descriptor.value.name !== ""
    ) {
      return descriptor.value.name;
    }
    object = ObjectGetPrototypeOf(object);
  }
  return null;
}

function prefix(name, tag, fallback, size = "") {
  if (name === null) {
    return `[${fallback}${size}: null prototype]${tag !== "" && tag !== fallback ? ` [${tag}]` : ""} `;
  }
  if (tag !== "" && tag !== name) {
    return `${name}${size} [${tag}] `;
  }
  return `${name}${size} `;
}

function ownKeys(value, skipIndices) {
  if (skipIndices) {
    // Listing indices would create a string per element.
    return op_own_non_index_keys(value);
  }
  const keys = [];
  const all = ReflectOwnKeys(value);
  for (let i = 0; i < all.length; i++) {
    const key = all[i];
    if (ObjectPrototypePropertyIsEnumerable(value, key)) {
      ArrayPrototypePush(keys, key);
    }
  }
  return keys;
}

function formatProperty(ctx, value, key, recurseTimes) {
  const descriptor = ReflectGetOwnPropertyDescriptor(value, key);
  let shown;
  if (descriptor === undefined) {
    shown = "undefined";
  } else if (descriptor.get !== undefined || descriptor.set !== undefined) {
    shown = descriptor.get !== undefined
      ? descriptor.set !== undefined ? "[Getter/Setter]" : "[Getter]"
      : "[Setter]";
  } else {
    shown = formatValue(ctx, descriptor.value, recurseTimes + 1);
  }
  return `${formatKey(key)}: ${shown}`;
}

function toStringTag(value) {
  try {
    const tag = value[SymbolToStringTag];
    return typeof tag === "string" ? tag : "";
  } catch {
    return "";
  }
}

function formatRaw(ctx, value, recurseTimes) {
  const name = constructorName(value);
  const tag = toStringTag(value);
  let keys;
  let base = "";
  let braces;
  let entries = [];
  let formatEntries = () => entries;
  let skipIndices = false;

  if (ArrayIsArray(value)) {
    skipIndices = true;
    const head = name !== "Array" || tag !== "" ? prefix(name, tag, "Array", `(${value.length})`) : "";
    braces = [`${head}[`, "]"];
    formatEntries = () => formatArrayEntries(ctx, value, recurseTimes);
  } else if (core.isTypedArray(value)) {
    skipIndices = true;
    const length = TypedArrayPrototypeGetLength(value);
    const typedName = TypedArrayPrototypeGetSymbolToStringTag(value);
    braces = [`${prefix(name, tag === typedName ? "" : tag, typedName, `(${length})`)}[`, "]"];
    formatEntries = () => formatTypedArrayEntries(ctx, value, length);
  } else if (core.isMap(value)) {
    const size = MapPrototypeGetSize(value);
    braces = [`${prefix(name, tag === "Map" ? "" : tag, "Map", `(${size})`)}{`, "}"];
    formatEntries = () => {
      const output = [];
      MapPrototypeForEach(value, (entry, key) => {
        ArrayPrototypePush(
          output,
          `${formatValue(ctx, key, recurseTimes + 1)} => ${formatValue(ctx, entry, recurseTimes + 1)}`,
        );
      });
      return output;
    };
  } else if (core.isSet(value)) {
    const size = SetPrototypeGetSize(value);
    braces = [`${prefix(name, tag === "Set" ? "" : tag, "Set", `(${size})`)}{`, "}"];
    formatEntries = () => {
      const output = [];
      SetPrototypeForEach(value, (entry) => {
        ArrayPrototypePush(output, formatValue(ctx, entry, recurseTimes + 1));
      });
      return output;
    };
  } else if (typeof value === "function") {
    base = formatFunction(value, name);
    braces = ["{", "}"];
    if (ownKeys(value, false).length === 0) {
      return base;
    }
  } else if (core.isRegExp(value)) {
    base = RegExpPrototypeToString(value);
    braces = ["{", "}"];
    if (ownKeys(value, false).length === 0) {
      return base;
    }
  } else if (core.isDate(value)) {
    const time = DatePrototypeGetTime(value);
    base = NumberIsNaN(time) ? "Invalid Date" : DatePrototypeToISOString(value);
    braces = ["{", "}"];
    if (ownKeys(value, false).length === 0) {
      return base;
    }
  } else if (core.isNativeError(value)) {
    return formatError(ctx, value, recurseTimes);
  } else if (core.isAnyArrayBuffer(value)) {
    const kind = core.isArrayBuffer(value) ? "ArrayBuffer" : "SharedArrayBuffer";
    braces = [`${prefix(name, tag === kind ? "" : tag, kind)}{`, "}"];
    formatEntries = () => formatArrayBuffer(ctx, value);
  } else if (core.isPromise(value)) {
    braces = [`${prefix(name, tag === "Promise" ? "" : tag, "Promise")}{`, "}"];
    formatEntries = () => {
      const state = op_promise_state(value);
      if (state[0] === 0) {
        return ["<pending>"];
      }
      const shown = formatValue(ctx, state[1], recurseTimes + 1);
      return [state[0] === 2 ? `<rejected> ${shown}` : shown];
    };
  } else if (core.isBoxedPrimitive(value)) {
    base = formatBoxed(ctx, value);
    braces = ["{", "}"];
    if (core.isStringObject(value)) {
      skipIndices = true;
    }
    if (ownKeys(value, skipIndices).length === 0) {
      return base;
    }
  } else if (tag === "WeakMap" || tag === "WeakSet" || tag === "WeakRef") {
    return `${tag} { <items unknown> }`;
  } else {
    const head = name === "Object" ? (tag !== "" ? `Object [${tag}] ` : "") : prefix(name, tag, "Object");
    braces = [`${head}{`, "}"];
  }

  if (ctx.depth !== null && recurseTimes > ctx.depth) {
    return `[${name ?? (tag || "Object")}]`;
  }

  ArrayPrototypePush(ctx.seen, value);
  let output;
  try {
    output = formatEntries();
    keys ??= ownKeys(value, skipIndices);
    for (let i = 0; i < keys.length; i++) {
      ArrayPrototypePush(output, formatProperty(ctx, value, keys[i], recurseTimes));
    }
  } catch (error) {
    output = [`[Inspection threw: ${safeMessage(error)}]`];
  }
  ArrayPrototypePop(ctx.seen);

  let result = reduceToSingleString(ctx, output, base, braces);
  const index = MapPrototypeGet(ctx.circular, value);
  if (index !== undefined) {
    result = `<ref *${index}> ${result}`;
  }
  return result;
}

function formatArrayEntries(ctx, value, recurseTimes) {
  const output = [];
  const length = value.length;
  const limit = ctx.maxArrayLength === null ? length : Math_min(length, ctx.maxArrayLength);
  let holes = 0;
  const flushHoles = () => {
    if (holes > 0) {
      ArrayPrototypePush(output, `<${holes} empty item${holes > 1 ? "s" : ""}>`);
      holes = 0;
    }
  };
  for (let i = 0; i < limit; i++) {
    if (!ObjectPrototypeHasOwnProperty(value, i)) {
      holes++;
      continue;
    }
    flushHoles();
    ArrayPrototypePush(output, formatArrayElement(ctx, value, i, recurseTimes));
  }
  flushHoles();
  if (length > limit) {
    const remaining = length - limit;
    ArrayPrototypePush(output, `... ${remaining} more item${remaining > 1 ? "s" : ""}`);
  }
  return output;
}

function formatArrayElement(ctx, value, index, recurseTimes) {
  const descriptor = ReflectGetOwnPropertyDescriptor(value, index);
  if (descriptor.get !== undefined || descriptor.set !== undefined) {
    return descriptor.get !== undefined
      ? descriptor.set !== undefined ? "[Getter/Setter]" : "[Getter]"
      : "[Setter]";
  }
  return formatValue(ctx, descriptor.value, recurseTimes + 1);
}

function Math_min(a, b) {
  return a < b ? a : b;
}

function formatTypedArrayEntries(ctx, value, length) {
  const output = [];
  const limit = ctx.maxArrayLength === null ? length : Math_min(length, ctx.maxArrayLength);
  for (let i = 0; i < limit; i++) {
    const element = value[i];
    ArrayPrototypePush(
      output,
      typeof element === "bigint" ? `${BigIntPrototypeToString(element)}n` : formatNumber(element),
    );
  }
  if (length > limit) {
    const remaining = length - limit;
    ArrayPrototypePush(output, `... ${remaining} more item${remaining > 1 ? "s" : ""}`);
  }
  return output;
}

function formatArrayBuffer(ctx, value) {
  let byteLength;
  try {
    byteLength = core.isArrayBuffer(value)
      ? ArrayBufferPrototypeGetByteLength(value)
      : new Uint8Array(value).length;
  } catch {
    return ["(detached)"];
  }
  const bytes = new Uint8Array(value);
  const limit = Math_min(byteLength, 50);
  let contents = "";
  for (let i = 0; i < limit; i++) {
    const hex = NumberPrototypeToString(bytes[i], 16);
    contents += `${i === 0 ? "" : " "}${hex.length === 1 ? "0" : ""}${hex}`;
  }
  if (byteLength > limit) {
    contents += ` ... ${byteLength - limit} more byte${byteLength - limit > 1 ? "s" : ""}`;
  }
  return [`[Uint8Contents]: <${contents}>`, `byteLength: ${byteLength}`];
}

function formatFunction(value, name) {
  let source = "";
  try {
    source = FunctionPrototypeToString(value);
  } catch {
    // A revoked or exotic function still prints by name.
  }
  const fnName = typeof value.name === "string" && value.name !== "" ? value.name : null;
  if (StringPrototypeStartsWith(source, "class") && StringPrototypeIncludes(source, "{")) {
    const parent = ObjectGetPrototypeOf(value);
    const extendsName = parent !== null && typeof parent.name === "string" && parent.name !== ""
      ? ` extends ${parent.name}`
      : "";
    return `[class ${fnName ?? "(anonymous)"}${extendsName}]`;
  }
  let kind = "Function";
  if (name === "AsyncFunction" || name === "GeneratorFunction" || name === "AsyncGeneratorFunction") {
    kind = name;
  }
  return `[${kind}${fnName === null ? " (anonymous)" : `: ${fnName}`}]`;
}

function formatBoxed(ctx, value) {
  if (core.isStringObject(value)) {
    return `[String: ${quoteString(ctx, StringPrototypeValueOf(value))}]`;
  }
  try {
    return `[Number: ${formatNumber(NumberPrototypeValueOf(value))}]`;
  } catch {
    // Not a Number wrapper.
  }
  try {
    return `[Boolean: ${BooleanPrototypeValueOf(value)}]`;
  } catch {
    // Not a Boolean wrapper.
  }
  try {
    return `[BigInt: ${BigIntPrototypeToString(BigIntPrototypeValueOf(value))}n]`;
  } catch {
    // Not a BigInt wrapper.
  }
  return `[Symbol: ${SymbolPrototypeToString(SymbolPrototypeValueOf(value))}]`;
}

function formatError(ctx, error, recurseTimes) {
  let stack;
  try {
    stack = error.stack;
  } catch {
    stack = undefined;
  }
  let base;
  if (typeof stack === "string" && stack !== "") {
    base = stack;
  } else {
    let name = "Error";
    let message = "";
    try {
      name = String(error.name ?? "Error");
      message = String(error.message ?? "");
    } catch {
      // Keep the defaults.
    }
    base = `[${message === "" ? name : `${name}: ${message}`}]`;
  }
  if (ctx.depth !== null && recurseTimes > ctx.depth) {
    return base;
  }
  ArrayPrototypePush(ctx.seen, error);
  const output = [];
  try {
    const keys = ArrayPrototypeFilter(
      ownKeys(error, false),
      (key) => key !== "stack" && key !== "message",
    );
    for (let i = 0; i < keys.length; i++) {
      ArrayPrototypePush(output, formatProperty(ctx, error, keys[i], recurseTimes));
    }
    if (ObjectPrototypeHasOwnProperty(error, "cause") && !ArrayPrototypeIncludes(keys, "cause")) {
      const cause = ReflectGetOwnPropertyDescriptor(error, "cause");
      ArrayPrototypePush(output, `[cause]: ${formatValue(ctx, cause.value, recurseTimes + 1)}`);
    }
    if (ObjectPrototypeHasOwnProperty(error, "errors") && !ArrayPrototypeIncludes(keys, "errors")) {
      const errors = ReflectGetOwnPropertyDescriptor(error, "errors");
      ArrayPrototypePush(output, `[errors]: ${formatValue(ctx, errors.value, recurseTimes + 1)}`);
    }
  } catch (inspectionError) {
    ArrayPrototypePush(output, `[Inspection threw: ${safeMessage(inspectionError)}]`);
  }
  ArrayPrototypePop(ctx.seen);
  if (output.length === 0) {
    return base;
  }
  return reduceToSingleString(ctx, output, base, ["{", "}"]);
}

function reduceToSingleString(ctx, output, base, braces) {
  const head = base === "" ? braces[0] : `${base} ${braces[0]}`;
  if (output.length === 0) {
    return base === "" ? `${braces[0]}${braces[1]}` : base;
  }
  let length = head.length + braces[1].length;
  let multiline = false;
  for (let i = 0; i < output.length; i++) {
    length += output[i].length + 2;
    if (StringPrototypeIncludes(output[i], "\n")) {
      multiline = true;
    }
  }
  if (!multiline && !StringPrototypeIncludes(base, "\n") && length <= ctx.breakLength) {
    return `${head} ${ArrayPrototypeJoin(output, ", ")} ${braces[1]}`;
  }
  const lines = ArrayPrototypeMap(
    output,
    (entry) => `  ${StringPrototypeReplaceAll(entry, "\n", "\n  ")}`,
  );
  return `${head}\n${ArrayPrototypeJoin(lines, ",\n")}\n${braces[1]}`;
}

/** Formats console arguments: printf-style substitutions in a leading string,
 * then every remaining argument, strings as is and other values inspected. */
function formatArgs(args, options = undefined) {
  if (args.length === 0) {
    return "";
  }
  const first = args[0];
  let index = 0;
  let text = "";
  if (typeof first === "string") {
    index = 1;
    if (args.length > 1 && StringPrototypeIncludes(first, "%")) {
      let last = 0;
      for (let i = 0; i < first.length - 1; i++) {
        if (first[i] !== "%") {
          continue;
        }
        const specifier = first[i + 1];
        let replacement;
        if (specifier === "%") {
          replacement = "%";
        } else if (index < args.length) {
          const arg = args[index];
          switch (specifier) {
            case "s":
              replacement = formatStringSubstitution(arg, options);
              break;
            case "d":
            case "i":
              replacement = formatInteger(arg, specifier === "i");
              break;
            case "f":
              replacement = typeof arg === "symbol" ? "NaN" : formatNumber(NumberParseFloat(arg));
              break;
            case "j":
              replacement = formatJson(arg);
              break;
            case "o":
            case "O":
              replacement = inspect(arg, options);
              break;
            case "c":
              // CSS styling has no meaning in a log line.
              replacement = "";
              break;
          }
          if (replacement !== undefined && specifier !== "%") {
            index++;
          }
        }
        if (replacement !== undefined) {
          text += StringPrototypeSlice(first, last, i) + replacement;
          last = i + 2;
          i++;
        }
      }
      text += StringPrototypeSlice(first, last);
    } else {
      text = first;
    }
  }
  for (; index < args.length; index++) {
    const arg = args[index];
    const shown = typeof arg === "string" ? arg : inspect(arg, options);
    text = text === "" && index === 0 ? shown : `${text} ${shown}`;
  }
  return text;
}

function formatStringSubstitution(value, options) {
  switch (typeof value) {
    case "string":
      return value;
    case "number":
      return formatNumber(value);
    case "bigint":
      return `${BigIntPrototypeToString(value)}n`;
    case "symbol":
      return SymbolPrototypeToString(value);
    case "object":
    case "function":
      return value === null ? "null" : inspect(value, { ...options, depth: 1 });
  }
  return String(value);
}

function formatInteger(value, truncate) {
  if (typeof value === "bigint") {
    return `${BigIntPrototypeToString(value)}n`;
  }
  if (typeof value === "symbol" || (typeof value === "object" && value !== null)) {
    return "NaN";
  }
  return formatNumber(truncate ? NumberParseInt(value) : MathFloor(Number(value)));
}

function formatJson(value) {
  try {
    return JSONStringify(value);
  } catch (error) {
    if (StringPrototypeIncludes(safeMessage(error), "circular")) {
      return "[Circular]";
    }
    throw error;
  }
}

/** An object's own enumerable keys for console.table, as
 * `[keys, omitted]`: at most TABLE_MAX_ROWS of an array's indices (never all
 * of them), then its other keys; `omitted` counts the indices left out. */
function tableKeys(value) {
  if (!ArrayIsArray(value) && !core.isTypedArray(value)) {
    const keys = ownKeys(value, false);
    return keys.length > TABLE_MAX_ROWS
      ? [ArrayPrototypeSlice(keys, 0, TABLE_MAX_ROWS), keys.length - TABLE_MAX_ROWS]
      : [keys, 0];
  }
  const keys = [];
  const length = ArrayIsArray(value) ? value.length : TypedArrayPrototypeGetLength(value);
  const limit = Math_min(length, TABLE_MAX_ROWS);
  for (let i = 0; i < limit; i++) {
    if (ObjectPrototypeHasOwnProperty(value, i)) {
      ArrayPrototypePush(keys, `${i}`);
    }
  }
  const named = ownKeys(value, true);
  for (let i = 0; i < named.length; i++) {
    ArrayPrototypePush(keys, named[i]);
  }
  return [keys, length - limit];
}

function formatTable(data, properties) {
  let omittedRows = 0;
  const rows = [];
  const columns = [];
  const addColumn = (name) => {
    if (!ArrayPrototypeIncludes(columns, name)) {
      ArrayPrototypePush(columns, name);
    }
  };
  const cell = (value) => formatValue({ ...DEFAULT_OPTIONS, depth: 1, seen: [], circular: new SafeMap() }, value, 1);
  let hasValues = false;
  const addRow = (index, value) => {
    const row = { __proto__: null, "(index)": index };
    if (value !== null && typeof value === "object" && !core.isNativeError(value)) {
      const keys = properties ?? tableKeys(value)[0];
      for (let i = 0; i < keys.length; i++) {
        const key = keys[i];
        if (typeof key === "symbol") {
          continue;
        }
        addColumn(key);
        if (ObjectPrototypeHasOwnProperty(value, key)) {
          row[key] = cell(value[key]);
        }
      }
    } else {
      hasValues = true;
      row.Values = cell(value);
    }
    ArrayPrototypePush(rows, row);
  };
  let indexHeader = "(index)";
  if (core.isMap(data)) {
    indexHeader = "(iteration index)";
    addColumn("Key");
    let i = 0;
    MapPrototypeForEach(data, (value, key) => {
      addRow(`${i++}`, value);
      rows[rows.length - 1].Key = cell(key);
    });
  } else if (core.isSet(data)) {
    indexHeader = "(iteration index)";
    let i = 0;
    SetPrototypeForEach(data, (value) => addRow(`${i++}`, value));
  } else {
    const listed = tableKeys(data);
    const keys = listed[0];
    for (let i = 0; i < keys.length; i++) {
      if (typeof keys[i] === "string") {
        addRow(keys[i], data[keys[i]]);
      }
    }
    omittedRows = listed[1];
  }
  const headers = ArrayPrototypeConcat([indexHeader], columns);
  if (hasValues) {
    ArrayPrototypePush(headers, "Values");
  }
  const keysFor = ArrayPrototypeConcat(["(index)"], columns);
  if (hasValues) {
    ArrayPrototypePush(keysFor, "Values");
  }
  const widths = ArrayPrototypeMap(headers, (header) => header.length + 2);
  const table = ArrayPrototypeMap(rows, (row) =>
    ArrayPrototypeMap(keysFor, (key, column) => {
      const value = row[key] ?? "";
      widths[column] = MathMax(widths[column], value.length + 2);
      return value;
    }));
  const line = (left, middle, right) =>
    `${left}${ArrayPrototypeJoin(ArrayPrototypeMap(widths, (width) => StringPrototypeRepeat("─", width)), middle)}${right}`;
  const renderRow = (cells) =>
    `│${ArrayPrototypeJoin(ArrayPrototypeMap(cells, (value, column) => ` ${StringPrototypePadEnd(value, widths[column] - 1)}`), "│")}│`;
  const out = [line("┌", "┬", "┐"), renderRow(headers), line("├", "┼", "┤")];
  for (let i = 0; i < table.length; i++) {
    ArrayPrototypePush(out, renderRow(table[i]));
  }
  ArrayPrototypePush(out, line("└", "┴", "┘"));
  if (omittedRows > 0) {
    ArrayPrototypePush(out, `... ${omittedRows} more row${omittedRows > 1 ? "s" : ""}`);
  }
  return ArrayPrototypeJoin(out, "\n");
}

const LEVEL_DEBUG = 0;
const LEVEL_INFO = 1;
const LEVEL_WARN = 2;
const LEVEL_ERROR = 3;

/**
 * A console whose output goes to `write(level, message)`, one call per
 * console call (levels: 0 debug, 1 info, 2 warn, 3 error). `now()` times
 * console.time.
 */
function createConsole(write, now) {
  const counts = new SafeMap();
  const timers = new SafeMap();
  let indent = "";
  const print = (level, message) => {
    const text = indent === "" ? message : `${indent}${StringPrototypeReplaceAll(message, "\n", `\n${indent}`)}`;
    write(level, text);
  };
  const stackTrace = () => {
    const holder = {};
    ErrorCaptureStackTrace(holder, console.trace);
    const stack = typeof holder.stack === "string" ? holder.stack : "";
    const firstNewline = StringPrototypeIndexOf(stack, "\n");
    return firstNewline === -1 ? "" : StringPrototypeSlice(stack, firstNewline);
  };
  const console = {
    log: (...args) => print(LEVEL_INFO, formatArgs(args)),
    info: (...args) => print(LEVEL_INFO, formatArgs(args)),
    debug: (...args) => print(LEVEL_DEBUG, formatArgs(args)),
    warn: (...args) => print(LEVEL_WARN, formatArgs(args)),
    error: (...args) => print(LEVEL_ERROR, formatArgs(args)),
    dirxml: (...args) => print(LEVEL_INFO, formatArgs(args)),
    dir: (value, options = undefined) => print(LEVEL_INFO, inspect(value, options)),
    trace: (...args) => {
      const message = args.length === 0 ? "Trace" : `Trace: ${formatArgs(args)}`;
      print(LEVEL_ERROR, `${message}${stackTrace()}`);
    },
    assert: (condition = false, ...args) => {
      if (condition) {
        return;
      }
      if (args.length === 0) {
        print(LEVEL_ERROR, "Assertion failed");
      } else if (typeof args[0] === "string") {
        print(LEVEL_ERROR, `Assertion failed: ${formatArgs(args)}`);
      } else {
        print(LEVEL_ERROR, `Assertion failed ${formatArgs(args)}`);
      }
    },
    table: (data, properties = undefined) => {
      if (data === null || typeof data !== "object") {
        print(LEVEL_INFO, formatArgs([data]));
        return;
      }
      const columns = ArrayIsArray(properties) ? ArrayPrototypeSlice(properties) : undefined;
      print(LEVEL_INFO, formatTable(data, columns));
    },
    count: (label = "default") => {
      const key = `${label}`;
      const count = (MapPrototypeGet(counts, key) ?? 0) + 1;
      MapPrototypeSet(counts, key, count);
      print(LEVEL_INFO, `${key}: ${count}`);
    },
    countReset: (label = "default") => {
      const key = `${label}`;
      if (MapPrototypeHas(counts, key)) {
        MapPrototypeSet(counts, key, 0);
      } else {
        print(LEVEL_WARN, `Count for '${key}' does not exist`);
      }
    },
    time: (label = "default") => {
      const key = `${label}`;
      if (MapPrototypeHas(timers, key)) {
        print(LEVEL_WARN, `Timer '${key}' already exists`);
        return;
      }
      MapPrototypeSet(timers, key, now());
    },
    timeLog: (label = "default", ...args) => {
      const key = `${label}`;
      if (!MapPrototypeHas(timers, key)) {
        print(LEVEL_WARN, `Timer '${key}' does not exist`);
        return;
      }
      const elapsed = formatElapsed(now() - MapPrototypeGet(timers, key));
      print(LEVEL_INFO, args.length === 0 ? `${key}: ${elapsed}` : `${key}: ${elapsed} ${formatArgs(args)}`);
    },
    timeEnd: (label = "default") => {
      const key = `${label}`;
      if (!MapPrototypeHas(timers, key)) {
        print(LEVEL_WARN, `Timer '${key}' does not exist`);
        return;
      }
      const elapsed = formatElapsed(now() - MapPrototypeGet(timers, key));
      MapPrototypeDelete(timers, key);
      print(LEVEL_INFO, `${key}: ${elapsed}`);
    },
    group: (...label) => {
      if (label.length > 0) {
        print(LEVEL_INFO, formatArgs(label));
      }
      indent += "  ";
    },
    groupCollapsed: (...label) => {
      if (label.length > 0) {
        print(LEVEL_INFO, formatArgs(label));
      }
      indent += "  ";
    },
    groupEnd: () => {
      indent = StringPrototypeSlice(indent, 0, MathMax(0, indent.length - 2));
    },
    clear: () => {},
    profile: () => {},
    profileEnd: () => {},
    timeStamp: () => {},
  };
  ObjectDefineProperty(console, SymbolToStringTag, {
    __proto__: null,
    value: "console",
    configurable: true,
  });
  return console;
}

function formatElapsed(ms) {
  if (ms >= 1000) {
    return `${NumberPrototypeToFixed(ms / 1000, 3)}s`;
  }
  return `${NumberPrototypeToFixed(ms, 3)}ms`;
}

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

return { createConsole, createFilteredInspectProxy, formatArgs, inspect };
})();
