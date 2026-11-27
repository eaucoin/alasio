#!/usr/bin/env -S node --disable-warning=ExperimentalWarning
import {
  __toESM,
  __commonJS,
  __require,
  require_src,
  require_src2,
  readTokenFile
} from "./main-qc1nvyfk.js";

// node_modules/.bun/@opentelemetry+context-async-hooks@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/context-async-hooks/build/src/AbstractAsyncHooksContextManager.js
var require_AbstractAsyncHooksContextManager = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.AbstractAsyncHooksContextManager = undefined;
  var events_1 = __require("events");
  var ADD_LISTENER_METHODS = [
    "addListener",
    "on",
    "once",
    "prependListener",
    "prependOnceListener"
  ];

  class AbstractAsyncHooksContextManager {
    bind(context, target) {
      if (target instanceof events_1.EventEmitter) {
        return this._bindEventEmitter(context, target);
      }
      if (typeof target === "function") {
        return this._bindFunction(context, target);
      }
      return target;
    }
    _bindFunction(context, target) {
      const manager = this;
      const contextWrapper = function(...args) {
        return manager.with(context, () => target.apply(this, args));
      };
      Object.defineProperty(contextWrapper, "length", {
        enumerable: false,
        configurable: true,
        writable: false,
        value: target.length
      });
      return contextWrapper;
    }
    _bindEventEmitter(context, ee) {
      const map = this._getPatchMap(ee);
      if (map !== undefined)
        return ee;
      this._createPatchMap(ee);
      ADD_LISTENER_METHODS.forEach((methodName) => {
        if (ee[methodName] === undefined)
          return;
        ee[methodName] = this._patchAddListener(ee, ee[methodName], context);
      });
      if (typeof ee.removeListener === "function") {
        ee.removeListener = this._patchRemoveListener(ee, ee.removeListener);
      }
      if (typeof ee.off === "function") {
        ee.off = this._patchRemoveListener(ee, ee.off);
      }
      if (typeof ee.removeAllListeners === "function") {
        ee.removeAllListeners = this._patchRemoveAllListeners(ee, ee.removeAllListeners);
      }
      return ee;
    }
    _patchRemoveListener(ee, original) {
      const contextManager = this;
      return function(event, listener) {
        const events = contextManager._getPatchMap(ee)?.[event];
        if (events === undefined) {
          return original.call(this, event, listener);
        }
        const patchedListener = events.get(listener);
        return original.call(this, event, patchedListener || listener);
      };
    }
    _patchRemoveAllListeners(ee, original) {
      const contextManager = this;
      return function(event) {
        const map = contextManager._getPatchMap(ee);
        if (map !== undefined) {
          if (arguments.length === 0) {
            contextManager._createPatchMap(ee);
          } else if (map[event] !== undefined) {
            delete map[event];
          }
        }
        return original.apply(this, arguments);
      };
    }
    _patchAddListener(ee, original, context) {
      const contextManager = this;
      return function(event, listener) {
        if (contextManager._wrapped) {
          return original.call(this, event, listener);
        }
        let map = contextManager._getPatchMap(ee);
        if (map === undefined) {
          map = contextManager._createPatchMap(ee);
        }
        let listeners = map[event];
        if (listeners === undefined) {
          listeners = new WeakMap;
          map[event] = listeners;
        }
        const patchedListener = contextManager.bind(context, listener);
        listeners.set(listener, patchedListener);
        contextManager._wrapped = true;
        try {
          return original.call(this, event, patchedListener);
        } finally {
          contextManager._wrapped = false;
        }
      };
    }
    _createPatchMap(ee) {
      const map = Object.create(null);
      ee[this._kOtListeners] = map;
      return map;
    }
    _getPatchMap(ee) {
      return ee[this._kOtListeners];
    }
    _kOtListeners = Symbol("OtListeners");
    _wrapped = false;
  }
  exports.AbstractAsyncHooksContextManager = AbstractAsyncHooksContextManager;
});

// node_modules/.bun/@opentelemetry+context-async-hooks@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/context-async-hooks/build/src/AsyncHooksContextManager.js
var require_AsyncHooksContextManager = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.AsyncHooksContextManager = undefined;
  var api_1 = require_src();
  var asyncHooks = __require("async_hooks");
  var AbstractAsyncHooksContextManager_1 = require_AbstractAsyncHooksContextManager();

  class AsyncHooksContextManager extends AbstractAsyncHooksContextManager_1.AbstractAsyncHooksContextManager {
    _asyncHook;
    _contexts = new Map;
    _stack = [];
    constructor() {
      super();
      this._asyncHook = asyncHooks.createHook({
        init: this._init.bind(this),
        before: this._before.bind(this),
        after: this._after.bind(this),
        destroy: this._destroy.bind(this),
        promiseResolve: this._destroy.bind(this)
      });
    }
    active() {
      return this._stack[this._stack.length - 1] ?? api_1.ROOT_CONTEXT;
    }
    with(context, fn, thisArg, ...args) {
      this._enterContext(context);
      try {
        return fn.call(thisArg, ...args);
      } finally {
        this._exitContext();
      }
    }
    enable() {
      this._asyncHook.enable();
      return this;
    }
    disable() {
      this._asyncHook.disable();
      this._contexts.clear();
      this._stack = [];
      return this;
    }
    _init(uid, type) {
      if (type === "TIMERWRAP")
        return;
      const context = this._stack[this._stack.length - 1];
      if (context !== undefined) {
        this._contexts.set(uid, context);
      }
    }
    _destroy(uid) {
      this._contexts.delete(uid);
    }
    _before(uid) {
      const context = this._contexts.get(uid);
      if (context !== undefined) {
        this._enterContext(context);
      }
    }
    _after() {
      this._exitContext();
    }
    _enterContext(context) {
      this._stack.push(context);
    }
    _exitContext() {
      this._stack.pop();
    }
  }
  exports.AsyncHooksContextManager = AsyncHooksContextManager;
});

// node_modules/.bun/@opentelemetry+context-async-hooks@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/context-async-hooks/build/src/AsyncLocalStorageContextManager.js
var require_AsyncLocalStorageContextManager = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.AsyncLocalStorageContextManager = undefined;
  var api_1 = require_src();
  var async_hooks_1 = __require("async_hooks");
  var AbstractAsyncHooksContextManager_1 = require_AbstractAsyncHooksContextManager();

  class DisposeOnceToken {
    _isDisposed = false;
    _previousContext;
    _asyncLocalStorage;
    constructor(previousContext, asyncLocalStorage) {
      this._previousContext = previousContext;
      this._asyncLocalStorage = asyncLocalStorage;
    }
    dispose() {
      if (this._isDisposed) {
        return;
      }
      this._asyncLocalStorage.enterWith(this._previousContext);
      this._isDisposed = true;
    }
  }

  class AsyncLocalStorageContextManager extends AbstractAsyncHooksContextManager_1.AbstractAsyncHooksContextManager {
    _asyncLocalStorage;
    constructor() {
      super();
      this._asyncLocalStorage = new async_hooks_1.AsyncLocalStorage;
    }
    active() {
      return this._asyncLocalStorage.getStore() ?? api_1.ROOT_CONTEXT;
    }
    with(context, fn, thisArg, ...args) {
      const cb = thisArg == null ? fn : fn.bind(thisArg);
      return this._asyncLocalStorage.run(context, cb, ...args);
    }
    enable() {
      return this;
    }
    disable() {
      this._asyncLocalStorage.disable();
      return this;
    }
    attach(context) {
      const withScope = this._asyncLocalStorage.withScope;
      if (withScope) {
        return withScope.call(this._asyncLocalStorage, context);
      }
      const previousContext = this.active();
      this._asyncLocalStorage.enterWith(context);
      return new DisposeOnceToken(previousContext, this._asyncLocalStorage);
    }
  }
  exports.AsyncLocalStorageContextManager = AsyncLocalStorageContextManager;
});

// node_modules/.bun/@opentelemetry+context-async-hooks@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/context-async-hooks/build/src/index.js
var require_src3 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.AsyncLocalStorageContextManager = exports.AsyncHooksContextManager = undefined;
  var AsyncHooksContextManager_1 = require_AsyncHooksContextManager();
  Object.defineProperty(exports, "AsyncHooksContextManager", { enumerable: true, get: function() {
    return AsyncHooksContextManager_1.AsyncHooksContextManager;
  } });
  var AsyncLocalStorageContextManager_1 = require_AsyncLocalStorageContextManager();
  Object.defineProperty(exports, "AsyncLocalStorageContextManager", { enumerable: true, get: function() {
    return AsyncLocalStorageContextManager_1.AsyncLocalStorageContextManager;
  } });
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/trace/suppress-tracing.js
var require_suppress_tracing = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.isTracingSuppressed = exports.unsuppressTracing = exports.suppressTracing = undefined;
  var api_1 = require_src();
  var SUPPRESS_TRACING_KEY = (0, api_1.createContextKey)("OpenTelemetry SDK Context Key SUPPRESS_TRACING");
  function suppressTracing(context) {
    return context.setValue(SUPPRESS_TRACING_KEY, true);
  }
  exports.suppressTracing = suppressTracing;
  function unsuppressTracing(context) {
    return context.deleteValue(SUPPRESS_TRACING_KEY);
  }
  exports.unsuppressTracing = unsuppressTracing;
  function isTracingSuppressed(context) {
    return context.getValue(SUPPRESS_TRACING_KEY) === true;
  }
  exports.isTracingSuppressed = isTracingSuppressed;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/baggage/constants.js
var require_constants = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.BAGGAGE_MAX_TOTAL_LENGTH = exports.BAGGAGE_MAX_PER_NAME_VALUE_PAIRS = exports.BAGGAGE_MAX_NAME_VALUE_PAIRS = exports.BAGGAGE_HEADER = exports.BAGGAGE_ITEMS_SEPARATOR = exports.BAGGAGE_PROPERTIES_SEPARATOR = exports.BAGGAGE_KEY_PAIR_SEPARATOR = undefined;
  exports.BAGGAGE_KEY_PAIR_SEPARATOR = "=";
  exports.BAGGAGE_PROPERTIES_SEPARATOR = ";";
  exports.BAGGAGE_ITEMS_SEPARATOR = ",";
  exports.BAGGAGE_HEADER = "baggage";
  exports.BAGGAGE_MAX_NAME_VALUE_PAIRS = 180;
  exports.BAGGAGE_MAX_PER_NAME_VALUE_PAIRS = 4096;
  exports.BAGGAGE_MAX_TOTAL_LENGTH = 8192;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/baggage/utils.js
var require_utils = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.parseKeyPairsIntoRecord = exports.parseBaggageHeaderString = exports.parsePairKeyValue = exports.getKeyPairs = exports.serializeKeyPairs = undefined;
  var api_1 = require_src();
  var constants_1 = require_constants();
  function serializeKeyPairs(keyPairs) {
    return keyPairs.reduce((hValue, current) => {
      const value = `${hValue}${hValue !== "" ? constants_1.BAGGAGE_ITEMS_SEPARATOR : ""}${current}`;
      return value.length > constants_1.BAGGAGE_MAX_TOTAL_LENGTH ? hValue : value;
    }, "");
  }
  exports.serializeKeyPairs = serializeKeyPairs;
  function getKeyPairs(baggage) {
    return baggage.getAllEntries().map(([key, value]) => {
      let entry = `${encodeURIComponent(key)}=${encodeURIComponent(value.value)}`;
      if (value.metadata !== undefined) {
        entry += constants_1.BAGGAGE_PROPERTIES_SEPARATOR + value.metadata.toString();
      }
      return entry;
    });
  }
  exports.getKeyPairs = getKeyPairs;
  function parsePairKeyValue(entry) {
    if (!entry)
      return;
    const metadataSeparatorIndex = entry.indexOf(constants_1.BAGGAGE_PROPERTIES_SEPARATOR);
    const keyPairPart = metadataSeparatorIndex === -1 ? entry : entry.substring(0, metadataSeparatorIndex);
    const separatorIndex = keyPairPart.indexOf(constants_1.BAGGAGE_KEY_PAIR_SEPARATOR);
    if (separatorIndex <= 0)
      return;
    const rawKey = keyPairPart.substring(0, separatorIndex).trim();
    const rawValue = keyPairPart.substring(separatorIndex + 1).trim();
    if (!rawKey || !rawValue)
      return;
    let key;
    let value;
    try {
      key = decodeURIComponent(rawKey);
      value = decodeURIComponent(rawValue);
    } catch {
      return;
    }
    let metadata;
    if (metadataSeparatorIndex !== -1 && metadataSeparatorIndex < entry.length - 1) {
      const metadataString = entry.substring(metadataSeparatorIndex + 1);
      metadata = (0, api_1.baggageEntryMetadataFromString)(metadataString);
    }
    return { key, value, metadata };
  }
  exports.parsePairKeyValue = parsePairKeyValue;
  function parseBaggageHeaderString(value, baggage, count, totalSize) {
    let start = 0;
    while (start < value.length && count < constants_1.BAGGAGE_MAX_NAME_VALUE_PAIRS) {
      const end = value.indexOf(constants_1.BAGGAGE_ITEMS_SEPARATOR, start);
      const entryEnd = end === -1 ? value.length : end;
      const entryLength = entryEnd - start;
      if (entryLength <= constants_1.BAGGAGE_MAX_PER_NAME_VALUE_PAIRS) {
        const keyPair = parsePairKeyValue(value.substring(start, entryEnd));
        if (keyPair) {
          const entrySize = (count === 0 ? 0 : 1) + entryLength;
          if (totalSize + entrySize > constants_1.BAGGAGE_MAX_TOTAL_LENGTH)
            break;
          baggage[keyPair.key] = keyPair.metadata ? { value: keyPair.value, metadata: keyPair.metadata } : { value: keyPair.value };
          count++;
          totalSize += entrySize;
        }
      }
      if (end === -1)
        break;
      start = end + 1;
    }
    return [count, totalSize];
  }
  exports.parseBaggageHeaderString = parseBaggageHeaderString;
  function parseKeyPairsIntoRecord(value) {
    const result = {};
    if (typeof value === "string" && value.length > 0) {
      value.split(constants_1.BAGGAGE_ITEMS_SEPARATOR).forEach((entry) => {
        const keyPair = parsePairKeyValue(entry);
        if (keyPair !== undefined && keyPair.value.length > 0) {
          result[keyPair.key] = keyPair.value;
        }
      });
    }
    return result;
  }
  exports.parseKeyPairsIntoRecord = parseKeyPairsIntoRecord;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/baggage/propagation/W3CBaggagePropagator.js
var require_W3CBaggagePropagator = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.W3CBaggagePropagator = undefined;
  var api_1 = require_src();
  var suppress_tracing_1 = require_suppress_tracing();
  var constants_1 = require_constants();
  var utils_1 = require_utils();

  class W3CBaggagePropagator {
    inject(context, carrier, setter) {
      const baggage = api_1.propagation.getBaggage(context);
      if (!baggage || (0, suppress_tracing_1.isTracingSuppressed)(context))
        return;
      const keyPairs = (0, utils_1.getKeyPairs)(baggage).filter((pair) => {
        return pair.length <= constants_1.BAGGAGE_MAX_PER_NAME_VALUE_PAIRS;
      }).slice(0, constants_1.BAGGAGE_MAX_NAME_VALUE_PAIRS);
      const headerValue = (0, utils_1.serializeKeyPairs)(keyPairs);
      if (headerValue.length > 0) {
        setter.set(carrier, constants_1.BAGGAGE_HEADER, headerValue);
      }
    }
    extract(context, carrier, getter) {
      const headerValue = getter.get(carrier, constants_1.BAGGAGE_HEADER);
      if (!headerValue) {
        return context;
      }
      const baggage = {};
      let count = 0;
      let totalSize = 0;
      if (Array.isArray(headerValue)) {
        for (let i = 0;i < headerValue.length; i++) {
          [count, totalSize] = (0, utils_1.parseBaggageHeaderString)(headerValue[i], baggage, count, totalSize);
        }
      } else {
        [count] = (0, utils_1.parseBaggageHeaderString)(headerValue, baggage, count, totalSize);
      }
      if (count === 0) {
        return context;
      }
      return api_1.propagation.setBaggage(context, api_1.propagation.createBaggage(baggage));
    }
    fields() {
      return [constants_1.BAGGAGE_HEADER];
    }
  }
  exports.W3CBaggagePropagator = W3CBaggagePropagator;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/common/anchored-clock.js
var require_anchored_clock = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.AnchoredClock = undefined;

  class AnchoredClock {
    _monotonicClock;
    _epochMillis;
    _performanceMillis;
    constructor(systemClock, monotonicClock) {
      this._monotonicClock = monotonicClock;
      this._epochMillis = systemClock.now();
      this._performanceMillis = monotonicClock.now();
    }
    now() {
      const delta = this._monotonicClock.now() - this._performanceMillis;
      return this._epochMillis + delta;
    }
  }
  exports.AnchoredClock = AnchoredClock;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/common/attributes.js
var require_attributes = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.isAttributeValue = exports.isAttributeKey = exports.sanitizeAttributes = undefined;
  var api_1 = require_src();
  function sanitizeAttributes(attributes) {
    const out = {};
    if (typeof attributes !== "object" || attributes == null) {
      return out;
    }
    for (const key in attributes) {
      if (!Object.prototype.hasOwnProperty.call(attributes, key)) {
        continue;
      }
      if (!isAttributeKey(key)) {
        api_1.diag.warn(`Invalid attribute key: ${key}`);
        continue;
      }
      const val = attributes[key];
      if (!isAttributeValue(val)) {
        api_1.diag.warn(`Invalid attribute value set for key: ${key}`);
        continue;
      }
      if (Array.isArray(val)) {
        out[key] = val.slice();
      } else {
        out[key] = val;
      }
    }
    return out;
  }
  exports.sanitizeAttributes = sanitizeAttributes;
  function isAttributeKey(key) {
    return typeof key === "string" && key !== "";
  }
  exports.isAttributeKey = isAttributeKey;
  function isAttributeValue(val) {
    if (val == null) {
      return true;
    }
    if (Array.isArray(val)) {
      return isHomogeneousAttributeValueArray(val);
    }
    return isValidPrimitiveAttributeValueType(typeof val);
  }
  exports.isAttributeValue = isAttributeValue;
  function isHomogeneousAttributeValueArray(arr) {
    let type;
    for (const element of arr) {
      if (element == null)
        continue;
      const elementType = typeof element;
      if (elementType === type) {
        continue;
      }
      if (!type) {
        if (isValidPrimitiveAttributeValueType(elementType)) {
          type = elementType;
          continue;
        }
        return false;
      }
      return false;
    }
    return true;
  }
  function isValidPrimitiveAttributeValueType(valType) {
    switch (valType) {
      case "number":
      case "boolean":
      case "string":
        return true;
    }
    return false;
  }
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/common/logging-error-handler.js
var require_logging_error_handler = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.loggingErrorHandler = undefined;
  var api_1 = require_src();
  function loggingErrorHandler() {
    return (ex) => {
      api_1.diag.error(stringifyException(ex));
    };
  }
  exports.loggingErrorHandler = loggingErrorHandler;
  function stringifyException(ex) {
    if (typeof ex === "string") {
      return ex;
    } else {
      return JSON.stringify(flattenException(ex));
    }
  }
  function flattenException(ex) {
    const result = {};
    let current = ex;
    while (current !== null) {
      Object.getOwnPropertyNames(current).forEach((propertyName) => {
        if (result[propertyName])
          return;
        const value = current[propertyName];
        if (value) {
          result[propertyName] = String(value);
        }
      });
      current = Object.getPrototypeOf(current);
    }
    return result;
  }
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/common/global-error-handler.js
var require_global_error_handler = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.globalErrorHandler = exports.setGlobalErrorHandler = undefined;
  var logging_error_handler_1 = require_logging_error_handler();
  var delegateHandler = (0, logging_error_handler_1.loggingErrorHandler)();
  function setGlobalErrorHandler(handler) {
    delegateHandler = handler;
  }
  exports.setGlobalErrorHandler = setGlobalErrorHandler;
  function globalErrorHandler(ex) {
    try {
      delegateHandler(ex);
    } catch {}
  }
  exports.globalErrorHandler = globalErrorHandler;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/platform/node/environment.js
var require_environment = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.getStringListFromEnv = exports.getBooleanFromEnv = exports.getStringFromEnv = exports.getNumberFromEnv = undefined;
  var api_1 = require_src();
  var util_1 = __require("util");
  function getNumberFromEnv(key) {
    const raw = process.env[key];
    if (raw == null || raw.trim() === "") {
      return;
    }
    const value = Number(raw);
    if (isNaN(value)) {
      api_1.diag.warn(`Unknown value ${(0, util_1.inspect)(raw)} for ${key}, expected a number, using defaults`);
      return;
    }
    return value;
  }
  exports.getNumberFromEnv = getNumberFromEnv;
  function getStringFromEnv(key) {
    const raw = process.env[key];
    if (raw == null || raw.trim() === "") {
      return;
    }
    return raw;
  }
  exports.getStringFromEnv = getStringFromEnv;
  function getBooleanFromEnv(key) {
    const raw = process.env[key]?.trim().toLowerCase();
    if (raw == null || raw === "") {
      return false;
    }
    if (raw === "true") {
      return true;
    } else if (raw === "false") {
      return false;
    } else {
      api_1.diag.warn(`Unknown value ${(0, util_1.inspect)(raw)} for ${key}, expected 'true' or 'false', falling back to 'false' (default)`);
      return false;
    }
  }
  exports.getBooleanFromEnv = getBooleanFromEnv;
  function getStringListFromEnv(key) {
    return getStringFromEnv(key)?.split(",").map((v) => v.trim()).filter((s) => s !== "");
  }
  exports.getStringListFromEnv = getStringListFromEnv;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/common/globalThis.js
var require_globalThis = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports._globalThis = undefined;
  exports._globalThis = globalThis;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/version.js
var require_version = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.VERSION = undefined;
  exports.VERSION = "2.11.0";
});

// node_modules/.bun/@opentelemetry+semantic-conventions@1.43.0/node_modules/@opentelemetry/semantic-conventions/build/src/internal/utils.js
var require_utils2 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.createConstMap = undefined;
  function createConstMap(values) {
    let res = {};
    const len = values.length;
    for (let lp = 0;lp < len; lp++) {
      const val = values[lp];
      if (val) {
        res[String(val).toUpperCase().replace(/[-.]/g, "_")] = val;
      }
    }
    return res;
  }
  exports.createConstMap = createConstMap;
});

// node_modules/.bun/@opentelemetry+semantic-conventions@1.43.0/node_modules/@opentelemetry/semantic-conventions/build/src/trace/SemanticAttributes.js
var require_SemanticAttributes = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.SEMATTRS_NET_HOST_CARRIER_ICC = exports.SEMATTRS_NET_HOST_CARRIER_MNC = exports.SEMATTRS_NET_HOST_CARRIER_MCC = exports.SEMATTRS_NET_HOST_CARRIER_NAME = exports.SEMATTRS_NET_HOST_CONNECTION_SUBTYPE = exports.SEMATTRS_NET_HOST_CONNECTION_TYPE = exports.SEMATTRS_NET_HOST_NAME = exports.SEMATTRS_NET_HOST_PORT = exports.SEMATTRS_NET_HOST_IP = exports.SEMATTRS_NET_PEER_NAME = exports.SEMATTRS_NET_PEER_PORT = exports.SEMATTRS_NET_PEER_IP = exports.SEMATTRS_NET_TRANSPORT = exports.SEMATTRS_FAAS_INVOKED_REGION = exports.SEMATTRS_FAAS_INVOKED_PROVIDER = exports.SEMATTRS_FAAS_INVOKED_NAME = exports.SEMATTRS_FAAS_COLDSTART = exports.SEMATTRS_FAAS_CRON = exports.SEMATTRS_FAAS_TIME = exports.SEMATTRS_FAAS_DOCUMENT_NAME = exports.SEMATTRS_FAAS_DOCUMENT_TIME = exports.SEMATTRS_FAAS_DOCUMENT_OPERATION = exports.SEMATTRS_FAAS_DOCUMENT_COLLECTION = exports.SEMATTRS_FAAS_EXECUTION = exports.SEMATTRS_FAAS_TRIGGER = exports.SEMATTRS_EXCEPTION_ESCAPED = exports.SEMATTRS_EXCEPTION_STACKTRACE = exports.SEMATTRS_EXCEPTION_MESSAGE = exports.SEMATTRS_EXCEPTION_TYPE = exports.SEMATTRS_DB_SQL_TABLE = exports.SEMATTRS_DB_MONGODB_COLLECTION = exports.SEMATTRS_DB_REDIS_DATABASE_INDEX = exports.SEMATTRS_DB_HBASE_NAMESPACE = exports.SEMATTRS_DB_CASSANDRA_COORDINATOR_DC = exports.SEMATTRS_DB_CASSANDRA_COORDINATOR_ID = exports.SEMATTRS_DB_CASSANDRA_SPECULATIVE_EXECUTION_COUNT = exports.SEMATTRS_DB_CASSANDRA_IDEMPOTENCE = exports.SEMATTRS_DB_CASSANDRA_TABLE = exports.SEMATTRS_DB_CASSANDRA_CONSISTENCY_LEVEL = exports.SEMATTRS_DB_CASSANDRA_PAGE_SIZE = exports.SEMATTRS_DB_CASSANDRA_KEYSPACE = exports.SEMATTRS_DB_MSSQL_INSTANCE_NAME = exports.SEMATTRS_DB_OPERATION = exports.SEMATTRS_DB_STATEMENT = exports.SEMATTRS_DB_NAME = exports.SEMATTRS_DB_JDBC_DRIVER_CLASSNAME = exports.SEMATTRS_DB_USER = exports.SEMATTRS_DB_CONNECTION_STRING = exports.SEMATTRS_DB_SYSTEM = exports.SEMATTRS_AWS_LAMBDA_INVOKED_ARN = undefined;
  exports.SEMATTRS_MESSAGING_DESTINATION_KIND = exports.SEMATTRS_MESSAGING_DESTINATION = exports.SEMATTRS_MESSAGING_SYSTEM = exports.SEMATTRS_AWS_DYNAMODB_GLOBAL_SECONDARY_INDEX_UPDATES = exports.SEMATTRS_AWS_DYNAMODB_ATTRIBUTE_DEFINITIONS = exports.SEMATTRS_AWS_DYNAMODB_SCANNED_COUNT = exports.SEMATTRS_AWS_DYNAMODB_COUNT = exports.SEMATTRS_AWS_DYNAMODB_TOTAL_SEGMENTS = exports.SEMATTRS_AWS_DYNAMODB_SEGMENT = exports.SEMATTRS_AWS_DYNAMODB_SCAN_FORWARD = exports.SEMATTRS_AWS_DYNAMODB_TABLE_COUNT = exports.SEMATTRS_AWS_DYNAMODB_EXCLUSIVE_START_TABLE = exports.SEMATTRS_AWS_DYNAMODB_LOCAL_SECONDARY_INDEXES = exports.SEMATTRS_AWS_DYNAMODB_GLOBAL_SECONDARY_INDEXES = exports.SEMATTRS_AWS_DYNAMODB_SELECT = exports.SEMATTRS_AWS_DYNAMODB_INDEX_NAME = exports.SEMATTRS_AWS_DYNAMODB_ATTRIBUTES_TO_GET = exports.SEMATTRS_AWS_DYNAMODB_LIMIT = exports.SEMATTRS_AWS_DYNAMODB_PROJECTION = exports.SEMATTRS_AWS_DYNAMODB_CONSISTENT_READ = exports.SEMATTRS_AWS_DYNAMODB_PROVISIONED_WRITE_CAPACITY = exports.SEMATTRS_AWS_DYNAMODB_PROVISIONED_READ_CAPACITY = exports.SEMATTRS_AWS_DYNAMODB_ITEM_COLLECTION_METRICS = exports.SEMATTRS_AWS_DYNAMODB_CONSUMED_CAPACITY = exports.SEMATTRS_AWS_DYNAMODB_TABLE_NAMES = exports.SEMATTRS_HTTP_CLIENT_IP = exports.SEMATTRS_HTTP_ROUTE = exports.SEMATTRS_HTTP_SERVER_NAME = exports.SEMATTRS_HTTP_RESPONSE_CONTENT_LENGTH_UNCOMPRESSED = exports.SEMATTRS_HTTP_RESPONSE_CONTENT_LENGTH = exports.SEMATTRS_HTTP_REQUEST_CONTENT_LENGTH_UNCOMPRESSED = exports.SEMATTRS_HTTP_REQUEST_CONTENT_LENGTH = exports.SEMATTRS_HTTP_USER_AGENT = exports.SEMATTRS_HTTP_FLAVOR = exports.SEMATTRS_HTTP_STATUS_CODE = exports.SEMATTRS_HTTP_SCHEME = exports.SEMATTRS_HTTP_HOST = exports.SEMATTRS_HTTP_TARGET = exports.SEMATTRS_HTTP_URL = exports.SEMATTRS_HTTP_METHOD = exports.SEMATTRS_CODE_LINENO = exports.SEMATTRS_CODE_FILEPATH = exports.SEMATTRS_CODE_NAMESPACE = exports.SEMATTRS_CODE_FUNCTION = exports.SEMATTRS_THREAD_NAME = exports.SEMATTRS_THREAD_ID = exports.SEMATTRS_ENDUSER_SCOPE = exports.SEMATTRS_ENDUSER_ROLE = exports.SEMATTRS_ENDUSER_ID = exports.SEMATTRS_PEER_SERVICE = undefined;
  exports.DBSYSTEMVALUES_FILEMAKER = exports.DBSYSTEMVALUES_DERBY = exports.DBSYSTEMVALUES_FIREBIRD = exports.DBSYSTEMVALUES_ADABAS = exports.DBSYSTEMVALUES_CACHE = exports.DBSYSTEMVALUES_EDB = exports.DBSYSTEMVALUES_FIRSTSQL = exports.DBSYSTEMVALUES_INGRES = exports.DBSYSTEMVALUES_HANADB = exports.DBSYSTEMVALUES_MAXDB = exports.DBSYSTEMVALUES_PROGRESS = exports.DBSYSTEMVALUES_HSQLDB = exports.DBSYSTEMVALUES_CLOUDSCAPE = exports.DBSYSTEMVALUES_HIVE = exports.DBSYSTEMVALUES_REDSHIFT = exports.DBSYSTEMVALUES_POSTGRESQL = exports.DBSYSTEMVALUES_DB2 = exports.DBSYSTEMVALUES_ORACLE = exports.DBSYSTEMVALUES_MYSQL = exports.DBSYSTEMVALUES_MSSQL = exports.DBSYSTEMVALUES_OTHER_SQL = exports.SemanticAttributes = exports.SEMATTRS_MESSAGE_UNCOMPRESSED_SIZE = exports.SEMATTRS_MESSAGE_COMPRESSED_SIZE = exports.SEMATTRS_MESSAGE_ID = exports.SEMATTRS_MESSAGE_TYPE = exports.SEMATTRS_RPC_JSONRPC_ERROR_MESSAGE = exports.SEMATTRS_RPC_JSONRPC_ERROR_CODE = exports.SEMATTRS_RPC_JSONRPC_REQUEST_ID = exports.SEMATTRS_RPC_JSONRPC_VERSION = exports.SEMATTRS_RPC_GRPC_STATUS_CODE = exports.SEMATTRS_RPC_METHOD = exports.SEMATTRS_RPC_SERVICE = exports.SEMATTRS_RPC_SYSTEM = exports.SEMATTRS_MESSAGING_KAFKA_TOMBSTONE = exports.SEMATTRS_MESSAGING_KAFKA_PARTITION = exports.SEMATTRS_MESSAGING_KAFKA_CLIENT_ID = exports.SEMATTRS_MESSAGING_KAFKA_CONSUMER_GROUP = exports.SEMATTRS_MESSAGING_KAFKA_MESSAGE_KEY = exports.SEMATTRS_MESSAGING_RABBITMQ_ROUTING_KEY = exports.SEMATTRS_MESSAGING_CONSUMER_ID = exports.SEMATTRS_MESSAGING_OPERATION = exports.SEMATTRS_MESSAGING_MESSAGE_PAYLOAD_COMPRESSED_SIZE_BYTES = exports.SEMATTRS_MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES = exports.SEMATTRS_MESSAGING_CONVERSATION_ID = exports.SEMATTRS_MESSAGING_MESSAGE_ID = exports.SEMATTRS_MESSAGING_URL = exports.SEMATTRS_MESSAGING_PROTOCOL_VERSION = exports.SEMATTRS_MESSAGING_PROTOCOL = exports.SEMATTRS_MESSAGING_TEMP_DESTINATION = undefined;
  exports.FAASINVOKEDPROVIDERVALUES_ALIBABA_CLOUD = exports.FaasDocumentOperationValues = exports.FAASDOCUMENTOPERATIONVALUES_DELETE = exports.FAASDOCUMENTOPERATIONVALUES_EDIT = exports.FAASDOCUMENTOPERATIONVALUES_INSERT = exports.FaasTriggerValues = exports.FAASTRIGGERVALUES_OTHER = exports.FAASTRIGGERVALUES_TIMER = exports.FAASTRIGGERVALUES_PUBSUB = exports.FAASTRIGGERVALUES_HTTP = exports.FAASTRIGGERVALUES_DATASOURCE = exports.DbCassandraConsistencyLevelValues = exports.DBCASSANDRACONSISTENCYLEVELVALUES_LOCAL_SERIAL = exports.DBCASSANDRACONSISTENCYLEVELVALUES_SERIAL = exports.DBCASSANDRACONSISTENCYLEVELVALUES_ANY = exports.DBCASSANDRACONSISTENCYLEVELVALUES_LOCAL_ONE = exports.DBCASSANDRACONSISTENCYLEVELVALUES_THREE = exports.DBCASSANDRACONSISTENCYLEVELVALUES_TWO = exports.DBCASSANDRACONSISTENCYLEVELVALUES_ONE = exports.DBCASSANDRACONSISTENCYLEVELVALUES_LOCAL_QUORUM = exports.DBCASSANDRACONSISTENCYLEVELVALUES_QUORUM = exports.DBCASSANDRACONSISTENCYLEVELVALUES_EACH_QUORUM = exports.DBCASSANDRACONSISTENCYLEVELVALUES_ALL = exports.DbSystemValues = exports.DBSYSTEMVALUES_COCKROACHDB = exports.DBSYSTEMVALUES_MEMCACHED = exports.DBSYSTEMVALUES_ELASTICSEARCH = exports.DBSYSTEMVALUES_GEODE = exports.DBSYSTEMVALUES_NEO4J = exports.DBSYSTEMVALUES_DYNAMODB = exports.DBSYSTEMVALUES_COSMOSDB = exports.DBSYSTEMVALUES_COUCHDB = exports.DBSYSTEMVALUES_COUCHBASE = exports.DBSYSTEMVALUES_REDIS = exports.DBSYSTEMVALUES_MONGODB = exports.DBSYSTEMVALUES_HBASE = exports.DBSYSTEMVALUES_CASSANDRA = exports.DBSYSTEMVALUES_COLDFUSION = exports.DBSYSTEMVALUES_H2 = exports.DBSYSTEMVALUES_VERTICA = exports.DBSYSTEMVALUES_TERADATA = exports.DBSYSTEMVALUES_SYBASE = exports.DBSYSTEMVALUES_SQLITE = exports.DBSYSTEMVALUES_POINTBASE = exports.DBSYSTEMVALUES_PERVASIVE = exports.DBSYSTEMVALUES_NETEZZA = exports.DBSYSTEMVALUES_MARIADB = exports.DBSYSTEMVALUES_INTERBASE = exports.DBSYSTEMVALUES_INSTANTDB = exports.DBSYSTEMVALUES_INFORMIX = undefined;
  exports.MESSAGINGOPERATIONVALUES_RECEIVE = exports.MessagingDestinationKindValues = exports.MESSAGINGDESTINATIONKINDVALUES_TOPIC = exports.MESSAGINGDESTINATIONKINDVALUES_QUEUE = exports.HttpFlavorValues = exports.HTTPFLAVORVALUES_QUIC = exports.HTTPFLAVORVALUES_SPDY = exports.HTTPFLAVORVALUES_HTTP_2_0 = exports.HTTPFLAVORVALUES_HTTP_1_1 = exports.HTTPFLAVORVALUES_HTTP_1_0 = exports.NetHostConnectionSubtypeValues = exports.NETHOSTCONNECTIONSUBTYPEVALUES_LTE_CA = exports.NETHOSTCONNECTIONSUBTYPEVALUES_NRNSA = exports.NETHOSTCONNECTIONSUBTYPEVALUES_NR = exports.NETHOSTCONNECTIONSUBTYPEVALUES_IWLAN = exports.NETHOSTCONNECTIONSUBTYPEVALUES_TD_SCDMA = exports.NETHOSTCONNECTIONSUBTYPEVALUES_GSM = exports.NETHOSTCONNECTIONSUBTYPEVALUES_HSPAP = exports.NETHOSTCONNECTIONSUBTYPEVALUES_EHRPD = exports.NETHOSTCONNECTIONSUBTYPEVALUES_LTE = exports.NETHOSTCONNECTIONSUBTYPEVALUES_EVDO_B = exports.NETHOSTCONNECTIONSUBTYPEVALUES_IDEN = exports.NETHOSTCONNECTIONSUBTYPEVALUES_HSPA = exports.NETHOSTCONNECTIONSUBTYPEVALUES_HSUPA = exports.NETHOSTCONNECTIONSUBTYPEVALUES_HSDPA = exports.NETHOSTCONNECTIONSUBTYPEVALUES_CDMA2000_1XRTT = exports.NETHOSTCONNECTIONSUBTYPEVALUES_EVDO_A = exports.NETHOSTCONNECTIONSUBTYPEVALUES_EVDO_0 = exports.NETHOSTCONNECTIONSUBTYPEVALUES_CDMA = exports.NETHOSTCONNECTIONSUBTYPEVALUES_UMTS = exports.NETHOSTCONNECTIONSUBTYPEVALUES_EDGE = exports.NETHOSTCONNECTIONSUBTYPEVALUES_GPRS = exports.NetHostConnectionTypeValues = exports.NETHOSTCONNECTIONTYPEVALUES_UNKNOWN = exports.NETHOSTCONNECTIONTYPEVALUES_UNAVAILABLE = exports.NETHOSTCONNECTIONTYPEVALUES_CELL = exports.NETHOSTCONNECTIONTYPEVALUES_WIRED = exports.NETHOSTCONNECTIONTYPEVALUES_WIFI = exports.NetTransportValues = exports.NETTRANSPORTVALUES_OTHER = exports.NETTRANSPORTVALUES_INPROC = exports.NETTRANSPORTVALUES_PIPE = exports.NETTRANSPORTVALUES_UNIX = exports.NETTRANSPORTVALUES_IP = exports.NETTRANSPORTVALUES_IP_UDP = exports.NETTRANSPORTVALUES_IP_TCP = exports.FaasInvokedProviderValues = exports.FAASINVOKEDPROVIDERVALUES_GCP = exports.FAASINVOKEDPROVIDERVALUES_AZURE = exports.FAASINVOKEDPROVIDERVALUES_AWS = undefined;
  exports.MessageTypeValues = exports.MESSAGETYPEVALUES_RECEIVED = exports.MESSAGETYPEVALUES_SENT = exports.RpcGrpcStatusCodeValues = exports.RPCGRPCSTATUSCODEVALUES_UNAUTHENTICATED = exports.RPCGRPCSTATUSCODEVALUES_DATA_LOSS = exports.RPCGRPCSTATUSCODEVALUES_UNAVAILABLE = exports.RPCGRPCSTATUSCODEVALUES_INTERNAL = exports.RPCGRPCSTATUSCODEVALUES_UNIMPLEMENTED = exports.RPCGRPCSTATUSCODEVALUES_OUT_OF_RANGE = exports.RPCGRPCSTATUSCODEVALUES_ABORTED = exports.RPCGRPCSTATUSCODEVALUES_FAILED_PRECONDITION = exports.RPCGRPCSTATUSCODEVALUES_RESOURCE_EXHAUSTED = exports.RPCGRPCSTATUSCODEVALUES_PERMISSION_DENIED = exports.RPCGRPCSTATUSCODEVALUES_ALREADY_EXISTS = exports.RPCGRPCSTATUSCODEVALUES_NOT_FOUND = exports.RPCGRPCSTATUSCODEVALUES_DEADLINE_EXCEEDED = exports.RPCGRPCSTATUSCODEVALUES_INVALID_ARGUMENT = exports.RPCGRPCSTATUSCODEVALUES_UNKNOWN = exports.RPCGRPCSTATUSCODEVALUES_CANCELLED = exports.RPCGRPCSTATUSCODEVALUES_OK = exports.MessagingOperationValues = exports.MESSAGINGOPERATIONVALUES_PROCESS = undefined;
  var utils_1 = require_utils2();
  var TMP_AWS_LAMBDA_INVOKED_ARN = "aws.lambda.invoked_arn";
  var TMP_DB_SYSTEM = "db.system";
  var TMP_DB_CONNECTION_STRING = "db.connection_string";
  var TMP_DB_USER = "db.user";
  var TMP_DB_JDBC_DRIVER_CLASSNAME = "db.jdbc.driver_classname";
  var TMP_DB_NAME = "db.name";
  var TMP_DB_STATEMENT = "db.statement";
  var TMP_DB_OPERATION = "db.operation";
  var TMP_DB_MSSQL_INSTANCE_NAME = "db.mssql.instance_name";
  var TMP_DB_CASSANDRA_KEYSPACE = "db.cassandra.keyspace";
  var TMP_DB_CASSANDRA_PAGE_SIZE = "db.cassandra.page_size";
  var TMP_DB_CASSANDRA_CONSISTENCY_LEVEL = "db.cassandra.consistency_level";
  var TMP_DB_CASSANDRA_TABLE = "db.cassandra.table";
  var TMP_DB_CASSANDRA_IDEMPOTENCE = "db.cassandra.idempotence";
  var TMP_DB_CASSANDRA_SPECULATIVE_EXECUTION_COUNT = "db.cassandra.speculative_execution_count";
  var TMP_DB_CASSANDRA_COORDINATOR_ID = "db.cassandra.coordinator.id";
  var TMP_DB_CASSANDRA_COORDINATOR_DC = "db.cassandra.coordinator.dc";
  var TMP_DB_HBASE_NAMESPACE = "db.hbase.namespace";
  var TMP_DB_REDIS_DATABASE_INDEX = "db.redis.database_index";
  var TMP_DB_MONGODB_COLLECTION = "db.mongodb.collection";
  var TMP_DB_SQL_TABLE = "db.sql.table";
  var TMP_EXCEPTION_TYPE = "exception.type";
  var TMP_EXCEPTION_MESSAGE = "exception.message";
  var TMP_EXCEPTION_STACKTRACE = "exception.stacktrace";
  var TMP_EXCEPTION_ESCAPED = "exception.escaped";
  var TMP_FAAS_TRIGGER = "faas.trigger";
  var TMP_FAAS_EXECUTION = "faas.execution";
  var TMP_FAAS_DOCUMENT_COLLECTION = "faas.document.collection";
  var TMP_FAAS_DOCUMENT_OPERATION = "faas.document.operation";
  var TMP_FAAS_DOCUMENT_TIME = "faas.document.time";
  var TMP_FAAS_DOCUMENT_NAME = "faas.document.name";
  var TMP_FAAS_TIME = "faas.time";
  var TMP_FAAS_CRON = "faas.cron";
  var TMP_FAAS_COLDSTART = "faas.coldstart";
  var TMP_FAAS_INVOKED_NAME = "faas.invoked_name";
  var TMP_FAAS_INVOKED_PROVIDER = "faas.invoked_provider";
  var TMP_FAAS_INVOKED_REGION = "faas.invoked_region";
  var TMP_NET_TRANSPORT = "net.transport";
  var TMP_NET_PEER_IP = "net.peer.ip";
  var TMP_NET_PEER_PORT = "net.peer.port";
  var TMP_NET_PEER_NAME = "net.peer.name";
  var TMP_NET_HOST_IP = "net.host.ip";
  var TMP_NET_HOST_PORT = "net.host.port";
  var TMP_NET_HOST_NAME = "net.host.name";
  var TMP_NET_HOST_CONNECTION_TYPE = "net.host.connection.type";
  var TMP_NET_HOST_CONNECTION_SUBTYPE = "net.host.connection.subtype";
  var TMP_NET_HOST_CARRIER_NAME = "net.host.carrier.name";
  var TMP_NET_HOST_CARRIER_MCC = "net.host.carrier.mcc";
  var TMP_NET_HOST_CARRIER_MNC = "net.host.carrier.mnc";
  var TMP_NET_HOST_CARRIER_ICC = "net.host.carrier.icc";
  var TMP_PEER_SERVICE = "peer.service";
  var TMP_ENDUSER_ID = "enduser.id";
  var TMP_ENDUSER_ROLE = "enduser.role";
  var TMP_ENDUSER_SCOPE = "enduser.scope";
  var TMP_THREAD_ID = "thread.id";
  var TMP_THREAD_NAME = "thread.name";
  var TMP_CODE_FUNCTION = "code.function";
  var TMP_CODE_NAMESPACE = "code.namespace";
  var TMP_CODE_FILEPATH = "code.filepath";
  var TMP_CODE_LINENO = "code.lineno";
  var TMP_HTTP_METHOD = "http.method";
  var TMP_HTTP_URL = "http.url";
  var TMP_HTTP_TARGET = "http.target";
  var TMP_HTTP_HOST = "http.host";
  var TMP_HTTP_SCHEME = "http.scheme";
  var TMP_HTTP_STATUS_CODE = "http.status_code";
  var TMP_HTTP_FLAVOR = "http.flavor";
  var TMP_HTTP_USER_AGENT = "http.user_agent";
  var TMP_HTTP_REQUEST_CONTENT_LENGTH = "http.request_content_length";
  var TMP_HTTP_REQUEST_CONTENT_LENGTH_UNCOMPRESSED = "http.request_content_length_uncompressed";
  var TMP_HTTP_RESPONSE_CONTENT_LENGTH = "http.response_content_length";
  var TMP_HTTP_RESPONSE_CONTENT_LENGTH_UNCOMPRESSED = "http.response_content_length_uncompressed";
  var TMP_HTTP_SERVER_NAME = "http.server_name";
  var TMP_HTTP_ROUTE = "http.route";
  var TMP_HTTP_CLIENT_IP = "http.client_ip";
  var TMP_AWS_DYNAMODB_TABLE_NAMES = "aws.dynamodb.table_names";
  var TMP_AWS_DYNAMODB_CONSUMED_CAPACITY = "aws.dynamodb.consumed_capacity";
  var TMP_AWS_DYNAMODB_ITEM_COLLECTION_METRICS = "aws.dynamodb.item_collection_metrics";
  var TMP_AWS_DYNAMODB_PROVISIONED_READ_CAPACITY = "aws.dynamodb.provisioned_read_capacity";
  var TMP_AWS_DYNAMODB_PROVISIONED_WRITE_CAPACITY = "aws.dynamodb.provisioned_write_capacity";
  var TMP_AWS_DYNAMODB_CONSISTENT_READ = "aws.dynamodb.consistent_read";
  var TMP_AWS_DYNAMODB_PROJECTION = "aws.dynamodb.projection";
  var TMP_AWS_DYNAMODB_LIMIT = "aws.dynamodb.limit";
  var TMP_AWS_DYNAMODB_ATTRIBUTES_TO_GET = "aws.dynamodb.attributes_to_get";
  var TMP_AWS_DYNAMODB_INDEX_NAME = "aws.dynamodb.index_name";
  var TMP_AWS_DYNAMODB_SELECT = "aws.dynamodb.select";
  var TMP_AWS_DYNAMODB_GLOBAL_SECONDARY_INDEXES = "aws.dynamodb.global_secondary_indexes";
  var TMP_AWS_DYNAMODB_LOCAL_SECONDARY_INDEXES = "aws.dynamodb.local_secondary_indexes";
  var TMP_AWS_DYNAMODB_EXCLUSIVE_START_TABLE = "aws.dynamodb.exclusive_start_table";
  var TMP_AWS_DYNAMODB_TABLE_COUNT = "aws.dynamodb.table_count";
  var TMP_AWS_DYNAMODB_SCAN_FORWARD = "aws.dynamodb.scan_forward";
  var TMP_AWS_DYNAMODB_SEGMENT = "aws.dynamodb.segment";
  var TMP_AWS_DYNAMODB_TOTAL_SEGMENTS = "aws.dynamodb.total_segments";
  var TMP_AWS_DYNAMODB_COUNT = "aws.dynamodb.count";
  var TMP_AWS_DYNAMODB_SCANNED_COUNT = "aws.dynamodb.scanned_count";
  var TMP_AWS_DYNAMODB_ATTRIBUTE_DEFINITIONS = "aws.dynamodb.attribute_definitions";
  var TMP_AWS_DYNAMODB_GLOBAL_SECONDARY_INDEX_UPDATES = "aws.dynamodb.global_secondary_index_updates";
  var TMP_MESSAGING_SYSTEM = "messaging.system";
  var TMP_MESSAGING_DESTINATION = "messaging.destination";
  var TMP_MESSAGING_DESTINATION_KIND = "messaging.destination_kind";
  var TMP_MESSAGING_TEMP_DESTINATION = "messaging.temp_destination";
  var TMP_MESSAGING_PROTOCOL = "messaging.protocol";
  var TMP_MESSAGING_PROTOCOL_VERSION = "messaging.protocol_version";
  var TMP_MESSAGING_URL = "messaging.url";
  var TMP_MESSAGING_MESSAGE_ID = "messaging.message_id";
  var TMP_MESSAGING_CONVERSATION_ID = "messaging.conversation_id";
  var TMP_MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES = "messaging.message_payload_size_bytes";
  var TMP_MESSAGING_MESSAGE_PAYLOAD_COMPRESSED_SIZE_BYTES = "messaging.message_payload_compressed_size_bytes";
  var TMP_MESSAGING_OPERATION = "messaging.operation";
  var TMP_MESSAGING_CONSUMER_ID = "messaging.consumer_id";
  var TMP_MESSAGING_RABBITMQ_ROUTING_KEY = "messaging.rabbitmq.routing_key";
  var TMP_MESSAGING_KAFKA_MESSAGE_KEY = "messaging.kafka.message_key";
  var TMP_MESSAGING_KAFKA_CONSUMER_GROUP = "messaging.kafka.consumer_group";
  var TMP_MESSAGING_KAFKA_CLIENT_ID = "messaging.kafka.client_id";
  var TMP_MESSAGING_KAFKA_PARTITION = "messaging.kafka.partition";
  var TMP_MESSAGING_KAFKA_TOMBSTONE = "messaging.kafka.tombstone";
  var TMP_RPC_SYSTEM = "rpc.system";
  var TMP_RPC_SERVICE = "rpc.service";
  var TMP_RPC_METHOD = "rpc.method";
  var TMP_RPC_GRPC_STATUS_CODE = "rpc.grpc.status_code";
  var TMP_RPC_JSONRPC_VERSION = "rpc.jsonrpc.version";
  var TMP_RPC_JSONRPC_REQUEST_ID = "rpc.jsonrpc.request_id";
  var TMP_RPC_JSONRPC_ERROR_CODE = "rpc.jsonrpc.error_code";
  var TMP_RPC_JSONRPC_ERROR_MESSAGE = "rpc.jsonrpc.error_message";
  var TMP_MESSAGE_TYPE = "message.type";
  var TMP_MESSAGE_ID = "message.id";
  var TMP_MESSAGE_COMPRESSED_SIZE = "message.compressed_size";
  var TMP_MESSAGE_UNCOMPRESSED_SIZE = "message.uncompressed_size";
  exports.SEMATTRS_AWS_LAMBDA_INVOKED_ARN = TMP_AWS_LAMBDA_INVOKED_ARN;
  exports.SEMATTRS_DB_SYSTEM = TMP_DB_SYSTEM;
  exports.SEMATTRS_DB_CONNECTION_STRING = TMP_DB_CONNECTION_STRING;
  exports.SEMATTRS_DB_USER = TMP_DB_USER;
  exports.SEMATTRS_DB_JDBC_DRIVER_CLASSNAME = TMP_DB_JDBC_DRIVER_CLASSNAME;
  exports.SEMATTRS_DB_NAME = TMP_DB_NAME;
  exports.SEMATTRS_DB_STATEMENT = TMP_DB_STATEMENT;
  exports.SEMATTRS_DB_OPERATION = TMP_DB_OPERATION;
  exports.SEMATTRS_DB_MSSQL_INSTANCE_NAME = TMP_DB_MSSQL_INSTANCE_NAME;
  exports.SEMATTRS_DB_CASSANDRA_KEYSPACE = TMP_DB_CASSANDRA_KEYSPACE;
  exports.SEMATTRS_DB_CASSANDRA_PAGE_SIZE = TMP_DB_CASSANDRA_PAGE_SIZE;
  exports.SEMATTRS_DB_CASSANDRA_CONSISTENCY_LEVEL = TMP_DB_CASSANDRA_CONSISTENCY_LEVEL;
  exports.SEMATTRS_DB_CASSANDRA_TABLE = TMP_DB_CASSANDRA_TABLE;
  exports.SEMATTRS_DB_CASSANDRA_IDEMPOTENCE = TMP_DB_CASSANDRA_IDEMPOTENCE;
  exports.SEMATTRS_DB_CASSANDRA_SPECULATIVE_EXECUTION_COUNT = TMP_DB_CASSANDRA_SPECULATIVE_EXECUTION_COUNT;
  exports.SEMATTRS_DB_CASSANDRA_COORDINATOR_ID = TMP_DB_CASSANDRA_COORDINATOR_ID;
  exports.SEMATTRS_DB_CASSANDRA_COORDINATOR_DC = TMP_DB_CASSANDRA_COORDINATOR_DC;
  exports.SEMATTRS_DB_HBASE_NAMESPACE = TMP_DB_HBASE_NAMESPACE;
  exports.SEMATTRS_DB_REDIS_DATABASE_INDEX = TMP_DB_REDIS_DATABASE_INDEX;
  exports.SEMATTRS_DB_MONGODB_COLLECTION = TMP_DB_MONGODB_COLLECTION;
  exports.SEMATTRS_DB_SQL_TABLE = TMP_DB_SQL_TABLE;
  exports.SEMATTRS_EXCEPTION_TYPE = TMP_EXCEPTION_TYPE;
  exports.SEMATTRS_EXCEPTION_MESSAGE = TMP_EXCEPTION_MESSAGE;
  exports.SEMATTRS_EXCEPTION_STACKTRACE = TMP_EXCEPTION_STACKTRACE;
  exports.SEMATTRS_EXCEPTION_ESCAPED = TMP_EXCEPTION_ESCAPED;
  exports.SEMATTRS_FAAS_TRIGGER = TMP_FAAS_TRIGGER;
  exports.SEMATTRS_FAAS_EXECUTION = TMP_FAAS_EXECUTION;
  exports.SEMATTRS_FAAS_DOCUMENT_COLLECTION = TMP_FAAS_DOCUMENT_COLLECTION;
  exports.SEMATTRS_FAAS_DOCUMENT_OPERATION = TMP_FAAS_DOCUMENT_OPERATION;
  exports.SEMATTRS_FAAS_DOCUMENT_TIME = TMP_FAAS_DOCUMENT_TIME;
  exports.SEMATTRS_FAAS_DOCUMENT_NAME = TMP_FAAS_DOCUMENT_NAME;
  exports.SEMATTRS_FAAS_TIME = TMP_FAAS_TIME;
  exports.SEMATTRS_FAAS_CRON = TMP_FAAS_CRON;
  exports.SEMATTRS_FAAS_COLDSTART = TMP_FAAS_COLDSTART;
  exports.SEMATTRS_FAAS_INVOKED_NAME = TMP_FAAS_INVOKED_NAME;
  exports.SEMATTRS_FAAS_INVOKED_PROVIDER = TMP_FAAS_INVOKED_PROVIDER;
  exports.SEMATTRS_FAAS_INVOKED_REGION = TMP_FAAS_INVOKED_REGION;
  exports.SEMATTRS_NET_TRANSPORT = TMP_NET_TRANSPORT;
  exports.SEMATTRS_NET_PEER_IP = TMP_NET_PEER_IP;
  exports.SEMATTRS_NET_PEER_PORT = TMP_NET_PEER_PORT;
  exports.SEMATTRS_NET_PEER_NAME = TMP_NET_PEER_NAME;
  exports.SEMATTRS_NET_HOST_IP = TMP_NET_HOST_IP;
  exports.SEMATTRS_NET_HOST_PORT = TMP_NET_HOST_PORT;
  exports.SEMATTRS_NET_HOST_NAME = TMP_NET_HOST_NAME;
  exports.SEMATTRS_NET_HOST_CONNECTION_TYPE = TMP_NET_HOST_CONNECTION_TYPE;
  exports.SEMATTRS_NET_HOST_CONNECTION_SUBTYPE = TMP_NET_HOST_CONNECTION_SUBTYPE;
  exports.SEMATTRS_NET_HOST_CARRIER_NAME = TMP_NET_HOST_CARRIER_NAME;
  exports.SEMATTRS_NET_HOST_CARRIER_MCC = TMP_NET_HOST_CARRIER_MCC;
  exports.SEMATTRS_NET_HOST_CARRIER_MNC = TMP_NET_HOST_CARRIER_MNC;
  exports.SEMATTRS_NET_HOST_CARRIER_ICC = TMP_NET_HOST_CARRIER_ICC;
  exports.SEMATTRS_PEER_SERVICE = TMP_PEER_SERVICE;
  exports.SEMATTRS_ENDUSER_ID = TMP_ENDUSER_ID;
  exports.SEMATTRS_ENDUSER_ROLE = TMP_ENDUSER_ROLE;
  exports.SEMATTRS_ENDUSER_SCOPE = TMP_ENDUSER_SCOPE;
  exports.SEMATTRS_THREAD_ID = TMP_THREAD_ID;
  exports.SEMATTRS_THREAD_NAME = TMP_THREAD_NAME;
  exports.SEMATTRS_CODE_FUNCTION = TMP_CODE_FUNCTION;
  exports.SEMATTRS_CODE_NAMESPACE = TMP_CODE_NAMESPACE;
  exports.SEMATTRS_CODE_FILEPATH = TMP_CODE_FILEPATH;
  exports.SEMATTRS_CODE_LINENO = TMP_CODE_LINENO;
  exports.SEMATTRS_HTTP_METHOD = TMP_HTTP_METHOD;
  exports.SEMATTRS_HTTP_URL = TMP_HTTP_URL;
  exports.SEMATTRS_HTTP_TARGET = TMP_HTTP_TARGET;
  exports.SEMATTRS_HTTP_HOST = TMP_HTTP_HOST;
  exports.SEMATTRS_HTTP_SCHEME = TMP_HTTP_SCHEME;
  exports.SEMATTRS_HTTP_STATUS_CODE = TMP_HTTP_STATUS_CODE;
  exports.SEMATTRS_HTTP_FLAVOR = TMP_HTTP_FLAVOR;
  exports.SEMATTRS_HTTP_USER_AGENT = TMP_HTTP_USER_AGENT;
  exports.SEMATTRS_HTTP_REQUEST_CONTENT_LENGTH = TMP_HTTP_REQUEST_CONTENT_LENGTH;
  exports.SEMATTRS_HTTP_REQUEST_CONTENT_LENGTH_UNCOMPRESSED = TMP_HTTP_REQUEST_CONTENT_LENGTH_UNCOMPRESSED;
  exports.SEMATTRS_HTTP_RESPONSE_CONTENT_LENGTH = TMP_HTTP_RESPONSE_CONTENT_LENGTH;
  exports.SEMATTRS_HTTP_RESPONSE_CONTENT_LENGTH_UNCOMPRESSED = TMP_HTTP_RESPONSE_CONTENT_LENGTH_UNCOMPRESSED;
  exports.SEMATTRS_HTTP_SERVER_NAME = TMP_HTTP_SERVER_NAME;
  exports.SEMATTRS_HTTP_ROUTE = TMP_HTTP_ROUTE;
  exports.SEMATTRS_HTTP_CLIENT_IP = TMP_HTTP_CLIENT_IP;
  exports.SEMATTRS_AWS_DYNAMODB_TABLE_NAMES = TMP_AWS_DYNAMODB_TABLE_NAMES;
  exports.SEMATTRS_AWS_DYNAMODB_CONSUMED_CAPACITY = TMP_AWS_DYNAMODB_CONSUMED_CAPACITY;
  exports.SEMATTRS_AWS_DYNAMODB_ITEM_COLLECTION_METRICS = TMP_AWS_DYNAMODB_ITEM_COLLECTION_METRICS;
  exports.SEMATTRS_AWS_DYNAMODB_PROVISIONED_READ_CAPACITY = TMP_AWS_DYNAMODB_PROVISIONED_READ_CAPACITY;
  exports.SEMATTRS_AWS_DYNAMODB_PROVISIONED_WRITE_CAPACITY = TMP_AWS_DYNAMODB_PROVISIONED_WRITE_CAPACITY;
  exports.SEMATTRS_AWS_DYNAMODB_CONSISTENT_READ = TMP_AWS_DYNAMODB_CONSISTENT_READ;
  exports.SEMATTRS_AWS_DYNAMODB_PROJECTION = TMP_AWS_DYNAMODB_PROJECTION;
  exports.SEMATTRS_AWS_DYNAMODB_LIMIT = TMP_AWS_DYNAMODB_LIMIT;
  exports.SEMATTRS_AWS_DYNAMODB_ATTRIBUTES_TO_GET = TMP_AWS_DYNAMODB_ATTRIBUTES_TO_GET;
  exports.SEMATTRS_AWS_DYNAMODB_INDEX_NAME = TMP_AWS_DYNAMODB_INDEX_NAME;
  exports.SEMATTRS_AWS_DYNAMODB_SELECT = TMP_AWS_DYNAMODB_SELECT;
  exports.SEMATTRS_AWS_DYNAMODB_GLOBAL_SECONDARY_INDEXES = TMP_AWS_DYNAMODB_GLOBAL_SECONDARY_INDEXES;
  exports.SEMATTRS_AWS_DYNAMODB_LOCAL_SECONDARY_INDEXES = TMP_AWS_DYNAMODB_LOCAL_SECONDARY_INDEXES;
  exports.SEMATTRS_AWS_DYNAMODB_EXCLUSIVE_START_TABLE = TMP_AWS_DYNAMODB_EXCLUSIVE_START_TABLE;
  exports.SEMATTRS_AWS_DYNAMODB_TABLE_COUNT = TMP_AWS_DYNAMODB_TABLE_COUNT;
  exports.SEMATTRS_AWS_DYNAMODB_SCAN_FORWARD = TMP_AWS_DYNAMODB_SCAN_FORWARD;
  exports.SEMATTRS_AWS_DYNAMODB_SEGMENT = TMP_AWS_DYNAMODB_SEGMENT;
  exports.SEMATTRS_AWS_DYNAMODB_TOTAL_SEGMENTS = TMP_AWS_DYNAMODB_TOTAL_SEGMENTS;
  exports.SEMATTRS_AWS_DYNAMODB_COUNT = TMP_AWS_DYNAMODB_COUNT;
  exports.SEMATTRS_AWS_DYNAMODB_SCANNED_COUNT = TMP_AWS_DYNAMODB_SCANNED_COUNT;
  exports.SEMATTRS_AWS_DYNAMODB_ATTRIBUTE_DEFINITIONS = TMP_AWS_DYNAMODB_ATTRIBUTE_DEFINITIONS;
  exports.SEMATTRS_AWS_DYNAMODB_GLOBAL_SECONDARY_INDEX_UPDATES = TMP_AWS_DYNAMODB_GLOBAL_SECONDARY_INDEX_UPDATES;
  exports.SEMATTRS_MESSAGING_SYSTEM = TMP_MESSAGING_SYSTEM;
  exports.SEMATTRS_MESSAGING_DESTINATION = TMP_MESSAGING_DESTINATION;
  exports.SEMATTRS_MESSAGING_DESTINATION_KIND = TMP_MESSAGING_DESTINATION_KIND;
  exports.SEMATTRS_MESSAGING_TEMP_DESTINATION = TMP_MESSAGING_TEMP_DESTINATION;
  exports.SEMATTRS_MESSAGING_PROTOCOL = TMP_MESSAGING_PROTOCOL;
  exports.SEMATTRS_MESSAGING_PROTOCOL_VERSION = TMP_MESSAGING_PROTOCOL_VERSION;
  exports.SEMATTRS_MESSAGING_URL = TMP_MESSAGING_URL;
  exports.SEMATTRS_MESSAGING_MESSAGE_ID = TMP_MESSAGING_MESSAGE_ID;
  exports.SEMATTRS_MESSAGING_CONVERSATION_ID = TMP_MESSAGING_CONVERSATION_ID;
  exports.SEMATTRS_MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES = TMP_MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES;
  exports.SEMATTRS_MESSAGING_MESSAGE_PAYLOAD_COMPRESSED_SIZE_BYTES = TMP_MESSAGING_MESSAGE_PAYLOAD_COMPRESSED_SIZE_BYTES;
  exports.SEMATTRS_MESSAGING_OPERATION = TMP_MESSAGING_OPERATION;
  exports.SEMATTRS_MESSAGING_CONSUMER_ID = TMP_MESSAGING_CONSUMER_ID;
  exports.SEMATTRS_MESSAGING_RABBITMQ_ROUTING_KEY = TMP_MESSAGING_RABBITMQ_ROUTING_KEY;
  exports.SEMATTRS_MESSAGING_KAFKA_MESSAGE_KEY = TMP_MESSAGING_KAFKA_MESSAGE_KEY;
  exports.SEMATTRS_MESSAGING_KAFKA_CONSUMER_GROUP = TMP_MESSAGING_KAFKA_CONSUMER_GROUP;
  exports.SEMATTRS_MESSAGING_KAFKA_CLIENT_ID = TMP_MESSAGING_KAFKA_CLIENT_ID;
  exports.SEMATTRS_MESSAGING_KAFKA_PARTITION = TMP_MESSAGING_KAFKA_PARTITION;
  exports.SEMATTRS_MESSAGING_KAFKA_TOMBSTONE = TMP_MESSAGING_KAFKA_TOMBSTONE;
  exports.SEMATTRS_RPC_SYSTEM = TMP_RPC_SYSTEM;
  exports.SEMATTRS_RPC_SERVICE = TMP_RPC_SERVICE;
  exports.SEMATTRS_RPC_METHOD = TMP_RPC_METHOD;
  exports.SEMATTRS_RPC_GRPC_STATUS_CODE = TMP_RPC_GRPC_STATUS_CODE;
  exports.SEMATTRS_RPC_JSONRPC_VERSION = TMP_RPC_JSONRPC_VERSION;
  exports.SEMATTRS_RPC_JSONRPC_REQUEST_ID = TMP_RPC_JSONRPC_REQUEST_ID;
  exports.SEMATTRS_RPC_JSONRPC_ERROR_CODE = TMP_RPC_JSONRPC_ERROR_CODE;
  exports.SEMATTRS_RPC_JSONRPC_ERROR_MESSAGE = TMP_RPC_JSONRPC_ERROR_MESSAGE;
  exports.SEMATTRS_MESSAGE_TYPE = TMP_MESSAGE_TYPE;
  exports.SEMATTRS_MESSAGE_ID = TMP_MESSAGE_ID;
  exports.SEMATTRS_MESSAGE_COMPRESSED_SIZE = TMP_MESSAGE_COMPRESSED_SIZE;
  exports.SEMATTRS_MESSAGE_UNCOMPRESSED_SIZE = TMP_MESSAGE_UNCOMPRESSED_SIZE;
  exports.SemanticAttributes = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_AWS_LAMBDA_INVOKED_ARN,
    TMP_DB_SYSTEM,
    TMP_DB_CONNECTION_STRING,
    TMP_DB_USER,
    TMP_DB_JDBC_DRIVER_CLASSNAME,
    TMP_DB_NAME,
    TMP_DB_STATEMENT,
    TMP_DB_OPERATION,
    TMP_DB_MSSQL_INSTANCE_NAME,
    TMP_DB_CASSANDRA_KEYSPACE,
    TMP_DB_CASSANDRA_PAGE_SIZE,
    TMP_DB_CASSANDRA_CONSISTENCY_LEVEL,
    TMP_DB_CASSANDRA_TABLE,
    TMP_DB_CASSANDRA_IDEMPOTENCE,
    TMP_DB_CASSANDRA_SPECULATIVE_EXECUTION_COUNT,
    TMP_DB_CASSANDRA_COORDINATOR_ID,
    TMP_DB_CASSANDRA_COORDINATOR_DC,
    TMP_DB_HBASE_NAMESPACE,
    TMP_DB_REDIS_DATABASE_INDEX,
    TMP_DB_MONGODB_COLLECTION,
    TMP_DB_SQL_TABLE,
    TMP_EXCEPTION_TYPE,
    TMP_EXCEPTION_MESSAGE,
    TMP_EXCEPTION_STACKTRACE,
    TMP_EXCEPTION_ESCAPED,
    TMP_FAAS_TRIGGER,
    TMP_FAAS_EXECUTION,
    TMP_FAAS_DOCUMENT_COLLECTION,
    TMP_FAAS_DOCUMENT_OPERATION,
    TMP_FAAS_DOCUMENT_TIME,
    TMP_FAAS_DOCUMENT_NAME,
    TMP_FAAS_TIME,
    TMP_FAAS_CRON,
    TMP_FAAS_COLDSTART,
    TMP_FAAS_INVOKED_NAME,
    TMP_FAAS_INVOKED_PROVIDER,
    TMP_FAAS_INVOKED_REGION,
    TMP_NET_TRANSPORT,
    TMP_NET_PEER_IP,
    TMP_NET_PEER_PORT,
    TMP_NET_PEER_NAME,
    TMP_NET_HOST_IP,
    TMP_NET_HOST_PORT,
    TMP_NET_HOST_NAME,
    TMP_NET_HOST_CONNECTION_TYPE,
    TMP_NET_HOST_CONNECTION_SUBTYPE,
    TMP_NET_HOST_CARRIER_NAME,
    TMP_NET_HOST_CARRIER_MCC,
    TMP_NET_HOST_CARRIER_MNC,
    TMP_NET_HOST_CARRIER_ICC,
    TMP_PEER_SERVICE,
    TMP_ENDUSER_ID,
    TMP_ENDUSER_ROLE,
    TMP_ENDUSER_SCOPE,
    TMP_THREAD_ID,
    TMP_THREAD_NAME,
    TMP_CODE_FUNCTION,
    TMP_CODE_NAMESPACE,
    TMP_CODE_FILEPATH,
    TMP_CODE_LINENO,
    TMP_HTTP_METHOD,
    TMP_HTTP_URL,
    TMP_HTTP_TARGET,
    TMP_HTTP_HOST,
    TMP_HTTP_SCHEME,
    TMP_HTTP_STATUS_CODE,
    TMP_HTTP_FLAVOR,
    TMP_HTTP_USER_AGENT,
    TMP_HTTP_REQUEST_CONTENT_LENGTH,
    TMP_HTTP_REQUEST_CONTENT_LENGTH_UNCOMPRESSED,
    TMP_HTTP_RESPONSE_CONTENT_LENGTH,
    TMP_HTTP_RESPONSE_CONTENT_LENGTH_UNCOMPRESSED,
    TMP_HTTP_SERVER_NAME,
    TMP_HTTP_ROUTE,
    TMP_HTTP_CLIENT_IP,
    TMP_AWS_DYNAMODB_TABLE_NAMES,
    TMP_AWS_DYNAMODB_CONSUMED_CAPACITY,
    TMP_AWS_DYNAMODB_ITEM_COLLECTION_METRICS,
    TMP_AWS_DYNAMODB_PROVISIONED_READ_CAPACITY,
    TMP_AWS_DYNAMODB_PROVISIONED_WRITE_CAPACITY,
    TMP_AWS_DYNAMODB_CONSISTENT_READ,
    TMP_AWS_DYNAMODB_PROJECTION,
    TMP_AWS_DYNAMODB_LIMIT,
    TMP_AWS_DYNAMODB_ATTRIBUTES_TO_GET,
    TMP_AWS_DYNAMODB_INDEX_NAME,
    TMP_AWS_DYNAMODB_SELECT,
    TMP_AWS_DYNAMODB_GLOBAL_SECONDARY_INDEXES,
    TMP_AWS_DYNAMODB_LOCAL_SECONDARY_INDEXES,
    TMP_AWS_DYNAMODB_EXCLUSIVE_START_TABLE,
    TMP_AWS_DYNAMODB_TABLE_COUNT,
    TMP_AWS_DYNAMODB_SCAN_FORWARD,
    TMP_AWS_DYNAMODB_SEGMENT,
    TMP_AWS_DYNAMODB_TOTAL_SEGMENTS,
    TMP_AWS_DYNAMODB_COUNT,
    TMP_AWS_DYNAMODB_SCANNED_COUNT,
    TMP_AWS_DYNAMODB_ATTRIBUTE_DEFINITIONS,
    TMP_AWS_DYNAMODB_GLOBAL_SECONDARY_INDEX_UPDATES,
    TMP_MESSAGING_SYSTEM,
    TMP_MESSAGING_DESTINATION,
    TMP_MESSAGING_DESTINATION_KIND,
    TMP_MESSAGING_TEMP_DESTINATION,
    TMP_MESSAGING_PROTOCOL,
    TMP_MESSAGING_PROTOCOL_VERSION,
    TMP_MESSAGING_URL,
    TMP_MESSAGING_MESSAGE_ID,
    TMP_MESSAGING_CONVERSATION_ID,
    TMP_MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES,
    TMP_MESSAGING_MESSAGE_PAYLOAD_COMPRESSED_SIZE_BYTES,
    TMP_MESSAGING_OPERATION,
    TMP_MESSAGING_CONSUMER_ID,
    TMP_MESSAGING_RABBITMQ_ROUTING_KEY,
    TMP_MESSAGING_KAFKA_MESSAGE_KEY,
    TMP_MESSAGING_KAFKA_CONSUMER_GROUP,
    TMP_MESSAGING_KAFKA_CLIENT_ID,
    TMP_MESSAGING_KAFKA_PARTITION,
    TMP_MESSAGING_KAFKA_TOMBSTONE,
    TMP_RPC_SYSTEM,
    TMP_RPC_SERVICE,
    TMP_RPC_METHOD,
    TMP_RPC_GRPC_STATUS_CODE,
    TMP_RPC_JSONRPC_VERSION,
    TMP_RPC_JSONRPC_REQUEST_ID,
    TMP_RPC_JSONRPC_ERROR_CODE,
    TMP_RPC_JSONRPC_ERROR_MESSAGE,
    TMP_MESSAGE_TYPE,
    TMP_MESSAGE_ID,
    TMP_MESSAGE_COMPRESSED_SIZE,
    TMP_MESSAGE_UNCOMPRESSED_SIZE
  ]);
  var TMP_DBSYSTEMVALUES_OTHER_SQL = "other_sql";
  var TMP_DBSYSTEMVALUES_MSSQL = "mssql";
  var TMP_DBSYSTEMVALUES_MYSQL = "mysql";
  var TMP_DBSYSTEMVALUES_ORACLE = "oracle";
  var TMP_DBSYSTEMVALUES_DB2 = "db2";
  var TMP_DBSYSTEMVALUES_POSTGRESQL = "postgresql";
  var TMP_DBSYSTEMVALUES_REDSHIFT = "redshift";
  var TMP_DBSYSTEMVALUES_HIVE = "hive";
  var TMP_DBSYSTEMVALUES_CLOUDSCAPE = "cloudscape";
  var TMP_DBSYSTEMVALUES_HSQLDB = "hsqldb";
  var TMP_DBSYSTEMVALUES_PROGRESS = "progress";
  var TMP_DBSYSTEMVALUES_MAXDB = "maxdb";
  var TMP_DBSYSTEMVALUES_HANADB = "hanadb";
  var TMP_DBSYSTEMVALUES_INGRES = "ingres";
  var TMP_DBSYSTEMVALUES_FIRSTSQL = "firstsql";
  var TMP_DBSYSTEMVALUES_EDB = "edb";
  var TMP_DBSYSTEMVALUES_CACHE = "cache";
  var TMP_DBSYSTEMVALUES_ADABAS = "adabas";
  var TMP_DBSYSTEMVALUES_FIREBIRD = "firebird";
  var TMP_DBSYSTEMVALUES_DERBY = "derby";
  var TMP_DBSYSTEMVALUES_FILEMAKER = "filemaker";
  var TMP_DBSYSTEMVALUES_INFORMIX = "informix";
  var TMP_DBSYSTEMVALUES_INSTANTDB = "instantdb";
  var TMP_DBSYSTEMVALUES_INTERBASE = "interbase";
  var TMP_DBSYSTEMVALUES_MARIADB = "mariadb";
  var TMP_DBSYSTEMVALUES_NETEZZA = "netezza";
  var TMP_DBSYSTEMVALUES_PERVASIVE = "pervasive";
  var TMP_DBSYSTEMVALUES_POINTBASE = "pointbase";
  var TMP_DBSYSTEMVALUES_SQLITE = "sqlite";
  var TMP_DBSYSTEMVALUES_SYBASE = "sybase";
  var TMP_DBSYSTEMVALUES_TERADATA = "teradata";
  var TMP_DBSYSTEMVALUES_VERTICA = "vertica";
  var TMP_DBSYSTEMVALUES_H2 = "h2";
  var TMP_DBSYSTEMVALUES_COLDFUSION = "coldfusion";
  var TMP_DBSYSTEMVALUES_CASSANDRA = "cassandra";
  var TMP_DBSYSTEMVALUES_HBASE = "hbase";
  var TMP_DBSYSTEMVALUES_MONGODB = "mongodb";
  var TMP_DBSYSTEMVALUES_REDIS = "redis";
  var TMP_DBSYSTEMVALUES_COUCHBASE = "couchbase";
  var TMP_DBSYSTEMVALUES_COUCHDB = "couchdb";
  var TMP_DBSYSTEMVALUES_COSMOSDB = "cosmosdb";
  var TMP_DBSYSTEMVALUES_DYNAMODB = "dynamodb";
  var TMP_DBSYSTEMVALUES_NEO4J = "neo4j";
  var TMP_DBSYSTEMVALUES_GEODE = "geode";
  var TMP_DBSYSTEMVALUES_ELASTICSEARCH = "elasticsearch";
  var TMP_DBSYSTEMVALUES_MEMCACHED = "memcached";
  var TMP_DBSYSTEMVALUES_COCKROACHDB = "cockroachdb";
  exports.DBSYSTEMVALUES_OTHER_SQL = TMP_DBSYSTEMVALUES_OTHER_SQL;
  exports.DBSYSTEMVALUES_MSSQL = TMP_DBSYSTEMVALUES_MSSQL;
  exports.DBSYSTEMVALUES_MYSQL = TMP_DBSYSTEMVALUES_MYSQL;
  exports.DBSYSTEMVALUES_ORACLE = TMP_DBSYSTEMVALUES_ORACLE;
  exports.DBSYSTEMVALUES_DB2 = TMP_DBSYSTEMVALUES_DB2;
  exports.DBSYSTEMVALUES_POSTGRESQL = TMP_DBSYSTEMVALUES_POSTGRESQL;
  exports.DBSYSTEMVALUES_REDSHIFT = TMP_DBSYSTEMVALUES_REDSHIFT;
  exports.DBSYSTEMVALUES_HIVE = TMP_DBSYSTEMVALUES_HIVE;
  exports.DBSYSTEMVALUES_CLOUDSCAPE = TMP_DBSYSTEMVALUES_CLOUDSCAPE;
  exports.DBSYSTEMVALUES_HSQLDB = TMP_DBSYSTEMVALUES_HSQLDB;
  exports.DBSYSTEMVALUES_PROGRESS = TMP_DBSYSTEMVALUES_PROGRESS;
  exports.DBSYSTEMVALUES_MAXDB = TMP_DBSYSTEMVALUES_MAXDB;
  exports.DBSYSTEMVALUES_HANADB = TMP_DBSYSTEMVALUES_HANADB;
  exports.DBSYSTEMVALUES_INGRES = TMP_DBSYSTEMVALUES_INGRES;
  exports.DBSYSTEMVALUES_FIRSTSQL = TMP_DBSYSTEMVALUES_FIRSTSQL;
  exports.DBSYSTEMVALUES_EDB = TMP_DBSYSTEMVALUES_EDB;
  exports.DBSYSTEMVALUES_CACHE = TMP_DBSYSTEMVALUES_CACHE;
  exports.DBSYSTEMVALUES_ADABAS = TMP_DBSYSTEMVALUES_ADABAS;
  exports.DBSYSTEMVALUES_FIREBIRD = TMP_DBSYSTEMVALUES_FIREBIRD;
  exports.DBSYSTEMVALUES_DERBY = TMP_DBSYSTEMVALUES_DERBY;
  exports.DBSYSTEMVALUES_FILEMAKER = TMP_DBSYSTEMVALUES_FILEMAKER;
  exports.DBSYSTEMVALUES_INFORMIX = TMP_DBSYSTEMVALUES_INFORMIX;
  exports.DBSYSTEMVALUES_INSTANTDB = TMP_DBSYSTEMVALUES_INSTANTDB;
  exports.DBSYSTEMVALUES_INTERBASE = TMP_DBSYSTEMVALUES_INTERBASE;
  exports.DBSYSTEMVALUES_MARIADB = TMP_DBSYSTEMVALUES_MARIADB;
  exports.DBSYSTEMVALUES_NETEZZA = TMP_DBSYSTEMVALUES_NETEZZA;
  exports.DBSYSTEMVALUES_PERVASIVE = TMP_DBSYSTEMVALUES_PERVASIVE;
  exports.DBSYSTEMVALUES_POINTBASE = TMP_DBSYSTEMVALUES_POINTBASE;
  exports.DBSYSTEMVALUES_SQLITE = TMP_DBSYSTEMVALUES_SQLITE;
  exports.DBSYSTEMVALUES_SYBASE = TMP_DBSYSTEMVALUES_SYBASE;
  exports.DBSYSTEMVALUES_TERADATA = TMP_DBSYSTEMVALUES_TERADATA;
  exports.DBSYSTEMVALUES_VERTICA = TMP_DBSYSTEMVALUES_VERTICA;
  exports.DBSYSTEMVALUES_H2 = TMP_DBSYSTEMVALUES_H2;
  exports.DBSYSTEMVALUES_COLDFUSION = TMP_DBSYSTEMVALUES_COLDFUSION;
  exports.DBSYSTEMVALUES_CASSANDRA = TMP_DBSYSTEMVALUES_CASSANDRA;
  exports.DBSYSTEMVALUES_HBASE = TMP_DBSYSTEMVALUES_HBASE;
  exports.DBSYSTEMVALUES_MONGODB = TMP_DBSYSTEMVALUES_MONGODB;
  exports.DBSYSTEMVALUES_REDIS = TMP_DBSYSTEMVALUES_REDIS;
  exports.DBSYSTEMVALUES_COUCHBASE = TMP_DBSYSTEMVALUES_COUCHBASE;
  exports.DBSYSTEMVALUES_COUCHDB = TMP_DBSYSTEMVALUES_COUCHDB;
  exports.DBSYSTEMVALUES_COSMOSDB = TMP_DBSYSTEMVALUES_COSMOSDB;
  exports.DBSYSTEMVALUES_DYNAMODB = TMP_DBSYSTEMVALUES_DYNAMODB;
  exports.DBSYSTEMVALUES_NEO4J = TMP_DBSYSTEMVALUES_NEO4J;
  exports.DBSYSTEMVALUES_GEODE = TMP_DBSYSTEMVALUES_GEODE;
  exports.DBSYSTEMVALUES_ELASTICSEARCH = TMP_DBSYSTEMVALUES_ELASTICSEARCH;
  exports.DBSYSTEMVALUES_MEMCACHED = TMP_DBSYSTEMVALUES_MEMCACHED;
  exports.DBSYSTEMVALUES_COCKROACHDB = TMP_DBSYSTEMVALUES_COCKROACHDB;
  exports.DbSystemValues = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_DBSYSTEMVALUES_OTHER_SQL,
    TMP_DBSYSTEMVALUES_MSSQL,
    TMP_DBSYSTEMVALUES_MYSQL,
    TMP_DBSYSTEMVALUES_ORACLE,
    TMP_DBSYSTEMVALUES_DB2,
    TMP_DBSYSTEMVALUES_POSTGRESQL,
    TMP_DBSYSTEMVALUES_REDSHIFT,
    TMP_DBSYSTEMVALUES_HIVE,
    TMP_DBSYSTEMVALUES_CLOUDSCAPE,
    TMP_DBSYSTEMVALUES_HSQLDB,
    TMP_DBSYSTEMVALUES_PROGRESS,
    TMP_DBSYSTEMVALUES_MAXDB,
    TMP_DBSYSTEMVALUES_HANADB,
    TMP_DBSYSTEMVALUES_INGRES,
    TMP_DBSYSTEMVALUES_FIRSTSQL,
    TMP_DBSYSTEMVALUES_EDB,
    TMP_DBSYSTEMVALUES_CACHE,
    TMP_DBSYSTEMVALUES_ADABAS,
    TMP_DBSYSTEMVALUES_FIREBIRD,
    TMP_DBSYSTEMVALUES_DERBY,
    TMP_DBSYSTEMVALUES_FILEMAKER,
    TMP_DBSYSTEMVALUES_INFORMIX,
    TMP_DBSYSTEMVALUES_INSTANTDB,
    TMP_DBSYSTEMVALUES_INTERBASE,
    TMP_DBSYSTEMVALUES_MARIADB,
    TMP_DBSYSTEMVALUES_NETEZZA,
    TMP_DBSYSTEMVALUES_PERVASIVE,
    TMP_DBSYSTEMVALUES_POINTBASE,
    TMP_DBSYSTEMVALUES_SQLITE,
    TMP_DBSYSTEMVALUES_SYBASE,
    TMP_DBSYSTEMVALUES_TERADATA,
    TMP_DBSYSTEMVALUES_VERTICA,
    TMP_DBSYSTEMVALUES_H2,
    TMP_DBSYSTEMVALUES_COLDFUSION,
    TMP_DBSYSTEMVALUES_CASSANDRA,
    TMP_DBSYSTEMVALUES_HBASE,
    TMP_DBSYSTEMVALUES_MONGODB,
    TMP_DBSYSTEMVALUES_REDIS,
    TMP_DBSYSTEMVALUES_COUCHBASE,
    TMP_DBSYSTEMVALUES_COUCHDB,
    TMP_DBSYSTEMVALUES_COSMOSDB,
    TMP_DBSYSTEMVALUES_DYNAMODB,
    TMP_DBSYSTEMVALUES_NEO4J,
    TMP_DBSYSTEMVALUES_GEODE,
    TMP_DBSYSTEMVALUES_ELASTICSEARCH,
    TMP_DBSYSTEMVALUES_MEMCACHED,
    TMP_DBSYSTEMVALUES_COCKROACHDB
  ]);
  var TMP_DBCASSANDRACONSISTENCYLEVELVALUES_ALL = "all";
  var TMP_DBCASSANDRACONSISTENCYLEVELVALUES_EACH_QUORUM = "each_quorum";
  var TMP_DBCASSANDRACONSISTENCYLEVELVALUES_QUORUM = "quorum";
  var TMP_DBCASSANDRACONSISTENCYLEVELVALUES_LOCAL_QUORUM = "local_quorum";
  var TMP_DBCASSANDRACONSISTENCYLEVELVALUES_ONE = "one";
  var TMP_DBCASSANDRACONSISTENCYLEVELVALUES_TWO = "two";
  var TMP_DBCASSANDRACONSISTENCYLEVELVALUES_THREE = "three";
  var TMP_DBCASSANDRACONSISTENCYLEVELVALUES_LOCAL_ONE = "local_one";
  var TMP_DBCASSANDRACONSISTENCYLEVELVALUES_ANY = "any";
  var TMP_DBCASSANDRACONSISTENCYLEVELVALUES_SERIAL = "serial";
  var TMP_DBCASSANDRACONSISTENCYLEVELVALUES_LOCAL_SERIAL = "local_serial";
  exports.DBCASSANDRACONSISTENCYLEVELVALUES_ALL = TMP_DBCASSANDRACONSISTENCYLEVELVALUES_ALL;
  exports.DBCASSANDRACONSISTENCYLEVELVALUES_EACH_QUORUM = TMP_DBCASSANDRACONSISTENCYLEVELVALUES_EACH_QUORUM;
  exports.DBCASSANDRACONSISTENCYLEVELVALUES_QUORUM = TMP_DBCASSANDRACONSISTENCYLEVELVALUES_QUORUM;
  exports.DBCASSANDRACONSISTENCYLEVELVALUES_LOCAL_QUORUM = TMP_DBCASSANDRACONSISTENCYLEVELVALUES_LOCAL_QUORUM;
  exports.DBCASSANDRACONSISTENCYLEVELVALUES_ONE = TMP_DBCASSANDRACONSISTENCYLEVELVALUES_ONE;
  exports.DBCASSANDRACONSISTENCYLEVELVALUES_TWO = TMP_DBCASSANDRACONSISTENCYLEVELVALUES_TWO;
  exports.DBCASSANDRACONSISTENCYLEVELVALUES_THREE = TMP_DBCASSANDRACONSISTENCYLEVELVALUES_THREE;
  exports.DBCASSANDRACONSISTENCYLEVELVALUES_LOCAL_ONE = TMP_DBCASSANDRACONSISTENCYLEVELVALUES_LOCAL_ONE;
  exports.DBCASSANDRACONSISTENCYLEVELVALUES_ANY = TMP_DBCASSANDRACONSISTENCYLEVELVALUES_ANY;
  exports.DBCASSANDRACONSISTENCYLEVELVALUES_SERIAL = TMP_DBCASSANDRACONSISTENCYLEVELVALUES_SERIAL;
  exports.DBCASSANDRACONSISTENCYLEVELVALUES_LOCAL_SERIAL = TMP_DBCASSANDRACONSISTENCYLEVELVALUES_LOCAL_SERIAL;
  exports.DbCassandraConsistencyLevelValues = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_DBCASSANDRACONSISTENCYLEVELVALUES_ALL,
    TMP_DBCASSANDRACONSISTENCYLEVELVALUES_EACH_QUORUM,
    TMP_DBCASSANDRACONSISTENCYLEVELVALUES_QUORUM,
    TMP_DBCASSANDRACONSISTENCYLEVELVALUES_LOCAL_QUORUM,
    TMP_DBCASSANDRACONSISTENCYLEVELVALUES_ONE,
    TMP_DBCASSANDRACONSISTENCYLEVELVALUES_TWO,
    TMP_DBCASSANDRACONSISTENCYLEVELVALUES_THREE,
    TMP_DBCASSANDRACONSISTENCYLEVELVALUES_LOCAL_ONE,
    TMP_DBCASSANDRACONSISTENCYLEVELVALUES_ANY,
    TMP_DBCASSANDRACONSISTENCYLEVELVALUES_SERIAL,
    TMP_DBCASSANDRACONSISTENCYLEVELVALUES_LOCAL_SERIAL
  ]);
  var TMP_FAASTRIGGERVALUES_DATASOURCE = "datasource";
  var TMP_FAASTRIGGERVALUES_HTTP = "http";
  var TMP_FAASTRIGGERVALUES_PUBSUB = "pubsub";
  var TMP_FAASTRIGGERVALUES_TIMER = "timer";
  var TMP_FAASTRIGGERVALUES_OTHER = "other";
  exports.FAASTRIGGERVALUES_DATASOURCE = TMP_FAASTRIGGERVALUES_DATASOURCE;
  exports.FAASTRIGGERVALUES_HTTP = TMP_FAASTRIGGERVALUES_HTTP;
  exports.FAASTRIGGERVALUES_PUBSUB = TMP_FAASTRIGGERVALUES_PUBSUB;
  exports.FAASTRIGGERVALUES_TIMER = TMP_FAASTRIGGERVALUES_TIMER;
  exports.FAASTRIGGERVALUES_OTHER = TMP_FAASTRIGGERVALUES_OTHER;
  exports.FaasTriggerValues = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_FAASTRIGGERVALUES_DATASOURCE,
    TMP_FAASTRIGGERVALUES_HTTP,
    TMP_FAASTRIGGERVALUES_PUBSUB,
    TMP_FAASTRIGGERVALUES_TIMER,
    TMP_FAASTRIGGERVALUES_OTHER
  ]);
  var TMP_FAASDOCUMENTOPERATIONVALUES_INSERT = "insert";
  var TMP_FAASDOCUMENTOPERATIONVALUES_EDIT = "edit";
  var TMP_FAASDOCUMENTOPERATIONVALUES_DELETE = "delete";
  exports.FAASDOCUMENTOPERATIONVALUES_INSERT = TMP_FAASDOCUMENTOPERATIONVALUES_INSERT;
  exports.FAASDOCUMENTOPERATIONVALUES_EDIT = TMP_FAASDOCUMENTOPERATIONVALUES_EDIT;
  exports.FAASDOCUMENTOPERATIONVALUES_DELETE = TMP_FAASDOCUMENTOPERATIONVALUES_DELETE;
  exports.FaasDocumentOperationValues = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_FAASDOCUMENTOPERATIONVALUES_INSERT,
    TMP_FAASDOCUMENTOPERATIONVALUES_EDIT,
    TMP_FAASDOCUMENTOPERATIONVALUES_DELETE
  ]);
  var TMP_FAASINVOKEDPROVIDERVALUES_ALIBABA_CLOUD = "alibaba_cloud";
  var TMP_FAASINVOKEDPROVIDERVALUES_AWS = "aws";
  var TMP_FAASINVOKEDPROVIDERVALUES_AZURE = "azure";
  var TMP_FAASINVOKEDPROVIDERVALUES_GCP = "gcp";
  exports.FAASINVOKEDPROVIDERVALUES_ALIBABA_CLOUD = TMP_FAASINVOKEDPROVIDERVALUES_ALIBABA_CLOUD;
  exports.FAASINVOKEDPROVIDERVALUES_AWS = TMP_FAASINVOKEDPROVIDERVALUES_AWS;
  exports.FAASINVOKEDPROVIDERVALUES_AZURE = TMP_FAASINVOKEDPROVIDERVALUES_AZURE;
  exports.FAASINVOKEDPROVIDERVALUES_GCP = TMP_FAASINVOKEDPROVIDERVALUES_GCP;
  exports.FaasInvokedProviderValues = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_FAASINVOKEDPROVIDERVALUES_ALIBABA_CLOUD,
    TMP_FAASINVOKEDPROVIDERVALUES_AWS,
    TMP_FAASINVOKEDPROVIDERVALUES_AZURE,
    TMP_FAASINVOKEDPROVIDERVALUES_GCP
  ]);
  var TMP_NETTRANSPORTVALUES_IP_TCP = "ip_tcp";
  var TMP_NETTRANSPORTVALUES_IP_UDP = "ip_udp";
  var TMP_NETTRANSPORTVALUES_IP = "ip";
  var TMP_NETTRANSPORTVALUES_UNIX = "unix";
  var TMP_NETTRANSPORTVALUES_PIPE = "pipe";
  var TMP_NETTRANSPORTVALUES_INPROC = "inproc";
  var TMP_NETTRANSPORTVALUES_OTHER = "other";
  exports.NETTRANSPORTVALUES_IP_TCP = TMP_NETTRANSPORTVALUES_IP_TCP;
  exports.NETTRANSPORTVALUES_IP_UDP = TMP_NETTRANSPORTVALUES_IP_UDP;
  exports.NETTRANSPORTVALUES_IP = TMP_NETTRANSPORTVALUES_IP;
  exports.NETTRANSPORTVALUES_UNIX = TMP_NETTRANSPORTVALUES_UNIX;
  exports.NETTRANSPORTVALUES_PIPE = TMP_NETTRANSPORTVALUES_PIPE;
  exports.NETTRANSPORTVALUES_INPROC = TMP_NETTRANSPORTVALUES_INPROC;
  exports.NETTRANSPORTVALUES_OTHER = TMP_NETTRANSPORTVALUES_OTHER;
  exports.NetTransportValues = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_NETTRANSPORTVALUES_IP_TCP,
    TMP_NETTRANSPORTVALUES_IP_UDP,
    TMP_NETTRANSPORTVALUES_IP,
    TMP_NETTRANSPORTVALUES_UNIX,
    TMP_NETTRANSPORTVALUES_PIPE,
    TMP_NETTRANSPORTVALUES_INPROC,
    TMP_NETTRANSPORTVALUES_OTHER
  ]);
  var TMP_NETHOSTCONNECTIONTYPEVALUES_WIFI = "wifi";
  var TMP_NETHOSTCONNECTIONTYPEVALUES_WIRED = "wired";
  var TMP_NETHOSTCONNECTIONTYPEVALUES_CELL = "cell";
  var TMP_NETHOSTCONNECTIONTYPEVALUES_UNAVAILABLE = "unavailable";
  var TMP_NETHOSTCONNECTIONTYPEVALUES_UNKNOWN = "unknown";
  exports.NETHOSTCONNECTIONTYPEVALUES_WIFI = TMP_NETHOSTCONNECTIONTYPEVALUES_WIFI;
  exports.NETHOSTCONNECTIONTYPEVALUES_WIRED = TMP_NETHOSTCONNECTIONTYPEVALUES_WIRED;
  exports.NETHOSTCONNECTIONTYPEVALUES_CELL = TMP_NETHOSTCONNECTIONTYPEVALUES_CELL;
  exports.NETHOSTCONNECTIONTYPEVALUES_UNAVAILABLE = TMP_NETHOSTCONNECTIONTYPEVALUES_UNAVAILABLE;
  exports.NETHOSTCONNECTIONTYPEVALUES_UNKNOWN = TMP_NETHOSTCONNECTIONTYPEVALUES_UNKNOWN;
  exports.NetHostConnectionTypeValues = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_NETHOSTCONNECTIONTYPEVALUES_WIFI,
    TMP_NETHOSTCONNECTIONTYPEVALUES_WIRED,
    TMP_NETHOSTCONNECTIONTYPEVALUES_CELL,
    TMP_NETHOSTCONNECTIONTYPEVALUES_UNAVAILABLE,
    TMP_NETHOSTCONNECTIONTYPEVALUES_UNKNOWN
  ]);
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_GPRS = "gprs";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_EDGE = "edge";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_UMTS = "umts";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_CDMA = "cdma";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_EVDO_0 = "evdo_0";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_EVDO_A = "evdo_a";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_CDMA2000_1XRTT = "cdma2000_1xrtt";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_HSDPA = "hsdpa";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_HSUPA = "hsupa";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_HSPA = "hspa";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_IDEN = "iden";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_EVDO_B = "evdo_b";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_LTE = "lte";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_EHRPD = "ehrpd";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_HSPAP = "hspap";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_GSM = "gsm";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_TD_SCDMA = "td_scdma";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_IWLAN = "iwlan";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_NR = "nr";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_NRNSA = "nrnsa";
  var TMP_NETHOSTCONNECTIONSUBTYPEVALUES_LTE_CA = "lte_ca";
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_GPRS = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_GPRS;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_EDGE = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_EDGE;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_UMTS = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_UMTS;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_CDMA = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_CDMA;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_EVDO_0 = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_EVDO_0;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_EVDO_A = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_EVDO_A;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_CDMA2000_1XRTT = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_CDMA2000_1XRTT;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_HSDPA = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_HSDPA;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_HSUPA = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_HSUPA;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_HSPA = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_HSPA;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_IDEN = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_IDEN;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_EVDO_B = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_EVDO_B;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_LTE = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_LTE;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_EHRPD = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_EHRPD;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_HSPAP = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_HSPAP;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_GSM = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_GSM;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_TD_SCDMA = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_TD_SCDMA;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_IWLAN = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_IWLAN;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_NR = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_NR;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_NRNSA = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_NRNSA;
  exports.NETHOSTCONNECTIONSUBTYPEVALUES_LTE_CA = TMP_NETHOSTCONNECTIONSUBTYPEVALUES_LTE_CA;
  exports.NetHostConnectionSubtypeValues = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_GPRS,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_EDGE,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_UMTS,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_CDMA,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_EVDO_0,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_EVDO_A,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_CDMA2000_1XRTT,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_HSDPA,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_HSUPA,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_HSPA,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_IDEN,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_EVDO_B,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_LTE,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_EHRPD,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_HSPAP,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_GSM,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_TD_SCDMA,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_IWLAN,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_NR,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_NRNSA,
    TMP_NETHOSTCONNECTIONSUBTYPEVALUES_LTE_CA
  ]);
  var TMP_HTTPFLAVORVALUES_HTTP_1_0 = "1.0";
  var TMP_HTTPFLAVORVALUES_HTTP_1_1 = "1.1";
  var TMP_HTTPFLAVORVALUES_HTTP_2_0 = "2.0";
  var TMP_HTTPFLAVORVALUES_SPDY = "SPDY";
  var TMP_HTTPFLAVORVALUES_QUIC = "QUIC";
  exports.HTTPFLAVORVALUES_HTTP_1_0 = TMP_HTTPFLAVORVALUES_HTTP_1_0;
  exports.HTTPFLAVORVALUES_HTTP_1_1 = TMP_HTTPFLAVORVALUES_HTTP_1_1;
  exports.HTTPFLAVORVALUES_HTTP_2_0 = TMP_HTTPFLAVORVALUES_HTTP_2_0;
  exports.HTTPFLAVORVALUES_SPDY = TMP_HTTPFLAVORVALUES_SPDY;
  exports.HTTPFLAVORVALUES_QUIC = TMP_HTTPFLAVORVALUES_QUIC;
  exports.HttpFlavorValues = {
    HTTP_1_0: TMP_HTTPFLAVORVALUES_HTTP_1_0,
    HTTP_1_1: TMP_HTTPFLAVORVALUES_HTTP_1_1,
    HTTP_2_0: TMP_HTTPFLAVORVALUES_HTTP_2_0,
    SPDY: TMP_HTTPFLAVORVALUES_SPDY,
    QUIC: TMP_HTTPFLAVORVALUES_QUIC
  };
  var TMP_MESSAGINGDESTINATIONKINDVALUES_QUEUE = "queue";
  var TMP_MESSAGINGDESTINATIONKINDVALUES_TOPIC = "topic";
  exports.MESSAGINGDESTINATIONKINDVALUES_QUEUE = TMP_MESSAGINGDESTINATIONKINDVALUES_QUEUE;
  exports.MESSAGINGDESTINATIONKINDVALUES_TOPIC = TMP_MESSAGINGDESTINATIONKINDVALUES_TOPIC;
  exports.MessagingDestinationKindValues = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_MESSAGINGDESTINATIONKINDVALUES_QUEUE,
    TMP_MESSAGINGDESTINATIONKINDVALUES_TOPIC
  ]);
  var TMP_MESSAGINGOPERATIONVALUES_RECEIVE = "receive";
  var TMP_MESSAGINGOPERATIONVALUES_PROCESS = "process";
  exports.MESSAGINGOPERATIONVALUES_RECEIVE = TMP_MESSAGINGOPERATIONVALUES_RECEIVE;
  exports.MESSAGINGOPERATIONVALUES_PROCESS = TMP_MESSAGINGOPERATIONVALUES_PROCESS;
  exports.MessagingOperationValues = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_MESSAGINGOPERATIONVALUES_RECEIVE,
    TMP_MESSAGINGOPERATIONVALUES_PROCESS
  ]);
  var TMP_RPCGRPCSTATUSCODEVALUES_OK = 0;
  var TMP_RPCGRPCSTATUSCODEVALUES_CANCELLED = 1;
  var TMP_RPCGRPCSTATUSCODEVALUES_UNKNOWN = 2;
  var TMP_RPCGRPCSTATUSCODEVALUES_INVALID_ARGUMENT = 3;
  var TMP_RPCGRPCSTATUSCODEVALUES_DEADLINE_EXCEEDED = 4;
  var TMP_RPCGRPCSTATUSCODEVALUES_NOT_FOUND = 5;
  var TMP_RPCGRPCSTATUSCODEVALUES_ALREADY_EXISTS = 6;
  var TMP_RPCGRPCSTATUSCODEVALUES_PERMISSION_DENIED = 7;
  var TMP_RPCGRPCSTATUSCODEVALUES_RESOURCE_EXHAUSTED = 8;
  var TMP_RPCGRPCSTATUSCODEVALUES_FAILED_PRECONDITION = 9;
  var TMP_RPCGRPCSTATUSCODEVALUES_ABORTED = 10;
  var TMP_RPCGRPCSTATUSCODEVALUES_OUT_OF_RANGE = 11;
  var TMP_RPCGRPCSTATUSCODEVALUES_UNIMPLEMENTED = 12;
  var TMP_RPCGRPCSTATUSCODEVALUES_INTERNAL = 13;
  var TMP_RPCGRPCSTATUSCODEVALUES_UNAVAILABLE = 14;
  var TMP_RPCGRPCSTATUSCODEVALUES_DATA_LOSS = 15;
  var TMP_RPCGRPCSTATUSCODEVALUES_UNAUTHENTICATED = 16;
  exports.RPCGRPCSTATUSCODEVALUES_OK = TMP_RPCGRPCSTATUSCODEVALUES_OK;
  exports.RPCGRPCSTATUSCODEVALUES_CANCELLED = TMP_RPCGRPCSTATUSCODEVALUES_CANCELLED;
  exports.RPCGRPCSTATUSCODEVALUES_UNKNOWN = TMP_RPCGRPCSTATUSCODEVALUES_UNKNOWN;
  exports.RPCGRPCSTATUSCODEVALUES_INVALID_ARGUMENT = TMP_RPCGRPCSTATUSCODEVALUES_INVALID_ARGUMENT;
  exports.RPCGRPCSTATUSCODEVALUES_DEADLINE_EXCEEDED = TMP_RPCGRPCSTATUSCODEVALUES_DEADLINE_EXCEEDED;
  exports.RPCGRPCSTATUSCODEVALUES_NOT_FOUND = TMP_RPCGRPCSTATUSCODEVALUES_NOT_FOUND;
  exports.RPCGRPCSTATUSCODEVALUES_ALREADY_EXISTS = TMP_RPCGRPCSTATUSCODEVALUES_ALREADY_EXISTS;
  exports.RPCGRPCSTATUSCODEVALUES_PERMISSION_DENIED = TMP_RPCGRPCSTATUSCODEVALUES_PERMISSION_DENIED;
  exports.RPCGRPCSTATUSCODEVALUES_RESOURCE_EXHAUSTED = TMP_RPCGRPCSTATUSCODEVALUES_RESOURCE_EXHAUSTED;
  exports.RPCGRPCSTATUSCODEVALUES_FAILED_PRECONDITION = TMP_RPCGRPCSTATUSCODEVALUES_FAILED_PRECONDITION;
  exports.RPCGRPCSTATUSCODEVALUES_ABORTED = TMP_RPCGRPCSTATUSCODEVALUES_ABORTED;
  exports.RPCGRPCSTATUSCODEVALUES_OUT_OF_RANGE = TMP_RPCGRPCSTATUSCODEVALUES_OUT_OF_RANGE;
  exports.RPCGRPCSTATUSCODEVALUES_UNIMPLEMENTED = TMP_RPCGRPCSTATUSCODEVALUES_UNIMPLEMENTED;
  exports.RPCGRPCSTATUSCODEVALUES_INTERNAL = TMP_RPCGRPCSTATUSCODEVALUES_INTERNAL;
  exports.RPCGRPCSTATUSCODEVALUES_UNAVAILABLE = TMP_RPCGRPCSTATUSCODEVALUES_UNAVAILABLE;
  exports.RPCGRPCSTATUSCODEVALUES_DATA_LOSS = TMP_RPCGRPCSTATUSCODEVALUES_DATA_LOSS;
  exports.RPCGRPCSTATUSCODEVALUES_UNAUTHENTICATED = TMP_RPCGRPCSTATUSCODEVALUES_UNAUTHENTICATED;
  exports.RpcGrpcStatusCodeValues = {
    OK: TMP_RPCGRPCSTATUSCODEVALUES_OK,
    CANCELLED: TMP_RPCGRPCSTATUSCODEVALUES_CANCELLED,
    UNKNOWN: TMP_RPCGRPCSTATUSCODEVALUES_UNKNOWN,
    INVALID_ARGUMENT: TMP_RPCGRPCSTATUSCODEVALUES_INVALID_ARGUMENT,
    DEADLINE_EXCEEDED: TMP_RPCGRPCSTATUSCODEVALUES_DEADLINE_EXCEEDED,
    NOT_FOUND: TMP_RPCGRPCSTATUSCODEVALUES_NOT_FOUND,
    ALREADY_EXISTS: TMP_RPCGRPCSTATUSCODEVALUES_ALREADY_EXISTS,
    PERMISSION_DENIED: TMP_RPCGRPCSTATUSCODEVALUES_PERMISSION_DENIED,
    RESOURCE_EXHAUSTED: TMP_RPCGRPCSTATUSCODEVALUES_RESOURCE_EXHAUSTED,
    FAILED_PRECONDITION: TMP_RPCGRPCSTATUSCODEVALUES_FAILED_PRECONDITION,
    ABORTED: TMP_RPCGRPCSTATUSCODEVALUES_ABORTED,
    OUT_OF_RANGE: TMP_RPCGRPCSTATUSCODEVALUES_OUT_OF_RANGE,
    UNIMPLEMENTED: TMP_RPCGRPCSTATUSCODEVALUES_UNIMPLEMENTED,
    INTERNAL: TMP_RPCGRPCSTATUSCODEVALUES_INTERNAL,
    UNAVAILABLE: TMP_RPCGRPCSTATUSCODEVALUES_UNAVAILABLE,
    DATA_LOSS: TMP_RPCGRPCSTATUSCODEVALUES_DATA_LOSS,
    UNAUTHENTICATED: TMP_RPCGRPCSTATUSCODEVALUES_UNAUTHENTICATED
  };
  var TMP_MESSAGETYPEVALUES_SENT = "SENT";
  var TMP_MESSAGETYPEVALUES_RECEIVED = "RECEIVED";
  exports.MESSAGETYPEVALUES_SENT = TMP_MESSAGETYPEVALUES_SENT;
  exports.MESSAGETYPEVALUES_RECEIVED = TMP_MESSAGETYPEVALUES_RECEIVED;
  exports.MessageTypeValues = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_MESSAGETYPEVALUES_SENT,
    TMP_MESSAGETYPEVALUES_RECEIVED
  ]);
});

// node_modules/.bun/@opentelemetry+semantic-conventions@1.43.0/node_modules/@opentelemetry/semantic-conventions/build/src/trace/index.js
var require_trace = __commonJS(function(exports) {
  var __createBinding = exports && exports.__createBinding || (Object.create ? function(o, m, k, k2) {
    if (k2 === undefined)
      k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() {
        return m[k];
      } };
    }
    Object.defineProperty(o, k2, desc);
  } : function(o, m, k, k2) {
    if (k2 === undefined)
      k2 = k;
    o[k2] = m[k];
  });
  var __exportStar = exports && exports.__exportStar || function(m, exports2) {
    for (var p in m)
      if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports2, p))
        __createBinding(exports2, m, p);
  };
  Object.defineProperty(exports, "__esModule", { value: true });
  __exportStar(require_SemanticAttributes(), exports);
});

// node_modules/.bun/@opentelemetry+semantic-conventions@1.43.0/node_modules/@opentelemetry/semantic-conventions/build/src/resource/SemanticResourceAttributes.js
var require_SemanticResourceAttributes = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.SEMRESATTRS_K8S_STATEFULSET_NAME = exports.SEMRESATTRS_K8S_STATEFULSET_UID = exports.SEMRESATTRS_K8S_DEPLOYMENT_NAME = exports.SEMRESATTRS_K8S_DEPLOYMENT_UID = exports.SEMRESATTRS_K8S_REPLICASET_NAME = exports.SEMRESATTRS_K8S_REPLICASET_UID = exports.SEMRESATTRS_K8S_CONTAINER_NAME = exports.SEMRESATTRS_K8S_POD_NAME = exports.SEMRESATTRS_K8S_POD_UID = exports.SEMRESATTRS_K8S_NAMESPACE_NAME = exports.SEMRESATTRS_K8S_NODE_UID = exports.SEMRESATTRS_K8S_NODE_NAME = exports.SEMRESATTRS_K8S_CLUSTER_NAME = exports.SEMRESATTRS_HOST_IMAGE_VERSION = exports.SEMRESATTRS_HOST_IMAGE_ID = exports.SEMRESATTRS_HOST_IMAGE_NAME = exports.SEMRESATTRS_HOST_ARCH = exports.SEMRESATTRS_HOST_TYPE = exports.SEMRESATTRS_HOST_NAME = exports.SEMRESATTRS_HOST_ID = exports.SEMRESATTRS_FAAS_MAX_MEMORY = exports.SEMRESATTRS_FAAS_INSTANCE = exports.SEMRESATTRS_FAAS_VERSION = exports.SEMRESATTRS_FAAS_ID = exports.SEMRESATTRS_FAAS_NAME = exports.SEMRESATTRS_DEVICE_MODEL_NAME = exports.SEMRESATTRS_DEVICE_MODEL_IDENTIFIER = exports.SEMRESATTRS_DEVICE_ID = exports.SEMRESATTRS_DEPLOYMENT_ENVIRONMENT = exports.SEMRESATTRS_CONTAINER_IMAGE_TAG = exports.SEMRESATTRS_CONTAINER_IMAGE_NAME = exports.SEMRESATTRS_CONTAINER_RUNTIME = exports.SEMRESATTRS_CONTAINER_ID = exports.SEMRESATTRS_CONTAINER_NAME = exports.SEMRESATTRS_AWS_LOG_STREAM_ARNS = exports.SEMRESATTRS_AWS_LOG_STREAM_NAMES = exports.SEMRESATTRS_AWS_LOG_GROUP_ARNS = exports.SEMRESATTRS_AWS_LOG_GROUP_NAMES = exports.SEMRESATTRS_AWS_EKS_CLUSTER_ARN = exports.SEMRESATTRS_AWS_ECS_TASK_REVISION = exports.SEMRESATTRS_AWS_ECS_TASK_FAMILY = exports.SEMRESATTRS_AWS_ECS_TASK_ARN = exports.SEMRESATTRS_AWS_ECS_LAUNCHTYPE = exports.SEMRESATTRS_AWS_ECS_CLUSTER_ARN = exports.SEMRESATTRS_AWS_ECS_CONTAINER_ARN = exports.SEMRESATTRS_CLOUD_PLATFORM = exports.SEMRESATTRS_CLOUD_AVAILABILITY_ZONE = exports.SEMRESATTRS_CLOUD_REGION = exports.SEMRESATTRS_CLOUD_ACCOUNT_ID = exports.SEMRESATTRS_CLOUD_PROVIDER = undefined;
  exports.CLOUDPLATFORMVALUES_GCP_COMPUTE_ENGINE = exports.CLOUDPLATFORMVALUES_AZURE_APP_SERVICE = exports.CLOUDPLATFORMVALUES_AZURE_FUNCTIONS = exports.CLOUDPLATFORMVALUES_AZURE_AKS = exports.CLOUDPLATFORMVALUES_AZURE_CONTAINER_INSTANCES = exports.CLOUDPLATFORMVALUES_AZURE_VM = exports.CLOUDPLATFORMVALUES_AWS_ELASTIC_BEANSTALK = exports.CLOUDPLATFORMVALUES_AWS_LAMBDA = exports.CLOUDPLATFORMVALUES_AWS_EKS = exports.CLOUDPLATFORMVALUES_AWS_ECS = exports.CLOUDPLATFORMVALUES_AWS_EC2 = exports.CLOUDPLATFORMVALUES_ALIBABA_CLOUD_FC = exports.CLOUDPLATFORMVALUES_ALIBABA_CLOUD_ECS = exports.CloudProviderValues = exports.CLOUDPROVIDERVALUES_GCP = exports.CLOUDPROVIDERVALUES_AZURE = exports.CLOUDPROVIDERVALUES_AWS = exports.CLOUDPROVIDERVALUES_ALIBABA_CLOUD = exports.SemanticResourceAttributes = exports.SEMRESATTRS_WEBENGINE_DESCRIPTION = exports.SEMRESATTRS_WEBENGINE_VERSION = exports.SEMRESATTRS_WEBENGINE_NAME = exports.SEMRESATTRS_TELEMETRY_AUTO_VERSION = exports.SEMRESATTRS_TELEMETRY_SDK_VERSION = exports.SEMRESATTRS_TELEMETRY_SDK_LANGUAGE = exports.SEMRESATTRS_TELEMETRY_SDK_NAME = exports.SEMRESATTRS_SERVICE_VERSION = exports.SEMRESATTRS_SERVICE_INSTANCE_ID = exports.SEMRESATTRS_SERVICE_NAMESPACE = exports.SEMRESATTRS_SERVICE_NAME = exports.SEMRESATTRS_PROCESS_RUNTIME_DESCRIPTION = exports.SEMRESATTRS_PROCESS_RUNTIME_VERSION = exports.SEMRESATTRS_PROCESS_RUNTIME_NAME = exports.SEMRESATTRS_PROCESS_OWNER = exports.SEMRESATTRS_PROCESS_COMMAND_ARGS = exports.SEMRESATTRS_PROCESS_COMMAND_LINE = exports.SEMRESATTRS_PROCESS_COMMAND = exports.SEMRESATTRS_PROCESS_EXECUTABLE_PATH = exports.SEMRESATTRS_PROCESS_EXECUTABLE_NAME = exports.SEMRESATTRS_PROCESS_PID = exports.SEMRESATTRS_OS_VERSION = exports.SEMRESATTRS_OS_NAME = exports.SEMRESATTRS_OS_DESCRIPTION = exports.SEMRESATTRS_OS_TYPE = exports.SEMRESATTRS_K8S_CRONJOB_NAME = exports.SEMRESATTRS_K8S_CRONJOB_UID = exports.SEMRESATTRS_K8S_JOB_NAME = exports.SEMRESATTRS_K8S_JOB_UID = exports.SEMRESATTRS_K8S_DAEMONSET_NAME = exports.SEMRESATTRS_K8S_DAEMONSET_UID = undefined;
  exports.TelemetrySdkLanguageValues = exports.TELEMETRYSDKLANGUAGEVALUES_WEBJS = exports.TELEMETRYSDKLANGUAGEVALUES_RUBY = exports.TELEMETRYSDKLANGUAGEVALUES_PYTHON = exports.TELEMETRYSDKLANGUAGEVALUES_PHP = exports.TELEMETRYSDKLANGUAGEVALUES_NODEJS = exports.TELEMETRYSDKLANGUAGEVALUES_JAVA = exports.TELEMETRYSDKLANGUAGEVALUES_GO = exports.TELEMETRYSDKLANGUAGEVALUES_ERLANG = exports.TELEMETRYSDKLANGUAGEVALUES_DOTNET = exports.TELEMETRYSDKLANGUAGEVALUES_CPP = exports.OsTypeValues = exports.OSTYPEVALUES_Z_OS = exports.OSTYPEVALUES_SOLARIS = exports.OSTYPEVALUES_AIX = exports.OSTYPEVALUES_HPUX = exports.OSTYPEVALUES_DRAGONFLYBSD = exports.OSTYPEVALUES_OPENBSD = exports.OSTYPEVALUES_NETBSD = exports.OSTYPEVALUES_FREEBSD = exports.OSTYPEVALUES_DARWIN = exports.OSTYPEVALUES_LINUX = exports.OSTYPEVALUES_WINDOWS = exports.HostArchValues = exports.HOSTARCHVALUES_X86 = exports.HOSTARCHVALUES_PPC64 = exports.HOSTARCHVALUES_PPC32 = exports.HOSTARCHVALUES_IA64 = exports.HOSTARCHVALUES_ARM64 = exports.HOSTARCHVALUES_ARM32 = exports.HOSTARCHVALUES_AMD64 = exports.AwsEcsLaunchtypeValues = exports.AWSECSLAUNCHTYPEVALUES_FARGATE = exports.AWSECSLAUNCHTYPEVALUES_EC2 = exports.CloudPlatformValues = exports.CLOUDPLATFORMVALUES_GCP_APP_ENGINE = exports.CLOUDPLATFORMVALUES_GCP_CLOUD_FUNCTIONS = exports.CLOUDPLATFORMVALUES_GCP_KUBERNETES_ENGINE = exports.CLOUDPLATFORMVALUES_GCP_CLOUD_RUN = undefined;
  var utils_1 = require_utils2();
  var TMP_CLOUD_PROVIDER = "cloud.provider";
  var TMP_CLOUD_ACCOUNT_ID = "cloud.account.id";
  var TMP_CLOUD_REGION = "cloud.region";
  var TMP_CLOUD_AVAILABILITY_ZONE = "cloud.availability_zone";
  var TMP_CLOUD_PLATFORM = "cloud.platform";
  var TMP_AWS_ECS_CONTAINER_ARN = "aws.ecs.container.arn";
  var TMP_AWS_ECS_CLUSTER_ARN = "aws.ecs.cluster.arn";
  var TMP_AWS_ECS_LAUNCHTYPE = "aws.ecs.launchtype";
  var TMP_AWS_ECS_TASK_ARN = "aws.ecs.task.arn";
  var TMP_AWS_ECS_TASK_FAMILY = "aws.ecs.task.family";
  var TMP_AWS_ECS_TASK_REVISION = "aws.ecs.task.revision";
  var TMP_AWS_EKS_CLUSTER_ARN = "aws.eks.cluster.arn";
  var TMP_AWS_LOG_GROUP_NAMES = "aws.log.group.names";
  var TMP_AWS_LOG_GROUP_ARNS = "aws.log.group.arns";
  var TMP_AWS_LOG_STREAM_NAMES = "aws.log.stream.names";
  var TMP_AWS_LOG_STREAM_ARNS = "aws.log.stream.arns";
  var TMP_CONTAINER_NAME = "container.name";
  var TMP_CONTAINER_ID = "container.id";
  var TMP_CONTAINER_RUNTIME = "container.runtime";
  var TMP_CONTAINER_IMAGE_NAME = "container.image.name";
  var TMP_CONTAINER_IMAGE_TAG = "container.image.tag";
  var TMP_DEPLOYMENT_ENVIRONMENT = "deployment.environment";
  var TMP_DEVICE_ID = "device.id";
  var TMP_DEVICE_MODEL_IDENTIFIER = "device.model.identifier";
  var TMP_DEVICE_MODEL_NAME = "device.model.name";
  var TMP_FAAS_NAME = "faas.name";
  var TMP_FAAS_ID = "faas.id";
  var TMP_FAAS_VERSION = "faas.version";
  var TMP_FAAS_INSTANCE = "faas.instance";
  var TMP_FAAS_MAX_MEMORY = "faas.max_memory";
  var TMP_HOST_ID = "host.id";
  var TMP_HOST_NAME = "host.name";
  var TMP_HOST_TYPE = "host.type";
  var TMP_HOST_ARCH = "host.arch";
  var TMP_HOST_IMAGE_NAME = "host.image.name";
  var TMP_HOST_IMAGE_ID = "host.image.id";
  var TMP_HOST_IMAGE_VERSION = "host.image.version";
  var TMP_K8S_CLUSTER_NAME = "k8s.cluster.name";
  var TMP_K8S_NODE_NAME = "k8s.node.name";
  var TMP_K8S_NODE_UID = "k8s.node.uid";
  var TMP_K8S_NAMESPACE_NAME = "k8s.namespace.name";
  var TMP_K8S_POD_UID = "k8s.pod.uid";
  var TMP_K8S_POD_NAME = "k8s.pod.name";
  var TMP_K8S_CONTAINER_NAME = "k8s.container.name";
  var TMP_K8S_REPLICASET_UID = "k8s.replicaset.uid";
  var TMP_K8S_REPLICASET_NAME = "k8s.replicaset.name";
  var TMP_K8S_DEPLOYMENT_UID = "k8s.deployment.uid";
  var TMP_K8S_DEPLOYMENT_NAME = "k8s.deployment.name";
  var TMP_K8S_STATEFULSET_UID = "k8s.statefulset.uid";
  var TMP_K8S_STATEFULSET_NAME = "k8s.statefulset.name";
  var TMP_K8S_DAEMONSET_UID = "k8s.daemonset.uid";
  var TMP_K8S_DAEMONSET_NAME = "k8s.daemonset.name";
  var TMP_K8S_JOB_UID = "k8s.job.uid";
  var TMP_K8S_JOB_NAME = "k8s.job.name";
  var TMP_K8S_CRONJOB_UID = "k8s.cronjob.uid";
  var TMP_K8S_CRONJOB_NAME = "k8s.cronjob.name";
  var TMP_OS_TYPE = "os.type";
  var TMP_OS_DESCRIPTION = "os.description";
  var TMP_OS_NAME = "os.name";
  var TMP_OS_VERSION = "os.version";
  var TMP_PROCESS_PID = "process.pid";
  var TMP_PROCESS_EXECUTABLE_NAME = "process.executable.name";
  var TMP_PROCESS_EXECUTABLE_PATH = "process.executable.path";
  var TMP_PROCESS_COMMAND = "process.command";
  var TMP_PROCESS_COMMAND_LINE = "process.command_line";
  var TMP_PROCESS_COMMAND_ARGS = "process.command_args";
  var TMP_PROCESS_OWNER = "process.owner";
  var TMP_PROCESS_RUNTIME_NAME = "process.runtime.name";
  var TMP_PROCESS_RUNTIME_VERSION = "process.runtime.version";
  var TMP_PROCESS_RUNTIME_DESCRIPTION = "process.runtime.description";
  var TMP_SERVICE_NAME = "service.name";
  var TMP_SERVICE_NAMESPACE = "service.namespace";
  var TMP_SERVICE_INSTANCE_ID = "service.instance.id";
  var TMP_SERVICE_VERSION = "service.version";
  var TMP_TELEMETRY_SDK_NAME = "telemetry.sdk.name";
  var TMP_TELEMETRY_SDK_LANGUAGE = "telemetry.sdk.language";
  var TMP_TELEMETRY_SDK_VERSION = "telemetry.sdk.version";
  var TMP_TELEMETRY_AUTO_VERSION = "telemetry.auto.version";
  var TMP_WEBENGINE_NAME = "webengine.name";
  var TMP_WEBENGINE_VERSION = "webengine.version";
  var TMP_WEBENGINE_DESCRIPTION = "webengine.description";
  exports.SEMRESATTRS_CLOUD_PROVIDER = TMP_CLOUD_PROVIDER;
  exports.SEMRESATTRS_CLOUD_ACCOUNT_ID = TMP_CLOUD_ACCOUNT_ID;
  exports.SEMRESATTRS_CLOUD_REGION = TMP_CLOUD_REGION;
  exports.SEMRESATTRS_CLOUD_AVAILABILITY_ZONE = TMP_CLOUD_AVAILABILITY_ZONE;
  exports.SEMRESATTRS_CLOUD_PLATFORM = TMP_CLOUD_PLATFORM;
  exports.SEMRESATTRS_AWS_ECS_CONTAINER_ARN = TMP_AWS_ECS_CONTAINER_ARN;
  exports.SEMRESATTRS_AWS_ECS_CLUSTER_ARN = TMP_AWS_ECS_CLUSTER_ARN;
  exports.SEMRESATTRS_AWS_ECS_LAUNCHTYPE = TMP_AWS_ECS_LAUNCHTYPE;
  exports.SEMRESATTRS_AWS_ECS_TASK_ARN = TMP_AWS_ECS_TASK_ARN;
  exports.SEMRESATTRS_AWS_ECS_TASK_FAMILY = TMP_AWS_ECS_TASK_FAMILY;
  exports.SEMRESATTRS_AWS_ECS_TASK_REVISION = TMP_AWS_ECS_TASK_REVISION;
  exports.SEMRESATTRS_AWS_EKS_CLUSTER_ARN = TMP_AWS_EKS_CLUSTER_ARN;
  exports.SEMRESATTRS_AWS_LOG_GROUP_NAMES = TMP_AWS_LOG_GROUP_NAMES;
  exports.SEMRESATTRS_AWS_LOG_GROUP_ARNS = TMP_AWS_LOG_GROUP_ARNS;
  exports.SEMRESATTRS_AWS_LOG_STREAM_NAMES = TMP_AWS_LOG_STREAM_NAMES;
  exports.SEMRESATTRS_AWS_LOG_STREAM_ARNS = TMP_AWS_LOG_STREAM_ARNS;
  exports.SEMRESATTRS_CONTAINER_NAME = TMP_CONTAINER_NAME;
  exports.SEMRESATTRS_CONTAINER_ID = TMP_CONTAINER_ID;
  exports.SEMRESATTRS_CONTAINER_RUNTIME = TMP_CONTAINER_RUNTIME;
  exports.SEMRESATTRS_CONTAINER_IMAGE_NAME = TMP_CONTAINER_IMAGE_NAME;
  exports.SEMRESATTRS_CONTAINER_IMAGE_TAG = TMP_CONTAINER_IMAGE_TAG;
  exports.SEMRESATTRS_DEPLOYMENT_ENVIRONMENT = TMP_DEPLOYMENT_ENVIRONMENT;
  exports.SEMRESATTRS_DEVICE_ID = TMP_DEVICE_ID;
  exports.SEMRESATTRS_DEVICE_MODEL_IDENTIFIER = TMP_DEVICE_MODEL_IDENTIFIER;
  exports.SEMRESATTRS_DEVICE_MODEL_NAME = TMP_DEVICE_MODEL_NAME;
  exports.SEMRESATTRS_FAAS_NAME = TMP_FAAS_NAME;
  exports.SEMRESATTRS_FAAS_ID = TMP_FAAS_ID;
  exports.SEMRESATTRS_FAAS_VERSION = TMP_FAAS_VERSION;
  exports.SEMRESATTRS_FAAS_INSTANCE = TMP_FAAS_INSTANCE;
  exports.SEMRESATTRS_FAAS_MAX_MEMORY = TMP_FAAS_MAX_MEMORY;
  exports.SEMRESATTRS_HOST_ID = TMP_HOST_ID;
  exports.SEMRESATTRS_HOST_NAME = TMP_HOST_NAME;
  exports.SEMRESATTRS_HOST_TYPE = TMP_HOST_TYPE;
  exports.SEMRESATTRS_HOST_ARCH = TMP_HOST_ARCH;
  exports.SEMRESATTRS_HOST_IMAGE_NAME = TMP_HOST_IMAGE_NAME;
  exports.SEMRESATTRS_HOST_IMAGE_ID = TMP_HOST_IMAGE_ID;
  exports.SEMRESATTRS_HOST_IMAGE_VERSION = TMP_HOST_IMAGE_VERSION;
  exports.SEMRESATTRS_K8S_CLUSTER_NAME = TMP_K8S_CLUSTER_NAME;
  exports.SEMRESATTRS_K8S_NODE_NAME = TMP_K8S_NODE_NAME;
  exports.SEMRESATTRS_K8S_NODE_UID = TMP_K8S_NODE_UID;
  exports.SEMRESATTRS_K8S_NAMESPACE_NAME = TMP_K8S_NAMESPACE_NAME;
  exports.SEMRESATTRS_K8S_POD_UID = TMP_K8S_POD_UID;
  exports.SEMRESATTRS_K8S_POD_NAME = TMP_K8S_POD_NAME;
  exports.SEMRESATTRS_K8S_CONTAINER_NAME = TMP_K8S_CONTAINER_NAME;
  exports.SEMRESATTRS_K8S_REPLICASET_UID = TMP_K8S_REPLICASET_UID;
  exports.SEMRESATTRS_K8S_REPLICASET_NAME = TMP_K8S_REPLICASET_NAME;
  exports.SEMRESATTRS_K8S_DEPLOYMENT_UID = TMP_K8S_DEPLOYMENT_UID;
  exports.SEMRESATTRS_K8S_DEPLOYMENT_NAME = TMP_K8S_DEPLOYMENT_NAME;
  exports.SEMRESATTRS_K8S_STATEFULSET_UID = TMP_K8S_STATEFULSET_UID;
  exports.SEMRESATTRS_K8S_STATEFULSET_NAME = TMP_K8S_STATEFULSET_NAME;
  exports.SEMRESATTRS_K8S_DAEMONSET_UID = TMP_K8S_DAEMONSET_UID;
  exports.SEMRESATTRS_K8S_DAEMONSET_NAME = TMP_K8S_DAEMONSET_NAME;
  exports.SEMRESATTRS_K8S_JOB_UID = TMP_K8S_JOB_UID;
  exports.SEMRESATTRS_K8S_JOB_NAME = TMP_K8S_JOB_NAME;
  exports.SEMRESATTRS_K8S_CRONJOB_UID = TMP_K8S_CRONJOB_UID;
  exports.SEMRESATTRS_K8S_CRONJOB_NAME = TMP_K8S_CRONJOB_NAME;
  exports.SEMRESATTRS_OS_TYPE = TMP_OS_TYPE;
  exports.SEMRESATTRS_OS_DESCRIPTION = TMP_OS_DESCRIPTION;
  exports.SEMRESATTRS_OS_NAME = TMP_OS_NAME;
  exports.SEMRESATTRS_OS_VERSION = TMP_OS_VERSION;
  exports.SEMRESATTRS_PROCESS_PID = TMP_PROCESS_PID;
  exports.SEMRESATTRS_PROCESS_EXECUTABLE_NAME = TMP_PROCESS_EXECUTABLE_NAME;
  exports.SEMRESATTRS_PROCESS_EXECUTABLE_PATH = TMP_PROCESS_EXECUTABLE_PATH;
  exports.SEMRESATTRS_PROCESS_COMMAND = TMP_PROCESS_COMMAND;
  exports.SEMRESATTRS_PROCESS_COMMAND_LINE = TMP_PROCESS_COMMAND_LINE;
  exports.SEMRESATTRS_PROCESS_COMMAND_ARGS = TMP_PROCESS_COMMAND_ARGS;
  exports.SEMRESATTRS_PROCESS_OWNER = TMP_PROCESS_OWNER;
  exports.SEMRESATTRS_PROCESS_RUNTIME_NAME = TMP_PROCESS_RUNTIME_NAME;
  exports.SEMRESATTRS_PROCESS_RUNTIME_VERSION = TMP_PROCESS_RUNTIME_VERSION;
  exports.SEMRESATTRS_PROCESS_RUNTIME_DESCRIPTION = TMP_PROCESS_RUNTIME_DESCRIPTION;
  exports.SEMRESATTRS_SERVICE_NAME = TMP_SERVICE_NAME;
  exports.SEMRESATTRS_SERVICE_NAMESPACE = TMP_SERVICE_NAMESPACE;
  exports.SEMRESATTRS_SERVICE_INSTANCE_ID = TMP_SERVICE_INSTANCE_ID;
  exports.SEMRESATTRS_SERVICE_VERSION = TMP_SERVICE_VERSION;
  exports.SEMRESATTRS_TELEMETRY_SDK_NAME = TMP_TELEMETRY_SDK_NAME;
  exports.SEMRESATTRS_TELEMETRY_SDK_LANGUAGE = TMP_TELEMETRY_SDK_LANGUAGE;
  exports.SEMRESATTRS_TELEMETRY_SDK_VERSION = TMP_TELEMETRY_SDK_VERSION;
  exports.SEMRESATTRS_TELEMETRY_AUTO_VERSION = TMP_TELEMETRY_AUTO_VERSION;
  exports.SEMRESATTRS_WEBENGINE_NAME = TMP_WEBENGINE_NAME;
  exports.SEMRESATTRS_WEBENGINE_VERSION = TMP_WEBENGINE_VERSION;
  exports.SEMRESATTRS_WEBENGINE_DESCRIPTION = TMP_WEBENGINE_DESCRIPTION;
  exports.SemanticResourceAttributes = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_CLOUD_PROVIDER,
    TMP_CLOUD_ACCOUNT_ID,
    TMP_CLOUD_REGION,
    TMP_CLOUD_AVAILABILITY_ZONE,
    TMP_CLOUD_PLATFORM,
    TMP_AWS_ECS_CONTAINER_ARN,
    TMP_AWS_ECS_CLUSTER_ARN,
    TMP_AWS_ECS_LAUNCHTYPE,
    TMP_AWS_ECS_TASK_ARN,
    TMP_AWS_ECS_TASK_FAMILY,
    TMP_AWS_ECS_TASK_REVISION,
    TMP_AWS_EKS_CLUSTER_ARN,
    TMP_AWS_LOG_GROUP_NAMES,
    TMP_AWS_LOG_GROUP_ARNS,
    TMP_AWS_LOG_STREAM_NAMES,
    TMP_AWS_LOG_STREAM_ARNS,
    TMP_CONTAINER_NAME,
    TMP_CONTAINER_ID,
    TMP_CONTAINER_RUNTIME,
    TMP_CONTAINER_IMAGE_NAME,
    TMP_CONTAINER_IMAGE_TAG,
    TMP_DEPLOYMENT_ENVIRONMENT,
    TMP_DEVICE_ID,
    TMP_DEVICE_MODEL_IDENTIFIER,
    TMP_DEVICE_MODEL_NAME,
    TMP_FAAS_NAME,
    TMP_FAAS_ID,
    TMP_FAAS_VERSION,
    TMP_FAAS_INSTANCE,
    TMP_FAAS_MAX_MEMORY,
    TMP_HOST_ID,
    TMP_HOST_NAME,
    TMP_HOST_TYPE,
    TMP_HOST_ARCH,
    TMP_HOST_IMAGE_NAME,
    TMP_HOST_IMAGE_ID,
    TMP_HOST_IMAGE_VERSION,
    TMP_K8S_CLUSTER_NAME,
    TMP_K8S_NODE_NAME,
    TMP_K8S_NODE_UID,
    TMP_K8S_NAMESPACE_NAME,
    TMP_K8S_POD_UID,
    TMP_K8S_POD_NAME,
    TMP_K8S_CONTAINER_NAME,
    TMP_K8S_REPLICASET_UID,
    TMP_K8S_REPLICASET_NAME,
    TMP_K8S_DEPLOYMENT_UID,
    TMP_K8S_DEPLOYMENT_NAME,
    TMP_K8S_STATEFULSET_UID,
    TMP_K8S_STATEFULSET_NAME,
    TMP_K8S_DAEMONSET_UID,
    TMP_K8S_DAEMONSET_NAME,
    TMP_K8S_JOB_UID,
    TMP_K8S_JOB_NAME,
    TMP_K8S_CRONJOB_UID,
    TMP_K8S_CRONJOB_NAME,
    TMP_OS_TYPE,
    TMP_OS_DESCRIPTION,
    TMP_OS_NAME,
    TMP_OS_VERSION,
    TMP_PROCESS_PID,
    TMP_PROCESS_EXECUTABLE_NAME,
    TMP_PROCESS_EXECUTABLE_PATH,
    TMP_PROCESS_COMMAND,
    TMP_PROCESS_COMMAND_LINE,
    TMP_PROCESS_COMMAND_ARGS,
    TMP_PROCESS_OWNER,
    TMP_PROCESS_RUNTIME_NAME,
    TMP_PROCESS_RUNTIME_VERSION,
    TMP_PROCESS_RUNTIME_DESCRIPTION,
    TMP_SERVICE_NAME,
    TMP_SERVICE_NAMESPACE,
    TMP_SERVICE_INSTANCE_ID,
    TMP_SERVICE_VERSION,
    TMP_TELEMETRY_SDK_NAME,
    TMP_TELEMETRY_SDK_LANGUAGE,
    TMP_TELEMETRY_SDK_VERSION,
    TMP_TELEMETRY_AUTO_VERSION,
    TMP_WEBENGINE_NAME,
    TMP_WEBENGINE_VERSION,
    TMP_WEBENGINE_DESCRIPTION
  ]);
  var TMP_CLOUDPROVIDERVALUES_ALIBABA_CLOUD = "alibaba_cloud";
  var TMP_CLOUDPROVIDERVALUES_AWS = "aws";
  var TMP_CLOUDPROVIDERVALUES_AZURE = "azure";
  var TMP_CLOUDPROVIDERVALUES_GCP = "gcp";
  exports.CLOUDPROVIDERVALUES_ALIBABA_CLOUD = TMP_CLOUDPROVIDERVALUES_ALIBABA_CLOUD;
  exports.CLOUDPROVIDERVALUES_AWS = TMP_CLOUDPROVIDERVALUES_AWS;
  exports.CLOUDPROVIDERVALUES_AZURE = TMP_CLOUDPROVIDERVALUES_AZURE;
  exports.CLOUDPROVIDERVALUES_GCP = TMP_CLOUDPROVIDERVALUES_GCP;
  exports.CloudProviderValues = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_CLOUDPROVIDERVALUES_ALIBABA_CLOUD,
    TMP_CLOUDPROVIDERVALUES_AWS,
    TMP_CLOUDPROVIDERVALUES_AZURE,
    TMP_CLOUDPROVIDERVALUES_GCP
  ]);
  var TMP_CLOUDPLATFORMVALUES_ALIBABA_CLOUD_ECS = "alibaba_cloud_ecs";
  var TMP_CLOUDPLATFORMVALUES_ALIBABA_CLOUD_FC = "alibaba_cloud_fc";
  var TMP_CLOUDPLATFORMVALUES_AWS_EC2 = "aws_ec2";
  var TMP_CLOUDPLATFORMVALUES_AWS_ECS = "aws_ecs";
  var TMP_CLOUDPLATFORMVALUES_AWS_EKS = "aws_eks";
  var TMP_CLOUDPLATFORMVALUES_AWS_LAMBDA = "aws_lambda";
  var TMP_CLOUDPLATFORMVALUES_AWS_ELASTIC_BEANSTALK = "aws_elastic_beanstalk";
  var TMP_CLOUDPLATFORMVALUES_AZURE_VM = "azure_vm";
  var TMP_CLOUDPLATFORMVALUES_AZURE_CONTAINER_INSTANCES = "azure_container_instances";
  var TMP_CLOUDPLATFORMVALUES_AZURE_AKS = "azure_aks";
  var TMP_CLOUDPLATFORMVALUES_AZURE_FUNCTIONS = "azure_functions";
  var TMP_CLOUDPLATFORMVALUES_AZURE_APP_SERVICE = "azure_app_service";
  var TMP_CLOUDPLATFORMVALUES_GCP_COMPUTE_ENGINE = "gcp_compute_engine";
  var TMP_CLOUDPLATFORMVALUES_GCP_CLOUD_RUN = "gcp_cloud_run";
  var TMP_CLOUDPLATFORMVALUES_GCP_KUBERNETES_ENGINE = "gcp_kubernetes_engine";
  var TMP_CLOUDPLATFORMVALUES_GCP_CLOUD_FUNCTIONS = "gcp_cloud_functions";
  var TMP_CLOUDPLATFORMVALUES_GCP_APP_ENGINE = "gcp_app_engine";
  exports.CLOUDPLATFORMVALUES_ALIBABA_CLOUD_ECS = TMP_CLOUDPLATFORMVALUES_ALIBABA_CLOUD_ECS;
  exports.CLOUDPLATFORMVALUES_ALIBABA_CLOUD_FC = TMP_CLOUDPLATFORMVALUES_ALIBABA_CLOUD_FC;
  exports.CLOUDPLATFORMVALUES_AWS_EC2 = TMP_CLOUDPLATFORMVALUES_AWS_EC2;
  exports.CLOUDPLATFORMVALUES_AWS_ECS = TMP_CLOUDPLATFORMVALUES_AWS_ECS;
  exports.CLOUDPLATFORMVALUES_AWS_EKS = TMP_CLOUDPLATFORMVALUES_AWS_EKS;
  exports.CLOUDPLATFORMVALUES_AWS_LAMBDA = TMP_CLOUDPLATFORMVALUES_AWS_LAMBDA;
  exports.CLOUDPLATFORMVALUES_AWS_ELASTIC_BEANSTALK = TMP_CLOUDPLATFORMVALUES_AWS_ELASTIC_BEANSTALK;
  exports.CLOUDPLATFORMVALUES_AZURE_VM = TMP_CLOUDPLATFORMVALUES_AZURE_VM;
  exports.CLOUDPLATFORMVALUES_AZURE_CONTAINER_INSTANCES = TMP_CLOUDPLATFORMVALUES_AZURE_CONTAINER_INSTANCES;
  exports.CLOUDPLATFORMVALUES_AZURE_AKS = TMP_CLOUDPLATFORMVALUES_AZURE_AKS;
  exports.CLOUDPLATFORMVALUES_AZURE_FUNCTIONS = TMP_CLOUDPLATFORMVALUES_AZURE_FUNCTIONS;
  exports.CLOUDPLATFORMVALUES_AZURE_APP_SERVICE = TMP_CLOUDPLATFORMVALUES_AZURE_APP_SERVICE;
  exports.CLOUDPLATFORMVALUES_GCP_COMPUTE_ENGINE = TMP_CLOUDPLATFORMVALUES_GCP_COMPUTE_ENGINE;
  exports.CLOUDPLATFORMVALUES_GCP_CLOUD_RUN = TMP_CLOUDPLATFORMVALUES_GCP_CLOUD_RUN;
  exports.CLOUDPLATFORMVALUES_GCP_KUBERNETES_ENGINE = TMP_CLOUDPLATFORMVALUES_GCP_KUBERNETES_ENGINE;
  exports.CLOUDPLATFORMVALUES_GCP_CLOUD_FUNCTIONS = TMP_CLOUDPLATFORMVALUES_GCP_CLOUD_FUNCTIONS;
  exports.CLOUDPLATFORMVALUES_GCP_APP_ENGINE = TMP_CLOUDPLATFORMVALUES_GCP_APP_ENGINE;
  exports.CloudPlatformValues = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_CLOUDPLATFORMVALUES_ALIBABA_CLOUD_ECS,
    TMP_CLOUDPLATFORMVALUES_ALIBABA_CLOUD_FC,
    TMP_CLOUDPLATFORMVALUES_AWS_EC2,
    TMP_CLOUDPLATFORMVALUES_AWS_ECS,
    TMP_CLOUDPLATFORMVALUES_AWS_EKS,
    TMP_CLOUDPLATFORMVALUES_AWS_LAMBDA,
    TMP_CLOUDPLATFORMVALUES_AWS_ELASTIC_BEANSTALK,
    TMP_CLOUDPLATFORMVALUES_AZURE_VM,
    TMP_CLOUDPLATFORMVALUES_AZURE_CONTAINER_INSTANCES,
    TMP_CLOUDPLATFORMVALUES_AZURE_AKS,
    TMP_CLOUDPLATFORMVALUES_AZURE_FUNCTIONS,
    TMP_CLOUDPLATFORMVALUES_AZURE_APP_SERVICE,
    TMP_CLOUDPLATFORMVALUES_GCP_COMPUTE_ENGINE,
    TMP_CLOUDPLATFORMVALUES_GCP_CLOUD_RUN,
    TMP_CLOUDPLATFORMVALUES_GCP_KUBERNETES_ENGINE,
    TMP_CLOUDPLATFORMVALUES_GCP_CLOUD_FUNCTIONS,
    TMP_CLOUDPLATFORMVALUES_GCP_APP_ENGINE
  ]);
  var TMP_AWSECSLAUNCHTYPEVALUES_EC2 = "ec2";
  var TMP_AWSECSLAUNCHTYPEVALUES_FARGATE = "fargate";
  exports.AWSECSLAUNCHTYPEVALUES_EC2 = TMP_AWSECSLAUNCHTYPEVALUES_EC2;
  exports.AWSECSLAUNCHTYPEVALUES_FARGATE = TMP_AWSECSLAUNCHTYPEVALUES_FARGATE;
  exports.AwsEcsLaunchtypeValues = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_AWSECSLAUNCHTYPEVALUES_EC2,
    TMP_AWSECSLAUNCHTYPEVALUES_FARGATE
  ]);
  var TMP_HOSTARCHVALUES_AMD64 = "amd64";
  var TMP_HOSTARCHVALUES_ARM32 = "arm32";
  var TMP_HOSTARCHVALUES_ARM64 = "arm64";
  var TMP_HOSTARCHVALUES_IA64 = "ia64";
  var TMP_HOSTARCHVALUES_PPC32 = "ppc32";
  var TMP_HOSTARCHVALUES_PPC64 = "ppc64";
  var TMP_HOSTARCHVALUES_X86 = "x86";
  exports.HOSTARCHVALUES_AMD64 = TMP_HOSTARCHVALUES_AMD64;
  exports.HOSTARCHVALUES_ARM32 = TMP_HOSTARCHVALUES_ARM32;
  exports.HOSTARCHVALUES_ARM64 = TMP_HOSTARCHVALUES_ARM64;
  exports.HOSTARCHVALUES_IA64 = TMP_HOSTARCHVALUES_IA64;
  exports.HOSTARCHVALUES_PPC32 = TMP_HOSTARCHVALUES_PPC32;
  exports.HOSTARCHVALUES_PPC64 = TMP_HOSTARCHVALUES_PPC64;
  exports.HOSTARCHVALUES_X86 = TMP_HOSTARCHVALUES_X86;
  exports.HostArchValues = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_HOSTARCHVALUES_AMD64,
    TMP_HOSTARCHVALUES_ARM32,
    TMP_HOSTARCHVALUES_ARM64,
    TMP_HOSTARCHVALUES_IA64,
    TMP_HOSTARCHVALUES_PPC32,
    TMP_HOSTARCHVALUES_PPC64,
    TMP_HOSTARCHVALUES_X86
  ]);
  var TMP_OSTYPEVALUES_WINDOWS = "windows";
  var TMP_OSTYPEVALUES_LINUX = "linux";
  var TMP_OSTYPEVALUES_DARWIN = "darwin";
  var TMP_OSTYPEVALUES_FREEBSD = "freebsd";
  var TMP_OSTYPEVALUES_NETBSD = "netbsd";
  var TMP_OSTYPEVALUES_OPENBSD = "openbsd";
  var TMP_OSTYPEVALUES_DRAGONFLYBSD = "dragonflybsd";
  var TMP_OSTYPEVALUES_HPUX = "hpux";
  var TMP_OSTYPEVALUES_AIX = "aix";
  var TMP_OSTYPEVALUES_SOLARIS = "solaris";
  var TMP_OSTYPEVALUES_Z_OS = "z_os";
  exports.OSTYPEVALUES_WINDOWS = TMP_OSTYPEVALUES_WINDOWS;
  exports.OSTYPEVALUES_LINUX = TMP_OSTYPEVALUES_LINUX;
  exports.OSTYPEVALUES_DARWIN = TMP_OSTYPEVALUES_DARWIN;
  exports.OSTYPEVALUES_FREEBSD = TMP_OSTYPEVALUES_FREEBSD;
  exports.OSTYPEVALUES_NETBSD = TMP_OSTYPEVALUES_NETBSD;
  exports.OSTYPEVALUES_OPENBSD = TMP_OSTYPEVALUES_OPENBSD;
  exports.OSTYPEVALUES_DRAGONFLYBSD = TMP_OSTYPEVALUES_DRAGONFLYBSD;
  exports.OSTYPEVALUES_HPUX = TMP_OSTYPEVALUES_HPUX;
  exports.OSTYPEVALUES_AIX = TMP_OSTYPEVALUES_AIX;
  exports.OSTYPEVALUES_SOLARIS = TMP_OSTYPEVALUES_SOLARIS;
  exports.OSTYPEVALUES_Z_OS = TMP_OSTYPEVALUES_Z_OS;
  exports.OsTypeValues = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_OSTYPEVALUES_WINDOWS,
    TMP_OSTYPEVALUES_LINUX,
    TMP_OSTYPEVALUES_DARWIN,
    TMP_OSTYPEVALUES_FREEBSD,
    TMP_OSTYPEVALUES_NETBSD,
    TMP_OSTYPEVALUES_OPENBSD,
    TMP_OSTYPEVALUES_DRAGONFLYBSD,
    TMP_OSTYPEVALUES_HPUX,
    TMP_OSTYPEVALUES_AIX,
    TMP_OSTYPEVALUES_SOLARIS,
    TMP_OSTYPEVALUES_Z_OS
  ]);
  var TMP_TELEMETRYSDKLANGUAGEVALUES_CPP = "cpp";
  var TMP_TELEMETRYSDKLANGUAGEVALUES_DOTNET = "dotnet";
  var TMP_TELEMETRYSDKLANGUAGEVALUES_ERLANG = "erlang";
  var TMP_TELEMETRYSDKLANGUAGEVALUES_GO = "go";
  var TMP_TELEMETRYSDKLANGUAGEVALUES_JAVA = "java";
  var TMP_TELEMETRYSDKLANGUAGEVALUES_NODEJS = "nodejs";
  var TMP_TELEMETRYSDKLANGUAGEVALUES_PHP = "php";
  var TMP_TELEMETRYSDKLANGUAGEVALUES_PYTHON = "python";
  var TMP_TELEMETRYSDKLANGUAGEVALUES_RUBY = "ruby";
  var TMP_TELEMETRYSDKLANGUAGEVALUES_WEBJS = "webjs";
  exports.TELEMETRYSDKLANGUAGEVALUES_CPP = TMP_TELEMETRYSDKLANGUAGEVALUES_CPP;
  exports.TELEMETRYSDKLANGUAGEVALUES_DOTNET = TMP_TELEMETRYSDKLANGUAGEVALUES_DOTNET;
  exports.TELEMETRYSDKLANGUAGEVALUES_ERLANG = TMP_TELEMETRYSDKLANGUAGEVALUES_ERLANG;
  exports.TELEMETRYSDKLANGUAGEVALUES_GO = TMP_TELEMETRYSDKLANGUAGEVALUES_GO;
  exports.TELEMETRYSDKLANGUAGEVALUES_JAVA = TMP_TELEMETRYSDKLANGUAGEVALUES_JAVA;
  exports.TELEMETRYSDKLANGUAGEVALUES_NODEJS = TMP_TELEMETRYSDKLANGUAGEVALUES_NODEJS;
  exports.TELEMETRYSDKLANGUAGEVALUES_PHP = TMP_TELEMETRYSDKLANGUAGEVALUES_PHP;
  exports.TELEMETRYSDKLANGUAGEVALUES_PYTHON = TMP_TELEMETRYSDKLANGUAGEVALUES_PYTHON;
  exports.TELEMETRYSDKLANGUAGEVALUES_RUBY = TMP_TELEMETRYSDKLANGUAGEVALUES_RUBY;
  exports.TELEMETRYSDKLANGUAGEVALUES_WEBJS = TMP_TELEMETRYSDKLANGUAGEVALUES_WEBJS;
  exports.TelemetrySdkLanguageValues = /* @__PURE__ */ (0, utils_1.createConstMap)([
    TMP_TELEMETRYSDKLANGUAGEVALUES_CPP,
    TMP_TELEMETRYSDKLANGUAGEVALUES_DOTNET,
    TMP_TELEMETRYSDKLANGUAGEVALUES_ERLANG,
    TMP_TELEMETRYSDKLANGUAGEVALUES_GO,
    TMP_TELEMETRYSDKLANGUAGEVALUES_JAVA,
    TMP_TELEMETRYSDKLANGUAGEVALUES_NODEJS,
    TMP_TELEMETRYSDKLANGUAGEVALUES_PHP,
    TMP_TELEMETRYSDKLANGUAGEVALUES_PYTHON,
    TMP_TELEMETRYSDKLANGUAGEVALUES_RUBY,
    TMP_TELEMETRYSDKLANGUAGEVALUES_WEBJS
  ]);
});

// node_modules/.bun/@opentelemetry+semantic-conventions@1.43.0/node_modules/@opentelemetry/semantic-conventions/build/src/resource/index.js
var require_resource = __commonJS(function(exports) {
  var __createBinding = exports && exports.__createBinding || (Object.create ? function(o, m, k, k2) {
    if (k2 === undefined)
      k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() {
        return m[k];
      } };
    }
    Object.defineProperty(o, k2, desc);
  } : function(o, m, k, k2) {
    if (k2 === undefined)
      k2 = k;
    o[k2] = m[k];
  });
  var __exportStar = exports && exports.__exportStar || function(m, exports2) {
    for (var p in m)
      if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports2, p))
        __createBinding(exports2, m, p);
  };
  Object.defineProperty(exports, "__esModule", { value: true });
  __exportStar(require_SemanticResourceAttributes(), exports);
});

// node_modules/.bun/@opentelemetry+semantic-conventions@1.43.0/node_modules/@opentelemetry/semantic-conventions/build/src/stable_attributes.js
var require_stable_attributes = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.DOTNET_GC_HEAP_GENERATION_VALUE_GEN1 = exports.DOTNET_GC_HEAP_GENERATION_VALUE_GEN0 = exports.ATTR_DOTNET_GC_HEAP_GENERATION = exports.DEPLOYMENT_ENVIRONMENT_NAME_VALUE_TEST = exports.DEPLOYMENT_ENVIRONMENT_NAME_VALUE_STAGING = exports.DEPLOYMENT_ENVIRONMENT_NAME_VALUE_PRODUCTION = exports.DEPLOYMENT_ENVIRONMENT_NAME_VALUE_DEVELOPMENT = exports.ATTR_DEPLOYMENT_ENVIRONMENT_NAME = exports.DB_SYSTEM_NAME_VALUE_POSTGRESQL = exports.DB_SYSTEM_NAME_VALUE_MYSQL = exports.DB_SYSTEM_NAME_VALUE_MICROSOFT_SQL_SERVER = exports.DB_SYSTEM_NAME_VALUE_MARIADB = exports.ATTR_DB_SYSTEM_NAME = exports.ATTR_DB_STORED_PROCEDURE_NAME = exports.ATTR_DB_RESPONSE_STATUS_CODE = exports.ATTR_DB_QUERY_TEXT = exports.ATTR_DB_QUERY_SUMMARY = exports.ATTR_DB_OPERATION_NAME = exports.ATTR_DB_OPERATION_BATCH_SIZE = exports.ATTR_DB_NAMESPACE = exports.ATTR_DB_COLLECTION_NAME = exports.ATTR_CONTAINER_IMAGE_TAGS = exports.ATTR_CONTAINER_IMAGE_REPO_DIGESTS = exports.ATTR_CONTAINER_IMAGE_NAME = exports.ATTR_CONTAINER_ID = exports.ATTR_CODE_STACKTRACE = exports.ATTR_CODE_LINE_NUMBER = exports.ATTR_CODE_FUNCTION_NAME = exports.ATTR_CODE_FILE_PATH = exports.ATTR_CODE_COLUMN_NUMBER = exports.ATTR_CLIENT_PORT = exports.ATTR_CLIENT_ADDRESS = exports.ATTR_ASPNETCORE_USER_IS_AUTHENTICATED = exports.ASPNETCORE_ROUTING_MATCH_STATUS_VALUE_SUCCESS = exports.ASPNETCORE_ROUTING_MATCH_STATUS_VALUE_FAILURE = exports.ATTR_ASPNETCORE_ROUTING_MATCH_STATUS = exports.ATTR_ASPNETCORE_ROUTING_IS_FALLBACK = exports.ATTR_ASPNETCORE_REQUEST_IS_UNHANDLED = exports.ASPNETCORE_RATE_LIMITING_RESULT_VALUE_REQUEST_CANCELED = exports.ASPNETCORE_RATE_LIMITING_RESULT_VALUE_GLOBAL_LIMITER = exports.ASPNETCORE_RATE_LIMITING_RESULT_VALUE_ENDPOINT_LIMITER = exports.ASPNETCORE_RATE_LIMITING_RESULT_VALUE_ACQUIRED = exports.ATTR_ASPNETCORE_RATE_LIMITING_RESULT = exports.ATTR_ASPNETCORE_RATE_LIMITING_POLICY = exports.ATTR_ASPNETCORE_DIAGNOSTICS_HANDLER_TYPE = exports.ASPNETCORE_DIAGNOSTICS_EXCEPTION_RESULT_VALUE_UNHANDLED = exports.ASPNETCORE_DIAGNOSTICS_EXCEPTION_RESULT_VALUE_SKIPPED = exports.ASPNETCORE_DIAGNOSTICS_EXCEPTION_RESULT_VALUE_HANDLED = exports.ASPNETCORE_DIAGNOSTICS_EXCEPTION_RESULT_VALUE_ABORTED = exports.ATTR_ASPNETCORE_DIAGNOSTICS_EXCEPTION_RESULT = undefined;
  exports.ATTR_K8S_DAEMONSET_LABEL = exports.ATTR_K8S_DAEMONSET_ANNOTATION = exports.ATTR_K8S_CRONJOB_UID = exports.ATTR_K8S_CRONJOB_NAME = exports.ATTR_K8S_CRONJOB_LABEL = exports.ATTR_K8S_CRONJOB_ANNOTATION = exports.ATTR_K8S_CONTAINER_RESTART_COUNT = exports.ATTR_K8S_CONTAINER_NAME = exports.ATTR_K8S_CLUSTER_UID = exports.ATTR_K8S_CLUSTER_NAME = exports.JVM_THREAD_STATE_VALUE_WAITING = exports.JVM_THREAD_STATE_VALUE_TIMED_WAITING = exports.JVM_THREAD_STATE_VALUE_TERMINATED = exports.JVM_THREAD_STATE_VALUE_RUNNABLE = exports.JVM_THREAD_STATE_VALUE_NEW = exports.JVM_THREAD_STATE_VALUE_BLOCKED = exports.ATTR_JVM_THREAD_STATE = exports.ATTR_JVM_THREAD_DAEMON = exports.JVM_MEMORY_TYPE_VALUE_NON_HEAP = exports.JVM_MEMORY_TYPE_VALUE_HEAP = exports.ATTR_JVM_MEMORY_TYPE = exports.ATTR_JVM_MEMORY_POOL_NAME = exports.ATTR_JVM_GC_NAME = exports.ATTR_JVM_GC_ACTION = exports.ATTR_HTTP_ROUTE = exports.ATTR_HTTP_RESPONSE_STATUS_CODE = exports.ATTR_HTTP_RESPONSE_HEADER = exports.ATTR_HTTP_REQUEST_RESEND_COUNT = exports.ATTR_HTTP_REQUEST_METHOD_ORIGINAL = exports.HTTP_REQUEST_METHOD_VALUE_TRACE = exports.HTTP_REQUEST_METHOD_VALUE_PUT = exports.HTTP_REQUEST_METHOD_VALUE_POST = exports.HTTP_REQUEST_METHOD_VALUE_PATCH = exports.HTTP_REQUEST_METHOD_VALUE_OPTIONS = exports.HTTP_REQUEST_METHOD_VALUE_HEAD = exports.HTTP_REQUEST_METHOD_VALUE_GET = exports.HTTP_REQUEST_METHOD_VALUE_DELETE = exports.HTTP_REQUEST_METHOD_VALUE_CONNECT = exports.HTTP_REQUEST_METHOD_VALUE_OTHER = exports.ATTR_HTTP_REQUEST_METHOD = exports.ATTR_HTTP_REQUEST_HEADER = exports.ATTR_EXCEPTION_TYPE = exports.ATTR_EXCEPTION_STACKTRACE = exports.ATTR_EXCEPTION_MESSAGE = exports.ATTR_EXCEPTION_ESCAPED = exports.ERROR_TYPE_VALUE_OTHER = exports.ATTR_ERROR_TYPE = exports.DOTNET_GC_HEAP_GENERATION_VALUE_POH = exports.DOTNET_GC_HEAP_GENERATION_VALUE_LOH = exports.DOTNET_GC_HEAP_GENERATION_VALUE_GEN2 = undefined;
  exports.ATTR_OTEL_SCOPE_VERSION = exports.ATTR_OTEL_SCOPE_NAME = exports.ATTR_OTEL_EVENT_NAME = exports.NETWORK_TYPE_VALUE_IPV6 = exports.NETWORK_TYPE_VALUE_IPV4 = exports.ATTR_NETWORK_TYPE = exports.NETWORK_TRANSPORT_VALUE_UNIX = exports.NETWORK_TRANSPORT_VALUE_UDP = exports.NETWORK_TRANSPORT_VALUE_TCP = exports.NETWORK_TRANSPORT_VALUE_QUIC = exports.NETWORK_TRANSPORT_VALUE_PIPE = exports.ATTR_NETWORK_TRANSPORT = exports.ATTR_NETWORK_PROTOCOL_VERSION = exports.ATTR_NETWORK_PROTOCOL_NAME = exports.ATTR_NETWORK_PEER_PORT = exports.ATTR_NETWORK_PEER_ADDRESS = exports.ATTR_NETWORK_LOCAL_PORT = exports.ATTR_NETWORK_LOCAL_ADDRESS = exports.ATTR_K8S_STATEFULSET_UID = exports.ATTR_K8S_STATEFULSET_NAME = exports.ATTR_K8S_STATEFULSET_LABEL = exports.ATTR_K8S_STATEFULSET_ANNOTATION = exports.ATTR_K8S_REPLICASET_UID = exports.ATTR_K8S_REPLICASET_NAME = exports.ATTR_K8S_REPLICASET_LABEL = exports.ATTR_K8S_REPLICASET_ANNOTATION = exports.ATTR_K8S_POD_UID = exports.ATTR_K8S_POD_START_TIME = exports.ATTR_K8S_POD_NAME = exports.ATTR_K8S_POD_LABEL = exports.ATTR_K8S_POD_IP = exports.ATTR_K8S_POD_HOSTNAME = exports.ATTR_K8S_POD_ANNOTATION = exports.ATTR_K8S_NODE_UID = exports.ATTR_K8S_NODE_NAME = exports.ATTR_K8S_NODE_LABEL = exports.ATTR_K8S_NODE_ANNOTATION = exports.ATTR_K8S_NAMESPACE_NAME = exports.ATTR_K8S_NAMESPACE_LABEL = exports.ATTR_K8S_NAMESPACE_ANNOTATION = exports.ATTR_K8S_JOB_UID = exports.ATTR_K8S_JOB_NAME = exports.ATTR_K8S_JOB_LABEL = exports.ATTR_K8S_JOB_ANNOTATION = exports.ATTR_K8S_DEPLOYMENT_UID = exports.ATTR_K8S_DEPLOYMENT_NAME = exports.ATTR_K8S_DEPLOYMENT_LABEL = exports.ATTR_K8S_DEPLOYMENT_ANNOTATION = exports.ATTR_K8S_DAEMONSET_UID = exports.ATTR_K8S_DAEMONSET_NAME = undefined;
  exports.ATTR_USER_AGENT_ORIGINAL = exports.ATTR_URL_SCHEME = exports.ATTR_URL_QUERY = exports.ATTR_URL_PATH = exports.ATTR_URL_FULL = exports.ATTR_URL_FRAGMENT = exports.ATTR_TELEMETRY_SDK_VERSION = exports.ATTR_TELEMETRY_SDK_NAME = exports.TELEMETRY_SDK_LANGUAGE_VALUE_WEBJS = exports.TELEMETRY_SDK_LANGUAGE_VALUE_SWIFT = exports.TELEMETRY_SDK_LANGUAGE_VALUE_RUST = exports.TELEMETRY_SDK_LANGUAGE_VALUE_RUBY = exports.TELEMETRY_SDK_LANGUAGE_VALUE_PYTHON = exports.TELEMETRY_SDK_LANGUAGE_VALUE_PHP = exports.TELEMETRY_SDK_LANGUAGE_VALUE_NODEJS = exports.TELEMETRY_SDK_LANGUAGE_VALUE_KOTLIN = exports.TELEMETRY_SDK_LANGUAGE_VALUE_JAVA = exports.TELEMETRY_SDK_LANGUAGE_VALUE_GO = exports.TELEMETRY_SDK_LANGUAGE_VALUE_ERLANG = exports.TELEMETRY_SDK_LANGUAGE_VALUE_DOTNET = exports.TELEMETRY_SDK_LANGUAGE_VALUE_CPP = exports.ATTR_TELEMETRY_SDK_LANGUAGE = exports.ATTR_TELEMETRY_DISTRO_VERSION = exports.ATTR_TELEMETRY_DISTRO_NAME = exports.SIGNALR_TRANSPORT_VALUE_WEB_SOCKETS = exports.SIGNALR_TRANSPORT_VALUE_SERVER_SENT_EVENTS = exports.SIGNALR_TRANSPORT_VALUE_LONG_POLLING = exports.ATTR_SIGNALR_TRANSPORT = exports.SIGNALR_CONNECTION_STATUS_VALUE_TIMEOUT = exports.SIGNALR_CONNECTION_STATUS_VALUE_NORMAL_CLOSURE = exports.SIGNALR_CONNECTION_STATUS_VALUE_APP_SHUTDOWN = exports.ATTR_SIGNALR_CONNECTION_STATUS = exports.ATTR_SERVICE_VERSION = exports.ATTR_SERVICE_NAMESPACE = exports.ATTR_SERVICE_NAME = exports.ATTR_SERVICE_INSTANCE_ID = exports.ATTR_SERVER_PORT = exports.ATTR_SERVER_ADDRESS = exports.ATTR_OTEL_STATUS_DESCRIPTION = exports.OTEL_STATUS_CODE_VALUE_OK = exports.OTEL_STATUS_CODE_VALUE_ERROR = exports.ATTR_OTEL_STATUS_CODE = undefined;
  exports.ATTR_ASPNETCORE_DIAGNOSTICS_EXCEPTION_RESULT = "aspnetcore.diagnostics.exception.result";
  exports.ASPNETCORE_DIAGNOSTICS_EXCEPTION_RESULT_VALUE_ABORTED = "aborted";
  exports.ASPNETCORE_DIAGNOSTICS_EXCEPTION_RESULT_VALUE_HANDLED = "handled";
  exports.ASPNETCORE_DIAGNOSTICS_EXCEPTION_RESULT_VALUE_SKIPPED = "skipped";
  exports.ASPNETCORE_DIAGNOSTICS_EXCEPTION_RESULT_VALUE_UNHANDLED = "unhandled";
  exports.ATTR_ASPNETCORE_DIAGNOSTICS_HANDLER_TYPE = "aspnetcore.diagnostics.handler.type";
  exports.ATTR_ASPNETCORE_RATE_LIMITING_POLICY = "aspnetcore.rate_limiting.policy";
  exports.ATTR_ASPNETCORE_RATE_LIMITING_RESULT = "aspnetcore.rate_limiting.result";
  exports.ASPNETCORE_RATE_LIMITING_RESULT_VALUE_ACQUIRED = "acquired";
  exports.ASPNETCORE_RATE_LIMITING_RESULT_VALUE_ENDPOINT_LIMITER = "endpoint_limiter";
  exports.ASPNETCORE_RATE_LIMITING_RESULT_VALUE_GLOBAL_LIMITER = "global_limiter";
  exports.ASPNETCORE_RATE_LIMITING_RESULT_VALUE_REQUEST_CANCELED = "request_canceled";
  exports.ATTR_ASPNETCORE_REQUEST_IS_UNHANDLED = "aspnetcore.request.is_unhandled";
  exports.ATTR_ASPNETCORE_ROUTING_IS_FALLBACK = "aspnetcore.routing.is_fallback";
  exports.ATTR_ASPNETCORE_ROUTING_MATCH_STATUS = "aspnetcore.routing.match_status";
  exports.ASPNETCORE_ROUTING_MATCH_STATUS_VALUE_FAILURE = "failure";
  exports.ASPNETCORE_ROUTING_MATCH_STATUS_VALUE_SUCCESS = "success";
  exports.ATTR_ASPNETCORE_USER_IS_AUTHENTICATED = "aspnetcore.user.is_authenticated";
  exports.ATTR_CLIENT_ADDRESS = "client.address";
  exports.ATTR_CLIENT_PORT = "client.port";
  exports.ATTR_CODE_COLUMN_NUMBER = "code.column.number";
  exports.ATTR_CODE_FILE_PATH = "code.file.path";
  exports.ATTR_CODE_FUNCTION_NAME = "code.function.name";
  exports.ATTR_CODE_LINE_NUMBER = "code.line.number";
  exports.ATTR_CODE_STACKTRACE = "code.stacktrace";
  exports.ATTR_CONTAINER_ID = "container.id";
  exports.ATTR_CONTAINER_IMAGE_NAME = "container.image.name";
  exports.ATTR_CONTAINER_IMAGE_REPO_DIGESTS = "container.image.repo_digests";
  exports.ATTR_CONTAINER_IMAGE_TAGS = "container.image.tags";
  exports.ATTR_DB_COLLECTION_NAME = "db.collection.name";
  exports.ATTR_DB_NAMESPACE = "db.namespace";
  exports.ATTR_DB_OPERATION_BATCH_SIZE = "db.operation.batch.size";
  exports.ATTR_DB_OPERATION_NAME = "db.operation.name";
  exports.ATTR_DB_QUERY_SUMMARY = "db.query.summary";
  exports.ATTR_DB_QUERY_TEXT = "db.query.text";
  exports.ATTR_DB_RESPONSE_STATUS_CODE = "db.response.status_code";
  exports.ATTR_DB_STORED_PROCEDURE_NAME = "db.stored_procedure.name";
  exports.ATTR_DB_SYSTEM_NAME = "db.system.name";
  exports.DB_SYSTEM_NAME_VALUE_MARIADB = "mariadb";
  exports.DB_SYSTEM_NAME_VALUE_MICROSOFT_SQL_SERVER = "microsoft.sql_server";
  exports.DB_SYSTEM_NAME_VALUE_MYSQL = "mysql";
  exports.DB_SYSTEM_NAME_VALUE_POSTGRESQL = "postgresql";
  exports.ATTR_DEPLOYMENT_ENVIRONMENT_NAME = "deployment.environment.name";
  exports.DEPLOYMENT_ENVIRONMENT_NAME_VALUE_DEVELOPMENT = "development";
  exports.DEPLOYMENT_ENVIRONMENT_NAME_VALUE_PRODUCTION = "production";
  exports.DEPLOYMENT_ENVIRONMENT_NAME_VALUE_STAGING = "staging";
  exports.DEPLOYMENT_ENVIRONMENT_NAME_VALUE_TEST = "test";
  exports.ATTR_DOTNET_GC_HEAP_GENERATION = "dotnet.gc.heap.generation";
  exports.DOTNET_GC_HEAP_GENERATION_VALUE_GEN0 = "gen0";
  exports.DOTNET_GC_HEAP_GENERATION_VALUE_GEN1 = "gen1";
  exports.DOTNET_GC_HEAP_GENERATION_VALUE_GEN2 = "gen2";
  exports.DOTNET_GC_HEAP_GENERATION_VALUE_LOH = "loh";
  exports.DOTNET_GC_HEAP_GENERATION_VALUE_POH = "poh";
  exports.ATTR_ERROR_TYPE = "error.type";
  exports.ERROR_TYPE_VALUE_OTHER = "_OTHER";
  exports.ATTR_EXCEPTION_ESCAPED = "exception.escaped";
  exports.ATTR_EXCEPTION_MESSAGE = "exception.message";
  exports.ATTR_EXCEPTION_STACKTRACE = "exception.stacktrace";
  exports.ATTR_EXCEPTION_TYPE = "exception.type";
  var ATTR_HTTP_REQUEST_HEADER = (key) => `http.request.header.${key}`;
  exports.ATTR_HTTP_REQUEST_HEADER = ATTR_HTTP_REQUEST_HEADER;
  exports.ATTR_HTTP_REQUEST_METHOD = "http.request.method";
  exports.HTTP_REQUEST_METHOD_VALUE_OTHER = "_OTHER";
  exports.HTTP_REQUEST_METHOD_VALUE_CONNECT = "CONNECT";
  exports.HTTP_REQUEST_METHOD_VALUE_DELETE = "DELETE";
  exports.HTTP_REQUEST_METHOD_VALUE_GET = "GET";
  exports.HTTP_REQUEST_METHOD_VALUE_HEAD = "HEAD";
  exports.HTTP_REQUEST_METHOD_VALUE_OPTIONS = "OPTIONS";
  exports.HTTP_REQUEST_METHOD_VALUE_PATCH = "PATCH";
  exports.HTTP_REQUEST_METHOD_VALUE_POST = "POST";
  exports.HTTP_REQUEST_METHOD_VALUE_PUT = "PUT";
  exports.HTTP_REQUEST_METHOD_VALUE_TRACE = "TRACE";
  exports.ATTR_HTTP_REQUEST_METHOD_ORIGINAL = "http.request.method_original";
  exports.ATTR_HTTP_REQUEST_RESEND_COUNT = "http.request.resend_count";
  var ATTR_HTTP_RESPONSE_HEADER = (key) => `http.response.header.${key}`;
  exports.ATTR_HTTP_RESPONSE_HEADER = ATTR_HTTP_RESPONSE_HEADER;
  exports.ATTR_HTTP_RESPONSE_STATUS_CODE = "http.response.status_code";
  exports.ATTR_HTTP_ROUTE = "http.route";
  exports.ATTR_JVM_GC_ACTION = "jvm.gc.action";
  exports.ATTR_JVM_GC_NAME = "jvm.gc.name";
  exports.ATTR_JVM_MEMORY_POOL_NAME = "jvm.memory.pool.name";
  exports.ATTR_JVM_MEMORY_TYPE = "jvm.memory.type";
  exports.JVM_MEMORY_TYPE_VALUE_HEAP = "heap";
  exports.JVM_MEMORY_TYPE_VALUE_NON_HEAP = "non_heap";
  exports.ATTR_JVM_THREAD_DAEMON = "jvm.thread.daemon";
  exports.ATTR_JVM_THREAD_STATE = "jvm.thread.state";
  exports.JVM_THREAD_STATE_VALUE_BLOCKED = "blocked";
  exports.JVM_THREAD_STATE_VALUE_NEW = "new";
  exports.JVM_THREAD_STATE_VALUE_RUNNABLE = "runnable";
  exports.JVM_THREAD_STATE_VALUE_TERMINATED = "terminated";
  exports.JVM_THREAD_STATE_VALUE_TIMED_WAITING = "timed_waiting";
  exports.JVM_THREAD_STATE_VALUE_WAITING = "waiting";
  exports.ATTR_K8S_CLUSTER_NAME = "k8s.cluster.name";
  exports.ATTR_K8S_CLUSTER_UID = "k8s.cluster.uid";
  exports.ATTR_K8S_CONTAINER_NAME = "k8s.container.name";
  exports.ATTR_K8S_CONTAINER_RESTART_COUNT = "k8s.container.restart_count";
  var ATTR_K8S_CRONJOB_ANNOTATION = (key) => `k8s.cronjob.annotation.${key}`;
  exports.ATTR_K8S_CRONJOB_ANNOTATION = ATTR_K8S_CRONJOB_ANNOTATION;
  var ATTR_K8S_CRONJOB_LABEL = (key) => `k8s.cronjob.label.${key}`;
  exports.ATTR_K8S_CRONJOB_LABEL = ATTR_K8S_CRONJOB_LABEL;
  exports.ATTR_K8S_CRONJOB_NAME = "k8s.cronjob.name";
  exports.ATTR_K8S_CRONJOB_UID = "k8s.cronjob.uid";
  var ATTR_K8S_DAEMONSET_ANNOTATION = (key) => `k8s.daemonset.annotation.${key}`;
  exports.ATTR_K8S_DAEMONSET_ANNOTATION = ATTR_K8S_DAEMONSET_ANNOTATION;
  var ATTR_K8S_DAEMONSET_LABEL = (key) => `k8s.daemonset.label.${key}`;
  exports.ATTR_K8S_DAEMONSET_LABEL = ATTR_K8S_DAEMONSET_LABEL;
  exports.ATTR_K8S_DAEMONSET_NAME = "k8s.daemonset.name";
  exports.ATTR_K8S_DAEMONSET_UID = "k8s.daemonset.uid";
  var ATTR_K8S_DEPLOYMENT_ANNOTATION = (key) => `k8s.deployment.annotation.${key}`;
  exports.ATTR_K8S_DEPLOYMENT_ANNOTATION = ATTR_K8S_DEPLOYMENT_ANNOTATION;
  var ATTR_K8S_DEPLOYMENT_LABEL = (key) => `k8s.deployment.label.${key}`;
  exports.ATTR_K8S_DEPLOYMENT_LABEL = ATTR_K8S_DEPLOYMENT_LABEL;
  exports.ATTR_K8S_DEPLOYMENT_NAME = "k8s.deployment.name";
  exports.ATTR_K8S_DEPLOYMENT_UID = "k8s.deployment.uid";
  var ATTR_K8S_JOB_ANNOTATION = (key) => `k8s.job.annotation.${key}`;
  exports.ATTR_K8S_JOB_ANNOTATION = ATTR_K8S_JOB_ANNOTATION;
  var ATTR_K8S_JOB_LABEL = (key) => `k8s.job.label.${key}`;
  exports.ATTR_K8S_JOB_LABEL = ATTR_K8S_JOB_LABEL;
  exports.ATTR_K8S_JOB_NAME = "k8s.job.name";
  exports.ATTR_K8S_JOB_UID = "k8s.job.uid";
  var ATTR_K8S_NAMESPACE_ANNOTATION = (key) => `k8s.namespace.annotation.${key}`;
  exports.ATTR_K8S_NAMESPACE_ANNOTATION = ATTR_K8S_NAMESPACE_ANNOTATION;
  var ATTR_K8S_NAMESPACE_LABEL = (key) => `k8s.namespace.label.${key}`;
  exports.ATTR_K8S_NAMESPACE_LABEL = ATTR_K8S_NAMESPACE_LABEL;
  exports.ATTR_K8S_NAMESPACE_NAME = "k8s.namespace.name";
  var ATTR_K8S_NODE_ANNOTATION = (key) => `k8s.node.annotation.${key}`;
  exports.ATTR_K8S_NODE_ANNOTATION = ATTR_K8S_NODE_ANNOTATION;
  var ATTR_K8S_NODE_LABEL = (key) => `k8s.node.label.${key}`;
  exports.ATTR_K8S_NODE_LABEL = ATTR_K8S_NODE_LABEL;
  exports.ATTR_K8S_NODE_NAME = "k8s.node.name";
  exports.ATTR_K8S_NODE_UID = "k8s.node.uid";
  var ATTR_K8S_POD_ANNOTATION = (key) => `k8s.pod.annotation.${key}`;
  exports.ATTR_K8S_POD_ANNOTATION = ATTR_K8S_POD_ANNOTATION;
  exports.ATTR_K8S_POD_HOSTNAME = "k8s.pod.hostname";
  exports.ATTR_K8S_POD_IP = "k8s.pod.ip";
  var ATTR_K8S_POD_LABEL = (key) => `k8s.pod.label.${key}`;
  exports.ATTR_K8S_POD_LABEL = ATTR_K8S_POD_LABEL;
  exports.ATTR_K8S_POD_NAME = "k8s.pod.name";
  exports.ATTR_K8S_POD_START_TIME = "k8s.pod.start_time";
  exports.ATTR_K8S_POD_UID = "k8s.pod.uid";
  var ATTR_K8S_REPLICASET_ANNOTATION = (key) => `k8s.replicaset.annotation.${key}`;
  exports.ATTR_K8S_REPLICASET_ANNOTATION = ATTR_K8S_REPLICASET_ANNOTATION;
  var ATTR_K8S_REPLICASET_LABEL = (key) => `k8s.replicaset.label.${key}`;
  exports.ATTR_K8S_REPLICASET_LABEL = ATTR_K8S_REPLICASET_LABEL;
  exports.ATTR_K8S_REPLICASET_NAME = "k8s.replicaset.name";
  exports.ATTR_K8S_REPLICASET_UID = "k8s.replicaset.uid";
  var ATTR_K8S_STATEFULSET_ANNOTATION = (key) => `k8s.statefulset.annotation.${key}`;
  exports.ATTR_K8S_STATEFULSET_ANNOTATION = ATTR_K8S_STATEFULSET_ANNOTATION;
  var ATTR_K8S_STATEFULSET_LABEL = (key) => `k8s.statefulset.label.${key}`;
  exports.ATTR_K8S_STATEFULSET_LABEL = ATTR_K8S_STATEFULSET_LABEL;
  exports.ATTR_K8S_STATEFULSET_NAME = "k8s.statefulset.name";
  exports.ATTR_K8S_STATEFULSET_UID = "k8s.statefulset.uid";
  exports.ATTR_NETWORK_LOCAL_ADDRESS = "network.local.address";
  exports.ATTR_NETWORK_LOCAL_PORT = "network.local.port";
  exports.ATTR_NETWORK_PEER_ADDRESS = "network.peer.address";
  exports.ATTR_NETWORK_PEER_PORT = "network.peer.port";
  exports.ATTR_NETWORK_PROTOCOL_NAME = "network.protocol.name";
  exports.ATTR_NETWORK_PROTOCOL_VERSION = "network.protocol.version";
  exports.ATTR_NETWORK_TRANSPORT = "network.transport";
  exports.NETWORK_TRANSPORT_VALUE_PIPE = "pipe";
  exports.NETWORK_TRANSPORT_VALUE_QUIC = "quic";
  exports.NETWORK_TRANSPORT_VALUE_TCP = "tcp";
  exports.NETWORK_TRANSPORT_VALUE_UDP = "udp";
  exports.NETWORK_TRANSPORT_VALUE_UNIX = "unix";
  exports.ATTR_NETWORK_TYPE = "network.type";
  exports.NETWORK_TYPE_VALUE_IPV4 = "ipv4";
  exports.NETWORK_TYPE_VALUE_IPV6 = "ipv6";
  exports.ATTR_OTEL_EVENT_NAME = "otel.event.name";
  exports.ATTR_OTEL_SCOPE_NAME = "otel.scope.name";
  exports.ATTR_OTEL_SCOPE_VERSION = "otel.scope.version";
  exports.ATTR_OTEL_STATUS_CODE = "otel.status_code";
  exports.OTEL_STATUS_CODE_VALUE_ERROR = "ERROR";
  exports.OTEL_STATUS_CODE_VALUE_OK = "OK";
  exports.ATTR_OTEL_STATUS_DESCRIPTION = "otel.status_description";
  exports.ATTR_SERVER_ADDRESS = "server.address";
  exports.ATTR_SERVER_PORT = "server.port";
  exports.ATTR_SERVICE_INSTANCE_ID = "service.instance.id";
  exports.ATTR_SERVICE_NAME = "service.name";
  exports.ATTR_SERVICE_NAMESPACE = "service.namespace";
  exports.ATTR_SERVICE_VERSION = "service.version";
  exports.ATTR_SIGNALR_CONNECTION_STATUS = "signalr.connection.status";
  exports.SIGNALR_CONNECTION_STATUS_VALUE_APP_SHUTDOWN = "app_shutdown";
  exports.SIGNALR_CONNECTION_STATUS_VALUE_NORMAL_CLOSURE = "normal_closure";
  exports.SIGNALR_CONNECTION_STATUS_VALUE_TIMEOUT = "timeout";
  exports.ATTR_SIGNALR_TRANSPORT = "signalr.transport";
  exports.SIGNALR_TRANSPORT_VALUE_LONG_POLLING = "long_polling";
  exports.SIGNALR_TRANSPORT_VALUE_SERVER_SENT_EVENTS = "server_sent_events";
  exports.SIGNALR_TRANSPORT_VALUE_WEB_SOCKETS = "web_sockets";
  exports.ATTR_TELEMETRY_DISTRO_NAME = "telemetry.distro.name";
  exports.ATTR_TELEMETRY_DISTRO_VERSION = "telemetry.distro.version";
  exports.ATTR_TELEMETRY_SDK_LANGUAGE = "telemetry.sdk.language";
  exports.TELEMETRY_SDK_LANGUAGE_VALUE_CPP = "cpp";
  exports.TELEMETRY_SDK_LANGUAGE_VALUE_DOTNET = "dotnet";
  exports.TELEMETRY_SDK_LANGUAGE_VALUE_ERLANG = "erlang";
  exports.TELEMETRY_SDK_LANGUAGE_VALUE_GO = "go";
  exports.TELEMETRY_SDK_LANGUAGE_VALUE_JAVA = "java";
  exports.TELEMETRY_SDK_LANGUAGE_VALUE_KOTLIN = "kotlin";
  exports.TELEMETRY_SDK_LANGUAGE_VALUE_NODEJS = "nodejs";
  exports.TELEMETRY_SDK_LANGUAGE_VALUE_PHP = "php";
  exports.TELEMETRY_SDK_LANGUAGE_VALUE_PYTHON = "python";
  exports.TELEMETRY_SDK_LANGUAGE_VALUE_RUBY = "ruby";
  exports.TELEMETRY_SDK_LANGUAGE_VALUE_RUST = "rust";
  exports.TELEMETRY_SDK_LANGUAGE_VALUE_SWIFT = "swift";
  exports.TELEMETRY_SDK_LANGUAGE_VALUE_WEBJS = "webjs";
  exports.ATTR_TELEMETRY_SDK_NAME = "telemetry.sdk.name";
  exports.ATTR_TELEMETRY_SDK_VERSION = "telemetry.sdk.version";
  exports.ATTR_URL_FRAGMENT = "url.fragment";
  exports.ATTR_URL_FULL = "url.full";
  exports.ATTR_URL_PATH = "url.path";
  exports.ATTR_URL_QUERY = "url.query";
  exports.ATTR_URL_SCHEME = "url.scheme";
  exports.ATTR_USER_AGENT_ORIGINAL = "user_agent.original";
});

// node_modules/.bun/@opentelemetry+semantic-conventions@1.43.0/node_modules/@opentelemetry/semantic-conventions/build/src/stable_metrics.js
var require_stable_metrics = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.METRIC_SIGNALR_SERVER_ACTIVE_CONNECTIONS = exports.METRIC_KESTREL_UPGRADED_CONNECTIONS = exports.METRIC_KESTREL_TLS_HANDSHAKE_DURATION = exports.METRIC_KESTREL_REJECTED_CONNECTIONS = exports.METRIC_KESTREL_QUEUED_REQUESTS = exports.METRIC_KESTREL_QUEUED_CONNECTIONS = exports.METRIC_KESTREL_CONNECTION_DURATION = exports.METRIC_KESTREL_ACTIVE_TLS_HANDSHAKES = exports.METRIC_KESTREL_ACTIVE_CONNECTIONS = exports.METRIC_JVM_THREAD_COUNT = exports.METRIC_JVM_MEMORY_USED_AFTER_LAST_GC = exports.METRIC_JVM_MEMORY_USED = exports.METRIC_JVM_MEMORY_LIMIT = exports.METRIC_JVM_MEMORY_COMMITTED = exports.METRIC_JVM_GC_DURATION = exports.METRIC_JVM_CPU_TIME = exports.METRIC_JVM_CPU_RECENT_UTILIZATION = exports.METRIC_JVM_CPU_COUNT = exports.METRIC_JVM_CLASS_UNLOADED = exports.METRIC_JVM_CLASS_LOADED = exports.METRIC_JVM_CLASS_COUNT = exports.METRIC_HTTP_SERVER_REQUEST_DURATION = exports.METRIC_HTTP_CLIENT_REQUEST_DURATION = exports.METRIC_DOTNET_TIMER_COUNT = exports.METRIC_DOTNET_THREAD_POOL_WORK_ITEM_COUNT = exports.METRIC_DOTNET_THREAD_POOL_THREAD_COUNT = exports.METRIC_DOTNET_THREAD_POOL_QUEUE_LENGTH = exports.METRIC_DOTNET_PROCESS_MEMORY_WORKING_SET = exports.METRIC_DOTNET_PROCESS_CPU_TIME = exports.METRIC_DOTNET_PROCESS_CPU_COUNT = exports.METRIC_DOTNET_MONITOR_LOCK_CONTENTIONS = exports.METRIC_DOTNET_JIT_COMPILED_METHODS = exports.METRIC_DOTNET_JIT_COMPILED_IL_SIZE = exports.METRIC_DOTNET_JIT_COMPILATION_TIME = exports.METRIC_DOTNET_GC_PAUSE_TIME = exports.METRIC_DOTNET_GC_LAST_COLLECTION_MEMORY_COMMITTED_SIZE = exports.METRIC_DOTNET_GC_LAST_COLLECTION_HEAP_SIZE = exports.METRIC_DOTNET_GC_LAST_COLLECTION_HEAP_FRAGMENTATION_SIZE = exports.METRIC_DOTNET_GC_HEAP_TOTAL_ALLOCATED = exports.METRIC_DOTNET_GC_COLLECTIONS = exports.METRIC_DOTNET_EXCEPTIONS = exports.METRIC_DOTNET_ASSEMBLY_COUNT = exports.METRIC_DB_CLIENT_OPERATION_DURATION = exports.METRIC_ASPNETCORE_ROUTING_MATCH_ATTEMPTS = exports.METRIC_ASPNETCORE_RATE_LIMITING_REQUESTS = exports.METRIC_ASPNETCORE_RATE_LIMITING_REQUEST_LEASE_DURATION = exports.METRIC_ASPNETCORE_RATE_LIMITING_REQUEST_TIME_IN_QUEUE = exports.METRIC_ASPNETCORE_RATE_LIMITING_QUEUED_REQUESTS = exports.METRIC_ASPNETCORE_RATE_LIMITING_ACTIVE_REQUEST_LEASES = exports.METRIC_ASPNETCORE_DIAGNOSTICS_EXCEPTIONS = undefined;
  exports.METRIC_SIGNALR_SERVER_CONNECTION_DURATION = undefined;
  exports.METRIC_ASPNETCORE_DIAGNOSTICS_EXCEPTIONS = "aspnetcore.diagnostics.exceptions";
  exports.METRIC_ASPNETCORE_RATE_LIMITING_ACTIVE_REQUEST_LEASES = "aspnetcore.rate_limiting.active_request_leases";
  exports.METRIC_ASPNETCORE_RATE_LIMITING_QUEUED_REQUESTS = "aspnetcore.rate_limiting.queued_requests";
  exports.METRIC_ASPNETCORE_RATE_LIMITING_REQUEST_TIME_IN_QUEUE = "aspnetcore.rate_limiting.request.time_in_queue";
  exports.METRIC_ASPNETCORE_RATE_LIMITING_REQUEST_LEASE_DURATION = "aspnetcore.rate_limiting.request_lease.duration";
  exports.METRIC_ASPNETCORE_RATE_LIMITING_REQUESTS = "aspnetcore.rate_limiting.requests";
  exports.METRIC_ASPNETCORE_ROUTING_MATCH_ATTEMPTS = "aspnetcore.routing.match_attempts";
  exports.METRIC_DB_CLIENT_OPERATION_DURATION = "db.client.operation.duration";
  exports.METRIC_DOTNET_ASSEMBLY_COUNT = "dotnet.assembly.count";
  exports.METRIC_DOTNET_EXCEPTIONS = "dotnet.exceptions";
  exports.METRIC_DOTNET_GC_COLLECTIONS = "dotnet.gc.collections";
  exports.METRIC_DOTNET_GC_HEAP_TOTAL_ALLOCATED = "dotnet.gc.heap.total_allocated";
  exports.METRIC_DOTNET_GC_LAST_COLLECTION_HEAP_FRAGMENTATION_SIZE = "dotnet.gc.last_collection.heap.fragmentation.size";
  exports.METRIC_DOTNET_GC_LAST_COLLECTION_HEAP_SIZE = "dotnet.gc.last_collection.heap.size";
  exports.METRIC_DOTNET_GC_LAST_COLLECTION_MEMORY_COMMITTED_SIZE = "dotnet.gc.last_collection.memory.committed_size";
  exports.METRIC_DOTNET_GC_PAUSE_TIME = "dotnet.gc.pause.time";
  exports.METRIC_DOTNET_JIT_COMPILATION_TIME = "dotnet.jit.compilation.time";
  exports.METRIC_DOTNET_JIT_COMPILED_IL_SIZE = "dotnet.jit.compiled_il.size";
  exports.METRIC_DOTNET_JIT_COMPILED_METHODS = "dotnet.jit.compiled_methods";
  exports.METRIC_DOTNET_MONITOR_LOCK_CONTENTIONS = "dotnet.monitor.lock_contentions";
  exports.METRIC_DOTNET_PROCESS_CPU_COUNT = "dotnet.process.cpu.count";
  exports.METRIC_DOTNET_PROCESS_CPU_TIME = "dotnet.process.cpu.time";
  exports.METRIC_DOTNET_PROCESS_MEMORY_WORKING_SET = "dotnet.process.memory.working_set";
  exports.METRIC_DOTNET_THREAD_POOL_QUEUE_LENGTH = "dotnet.thread_pool.queue.length";
  exports.METRIC_DOTNET_THREAD_POOL_THREAD_COUNT = "dotnet.thread_pool.thread.count";
  exports.METRIC_DOTNET_THREAD_POOL_WORK_ITEM_COUNT = "dotnet.thread_pool.work_item.count";
  exports.METRIC_DOTNET_TIMER_COUNT = "dotnet.timer.count";
  exports.METRIC_HTTP_CLIENT_REQUEST_DURATION = "http.client.request.duration";
  exports.METRIC_HTTP_SERVER_REQUEST_DURATION = "http.server.request.duration";
  exports.METRIC_JVM_CLASS_COUNT = "jvm.class.count";
  exports.METRIC_JVM_CLASS_LOADED = "jvm.class.loaded";
  exports.METRIC_JVM_CLASS_UNLOADED = "jvm.class.unloaded";
  exports.METRIC_JVM_CPU_COUNT = "jvm.cpu.count";
  exports.METRIC_JVM_CPU_RECENT_UTILIZATION = "jvm.cpu.recent_utilization";
  exports.METRIC_JVM_CPU_TIME = "jvm.cpu.time";
  exports.METRIC_JVM_GC_DURATION = "jvm.gc.duration";
  exports.METRIC_JVM_MEMORY_COMMITTED = "jvm.memory.committed";
  exports.METRIC_JVM_MEMORY_LIMIT = "jvm.memory.limit";
  exports.METRIC_JVM_MEMORY_USED = "jvm.memory.used";
  exports.METRIC_JVM_MEMORY_USED_AFTER_LAST_GC = "jvm.memory.used_after_last_gc";
  exports.METRIC_JVM_THREAD_COUNT = "jvm.thread.count";
  exports.METRIC_KESTREL_ACTIVE_CONNECTIONS = "kestrel.active_connections";
  exports.METRIC_KESTREL_ACTIVE_TLS_HANDSHAKES = "kestrel.active_tls_handshakes";
  exports.METRIC_KESTREL_CONNECTION_DURATION = "kestrel.connection.duration";
  exports.METRIC_KESTREL_QUEUED_CONNECTIONS = "kestrel.queued_connections";
  exports.METRIC_KESTREL_QUEUED_REQUESTS = "kestrel.queued_requests";
  exports.METRIC_KESTREL_REJECTED_CONNECTIONS = "kestrel.rejected_connections";
  exports.METRIC_KESTREL_TLS_HANDSHAKE_DURATION = "kestrel.tls_handshake.duration";
  exports.METRIC_KESTREL_UPGRADED_CONNECTIONS = "kestrel.upgraded_connections";
  exports.METRIC_SIGNALR_SERVER_ACTIVE_CONNECTIONS = "signalr.server.active_connections";
  exports.METRIC_SIGNALR_SERVER_CONNECTION_DURATION = "signalr.server.connection.duration";
});

// node_modules/.bun/@opentelemetry+semantic-conventions@1.43.0/node_modules/@opentelemetry/semantic-conventions/build/src/stable_events.js
var require_stable_events = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.EVENT_EXCEPTION = undefined;
  exports.EVENT_EXCEPTION = "exception";
});

// node_modules/.bun/@opentelemetry+semantic-conventions@1.43.0/node_modules/@opentelemetry/semantic-conventions/build/src/index.js
var require_src4 = __commonJS(function(exports) {
  var __createBinding = exports && exports.__createBinding || (Object.create ? function(o, m, k, k2) {
    if (k2 === undefined)
      k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() {
        return m[k];
      } };
    }
    Object.defineProperty(o, k2, desc);
  } : function(o, m, k, k2) {
    if (k2 === undefined)
      k2 = k;
    o[k2] = m[k];
  });
  var __exportStar = exports && exports.__exportStar || function(m, exports2) {
    for (var p in m)
      if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports2, p))
        __createBinding(exports2, m, p);
  };
  Object.defineProperty(exports, "__esModule", { value: true });
  __exportStar(require_trace(), exports);
  __exportStar(require_resource(), exports);
  __exportStar(require_stable_attributes(), exports);
  __exportStar(require_stable_metrics(), exports);
  __exportStar(require_stable_events(), exports);
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/semconv.js
var require_semconv = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ATTR_PROCESS_RUNTIME_NAME = undefined;
  exports.ATTR_PROCESS_RUNTIME_NAME = "process.runtime.name";
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/platform/node/sdk-info.js
var require_sdk_info = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.SDK_INFO = undefined;
  var version_1 = require_version();
  var semantic_conventions_1 = require_src4();
  var semconv_1 = require_semconv();
  exports.SDK_INFO = {
    [semantic_conventions_1.ATTR_TELEMETRY_SDK_NAME]: "opentelemetry",
    [semconv_1.ATTR_PROCESS_RUNTIME_NAME]: "node",
    [semantic_conventions_1.ATTR_TELEMETRY_SDK_LANGUAGE]: semantic_conventions_1.TELEMETRY_SDK_LANGUAGE_VALUE_NODEJS,
    [semantic_conventions_1.ATTR_TELEMETRY_SDK_VERSION]: version_1.VERSION
  };
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/platform/node/index.js
var require_node = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.otperformance = exports.SDK_INFO = exports._globalThis = exports.getStringListFromEnv = exports.getNumberFromEnv = exports.getBooleanFromEnv = exports.getStringFromEnv = undefined;
  var environment_1 = require_environment();
  Object.defineProperty(exports, "getStringFromEnv", { enumerable: true, get: function() {
    return environment_1.getStringFromEnv;
  } });
  Object.defineProperty(exports, "getBooleanFromEnv", { enumerable: true, get: function() {
    return environment_1.getBooleanFromEnv;
  } });
  Object.defineProperty(exports, "getNumberFromEnv", { enumerable: true, get: function() {
    return environment_1.getNumberFromEnv;
  } });
  Object.defineProperty(exports, "getStringListFromEnv", { enumerable: true, get: function() {
    return environment_1.getStringListFromEnv;
  } });
  var globalThis_1 = require_globalThis();
  Object.defineProperty(exports, "_globalThis", { enumerable: true, get: function() {
    return globalThis_1._globalThis;
  } });
  var sdk_info_1 = require_sdk_info();
  Object.defineProperty(exports, "SDK_INFO", { enumerable: true, get: function() {
    return sdk_info_1.SDK_INFO;
  } });
  exports.otperformance = performance;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/platform/index.js
var require_platform = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.getStringListFromEnv = exports.getNumberFromEnv = exports.getStringFromEnv = exports.getBooleanFromEnv = exports.otperformance = exports._globalThis = exports.SDK_INFO = undefined;
  var node_1 = require_node();
  Object.defineProperty(exports, "SDK_INFO", { enumerable: true, get: function() {
    return node_1.SDK_INFO;
  } });
  Object.defineProperty(exports, "_globalThis", { enumerable: true, get: function() {
    return node_1._globalThis;
  } });
  Object.defineProperty(exports, "otperformance", { enumerable: true, get: function() {
    return node_1.otperformance;
  } });
  Object.defineProperty(exports, "getBooleanFromEnv", { enumerable: true, get: function() {
    return node_1.getBooleanFromEnv;
  } });
  Object.defineProperty(exports, "getStringFromEnv", { enumerable: true, get: function() {
    return node_1.getStringFromEnv;
  } });
  Object.defineProperty(exports, "getNumberFromEnv", { enumerable: true, get: function() {
    return node_1.getNumberFromEnv;
  } });
  Object.defineProperty(exports, "getStringListFromEnv", { enumerable: true, get: function() {
    return node_1.getStringListFromEnv;
  } });
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/common/time.js
var require_time = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.addHrTimes = exports.isTimeInput = exports.isTimeInputHrTime = exports.hrTimeToSeconds = exports.hrTimeToMilliseconds = exports.hrTimeToMicroseconds = exports.hrTimeToNanoseconds = exports.hrTimeToTimeStamp = exports.hrTimeDuration = exports.timeInputToHrTime = exports.hrTime = exports.getTimeOrigin = exports.millisToHrTime = undefined;
  var platform_1 = require_platform();
  var NANOSECOND_DIGITS = 9;
  var NANOSECOND_DIGITS_IN_MILLIS = 6;
  var MILLISECONDS_TO_NANOSECONDS = Math.pow(10, NANOSECOND_DIGITS_IN_MILLIS);
  var SECOND_TO_NANOSECONDS = Math.pow(10, NANOSECOND_DIGITS);
  function millisToHrTime(epochMillis) {
    const epochSeconds = epochMillis / 1000;
    const seconds = Math.trunc(epochSeconds);
    const nanos = Math.round(epochMillis % 1000 * MILLISECONDS_TO_NANOSECONDS);
    return [seconds, nanos];
  }
  exports.millisToHrTime = millisToHrTime;
  function getTimeOrigin() {
    return platform_1.otperformance.timeOrigin;
  }
  exports.getTimeOrigin = getTimeOrigin;
  function hrTime(performanceNow) {
    const timeOrigin = millisToHrTime(platform_1.otperformance.timeOrigin);
    const now = millisToHrTime(typeof performanceNow === "number" ? performanceNow : platform_1.otperformance.now());
    return addHrTimes(timeOrigin, now);
  }
  exports.hrTime = hrTime;
  function timeInputToHrTime(time) {
    if (isTimeInputHrTime(time)) {
      return time;
    } else if (typeof time === "number") {
      if (time < platform_1.otperformance.timeOrigin / 2) {
        return hrTime(time);
      } else {
        return millisToHrTime(time);
      }
    } else if (time instanceof Date) {
      return millisToHrTime(time.getTime());
    } else {
      throw TypeError("Invalid input type");
    }
  }
  exports.timeInputToHrTime = timeInputToHrTime;
  function hrTimeDuration(startTime, endTime) {
    let seconds = endTime[0] - startTime[0];
    let nanos = endTime[1] - startTime[1];
    if (nanos < 0) {
      seconds -= 1;
      nanos += SECOND_TO_NANOSECONDS;
    }
    return [seconds, nanos];
  }
  exports.hrTimeDuration = hrTimeDuration;
  function hrTimeToTimeStamp(time) {
    const precision = NANOSECOND_DIGITS;
    const tmp = `${"0".repeat(precision)}${time[1]}Z`;
    const nanoString = tmp.substring(tmp.length - precision - 1);
    const date = new Date(time[0] * 1000).toISOString();
    return date.replace("000Z", nanoString);
  }
  exports.hrTimeToTimeStamp = hrTimeToTimeStamp;
  function hrTimeToNanoseconds(time) {
    return time[0] * SECOND_TO_NANOSECONDS + time[1];
  }
  exports.hrTimeToNanoseconds = hrTimeToNanoseconds;
  function hrTimeToMicroseconds(time) {
    return time[0] * 1e6 + time[1] / 1000;
  }
  exports.hrTimeToMicroseconds = hrTimeToMicroseconds;
  function hrTimeToMilliseconds(time) {
    return time[0] * 1000 + time[1] / 1e6;
  }
  exports.hrTimeToMilliseconds = hrTimeToMilliseconds;
  function hrTimeToSeconds(time) {
    return time[0] + time[1] / SECOND_TO_NANOSECONDS;
  }
  exports.hrTimeToSeconds = hrTimeToSeconds;
  function isTimeInputHrTime(value) {
    return Array.isArray(value) && value.length === 2 && typeof value[0] === "number" && typeof value[1] === "number";
  }
  exports.isTimeInputHrTime = isTimeInputHrTime;
  function isTimeInput(value) {
    return isTimeInputHrTime(value) || typeof value === "number" || value instanceof Date;
  }
  exports.isTimeInput = isTimeInput;
  function addHrTimes(time1, time2) {
    const out = [time1[0] + time2[0], time1[1] + time2[1]];
    if (out[1] >= SECOND_TO_NANOSECONDS) {
      out[1] -= SECOND_TO_NANOSECONDS;
      out[0] += 1;
    }
    return out;
  }
  exports.addHrTimes = addHrTimes;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/common/timer-util.js
var require_timer_util = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.unrefTimer = undefined;
  function unrefTimer(timer) {
    if (typeof timer !== "number") {
      timer.unref();
    }
  }
  exports.unrefTimer = unrefTimer;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/ExportResult.js
var require_ExportResult = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ExportResultCode = undefined;
  var ExportResultCode;
  (function(ExportResultCode) {
    ExportResultCode[ExportResultCode["SUCCESS"] = 0] = "SUCCESS";
    ExportResultCode[ExportResultCode["FAILED"] = 1] = "FAILED";
  })(ExportResultCode || (exports.ExportResultCode = ExportResultCode = {}));
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/propagation/composite.js
var require_composite = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.CompositePropagator = undefined;
  var api_1 = require_src();

  class CompositePropagator {
    _propagators;
    _fields;
    constructor(config = {}) {
      this._propagators = config.propagators ?? [];
      const fields = new Set;
      for (const propagator of this._propagators) {
        const propagatorFields = typeof propagator.fields === "function" ? propagator.fields() : [];
        for (const field of propagatorFields) {
          fields.add(field);
        }
      }
      this._fields = Array.from(fields);
    }
    inject(context, carrier, setter) {
      for (const propagator of this._propagators) {
        try {
          propagator.inject(context, carrier, setter);
        } catch (err) {
          api_1.diag.warn(`Failed to inject with ${propagator.constructor.name}. Err: ${err.message}`);
        }
      }
    }
    extract(context, carrier, getter) {
      return this._propagators.reduce((ctx, propagator) => {
        try {
          return propagator.extract(ctx, carrier, getter);
        } catch (err) {
          api_1.diag.warn(`Failed to extract with ${propagator.constructor.name}. Err: ${err.message}`);
        }
        return ctx;
      }, context);
    }
    fields() {
      return this._fields.slice();
    }
  }
  exports.CompositePropagator = CompositePropagator;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/internal/validators.js
var require_validators = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.validateValue = exports.validateKey = undefined;
  var VALID_KEY_CHAR_RANGE = "[_0-9a-z-*/]";
  var VALID_KEY = `[a-z]${VALID_KEY_CHAR_RANGE}{0,255}`;
  var VALID_VENDOR_KEY = `[a-z0-9]${VALID_KEY_CHAR_RANGE}{0,240}@[a-z]${VALID_KEY_CHAR_RANGE}{0,13}`;
  var VALID_KEY_REGEX = new RegExp(`^(?:${VALID_KEY}|${VALID_VENDOR_KEY})$`);
  var VALID_VALUE_BASE_REGEX = /^[ -~]{0,255}[!-~]$/;
  var INVALID_VALUE_COMMA_EQUAL_REGEX = /,|=/;
  function validateKey(key) {
    return VALID_KEY_REGEX.test(key);
  }
  exports.validateKey = validateKey;
  function validateValue(value) {
    return VALID_VALUE_BASE_REGEX.test(value) && !INVALID_VALUE_COMMA_EQUAL_REGEX.test(value);
  }
  exports.validateValue = validateValue;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/trace/TraceState.js
var require_TraceState = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.TraceState = undefined;
  var validators_1 = require_validators();
  var MAX_TRACE_STATE_ITEMS = 32;
  var MAX_TRACE_STATE_LEN = 512;
  var LIST_MEMBERS_SEPARATOR = ",";
  var LIST_MEMBER_KEY_VALUE_SPLITTER = "=";

  class TraceState {
    _length;
    _rawTraceState;
    _internalState;
    constructor(rawTraceState) {
      this._rawTraceState = typeof rawTraceState === "string" ? rawTraceState : "";
      this._length = this._rawTraceState.length;
    }
    set(key, value) {
      if (!(0, validators_1.validateKey)(key) || !(0, validators_1.validateValue)(value)) {
        return this;
      }
      const currState = this._getState();
      const currValue = currState.get(key);
      let newLength = this._length;
      if (typeof currValue === "string") {
        newLength += value.length - currValue.length;
      } else {
        newLength += key.length + value.length + (currState.size > 0 ? 2 : 1);
      }
      if (newLength > MAX_TRACE_STATE_LEN) {
        return this;
      }
      const newState = new Map(currState);
      newState.delete(key);
      newState.set(key, value);
      return this._fromState(newState, newLength);
    }
    unset(key) {
      const currState = this._getState();
      const currValue = currState.get(key);
      if (typeof currValue !== "string") {
        return this;
      }
      let newLength = this._length - (key.length + currValue.length + 1);
      if (currState.size > 1) {
        newLength = newLength - 1;
      }
      const newState = new Map(currState);
      newState.delete(key);
      return this._fromState(newState, newLength);
    }
    get(key) {
      const currState = this._getState();
      return currState.get(key);
    }
    serialize() {
      let serialized = "";
      let index = 0;
      for (const entry of this._getState()) {
        if (index > 0) {
          serialized = LIST_MEMBERS_SEPARATOR + serialized;
        }
        serialized = `${entry[0]}${LIST_MEMBER_KEY_VALUE_SPLITTER}${entry[1]}` + serialized;
        index++;
      }
      return serialized;
    }
    _getState() {
      if (this._internalState) {
        return this._internalState;
      }
      const vendorMembers = this._rawTraceState.split(LIST_MEMBERS_SEPARATOR);
      const vendorEntries = new Map;
      let currentLength = 0;
      for (const member of vendorMembers) {
        const m = member.trim();
        const idx = m.indexOf(LIST_MEMBER_KEY_VALUE_SPLITTER);
        if (idx === -1) {
          continue;
        }
        const key = m.slice(0, idx);
        const value = m.slice(idx + 1);
        if (!(0, validators_1.validateKey)(key) || !(0, validators_1.validateValue)(value)) {
          continue;
        }
        const futureLength = currentLength + m.length + (vendorEntries.size > 0 ? 1 : 0);
        if (futureLength > MAX_TRACE_STATE_LEN) {
          continue;
        }
        vendorEntries.set(key, value);
        currentLength = futureLength;
        if (vendorEntries.size >= MAX_TRACE_STATE_ITEMS) {
          break;
        }
      }
      this._length = currentLength;
      this._internalState = new Map(Array.from(vendorEntries.entries()).reverse());
      return this._internalState;
    }
    _fromState(state, length) {
      const traceState = Object.create(TraceState.prototype);
      traceState._internalState = state;
      traceState._length = length;
      return traceState;
    }
  }
  exports.TraceState = TraceState;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/trace/W3CTraceContextPropagator.js
var require_W3CTraceContextPropagator = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.W3CTraceContextPropagator = exports.parseTraceParent = exports.TRACE_STATE_HEADER = exports.TRACE_PARENT_HEADER = undefined;
  var api_1 = require_src();
  var suppress_tracing_1 = require_suppress_tracing();
  var TraceState_1 = require_TraceState();
  exports.TRACE_PARENT_HEADER = "traceparent";
  exports.TRACE_STATE_HEADER = "tracestate";
  var VERSION = "00";
  var VERSION_PART = "(?!ff)[\\da-f]{2}";
  var TRACE_ID_PART = "(?![0]{32})[\\da-f]{32}";
  var PARENT_ID_PART = "(?![0]{16})[\\da-f]{16}";
  var FLAGS_PART = "[\\da-f]{2}";
  var TRACE_PARENT_REGEX = new RegExp(`^\\s?(${VERSION_PART})-(${TRACE_ID_PART})-(${PARENT_ID_PART})-(${FLAGS_PART})(-.*)?\\s?$`);
  function parseTraceParent(traceParent) {
    const match = TRACE_PARENT_REGEX.exec(traceParent);
    if (!match)
      return null;
    if (match[1] === "00" && match[5])
      return null;
    return {
      traceId: match[2],
      spanId: match[3],
      traceFlags: parseInt(match[4], 16)
    };
  }
  exports.parseTraceParent = parseTraceParent;

  class W3CTraceContextPropagator {
    inject(context, carrier, setter) {
      const spanContext = api_1.trace.getSpanContext(context);
      if (!spanContext || (0, suppress_tracing_1.isTracingSuppressed)(context) || !(0, api_1.isSpanContextValid)(spanContext))
        return;
      const traceParent = `${VERSION}-${spanContext.traceId}-${spanContext.spanId}-0${Number(spanContext.traceFlags || api_1.TraceFlags.NONE).toString(16)}`;
      setter.set(carrier, exports.TRACE_PARENT_HEADER, traceParent);
      if (spanContext.traceState) {
        setter.set(carrier, exports.TRACE_STATE_HEADER, spanContext.traceState.serialize());
      }
    }
    extract(context, carrier, getter) {
      const traceParentHeader = getter.get(carrier, exports.TRACE_PARENT_HEADER);
      if (!traceParentHeader)
        return context;
      const traceParent = Array.isArray(traceParentHeader) ? traceParentHeader[0] : traceParentHeader;
      if (typeof traceParent !== "string")
        return context;
      const spanContext = parseTraceParent(traceParent);
      if (!spanContext)
        return context;
      spanContext.isRemote = true;
      const traceStateHeader = getter.get(carrier, exports.TRACE_STATE_HEADER);
      if (traceStateHeader) {
        const state = Array.isArray(traceStateHeader) ? traceStateHeader.join(",") : traceStateHeader;
        spanContext.traceState = new TraceState_1.TraceState(typeof state === "string" ? state : undefined);
      }
      return api_1.trace.setSpanContext(context, spanContext);
    }
    fields() {
      return [exports.TRACE_PARENT_HEADER, exports.TRACE_STATE_HEADER];
    }
  }
  exports.W3CTraceContextPropagator = W3CTraceContextPropagator;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/trace/rpc-metadata.js
var require_rpc_metadata = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.getRPCMetadata = exports.deleteRPCMetadata = exports.setRPCMetadata = exports.RPCType = undefined;
  var api_1 = require_src();
  var RPC_METADATA_KEY = (0, api_1.createContextKey)("OpenTelemetry SDK Context Key RPC_METADATA");
  var RPCType;
  (function(RPCType) {
    RPCType["HTTP"] = "http";
  })(RPCType || (exports.RPCType = RPCType = {}));
  function setRPCMetadata(context, meta) {
    return context.setValue(RPC_METADATA_KEY, meta);
  }
  exports.setRPCMetadata = setRPCMetadata;
  function deleteRPCMetadata(context) {
    return context.deleteValue(RPC_METADATA_KEY);
  }
  exports.deleteRPCMetadata = deleteRPCMetadata;
  function getRPCMetadata(context) {
    return context.getValue(RPC_METADATA_KEY);
  }
  exports.getRPCMetadata = getRPCMetadata;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/utils/lodash.merge.js
var require_lodash_merge = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.isPlainObject = undefined;
  var objectTag = "[object Object]";
  var nullTag = "[object Null]";
  var undefinedTag = "[object Undefined]";
  var funcProto = Function.prototype;
  var funcToString = funcProto.toString;
  var objectCtorString = funcToString.call(Object);
  var getPrototypeOf = Object.getPrototypeOf;
  var objectProto = Object.prototype;
  var hasOwnProperty = objectProto.hasOwnProperty;
  var symToStringTag = Symbol ? Symbol.toStringTag : undefined;
  var nativeObjectToString = objectProto.toString;
  function isPlainObject(value) {
    if (!isObjectLike(value) || baseGetTag(value) !== objectTag) {
      return false;
    }
    const proto = getPrototypeOf(value);
    if (proto === null) {
      return true;
    }
    const Ctor = hasOwnProperty.call(proto, "constructor") && proto.constructor;
    return typeof Ctor == "function" && Ctor instanceof Ctor && funcToString.call(Ctor) === objectCtorString;
  }
  exports.isPlainObject = isPlainObject;
  function isObjectLike(value) {
    return value != null && typeof value == "object";
  }
  function baseGetTag(value) {
    if (value == null) {
      return value === undefined ? undefinedTag : nullTag;
    }
    return symToStringTag && symToStringTag in Object(value) ? getRawTag(value) : objectToString(value);
  }
  function getRawTag(value) {
    const isOwn = hasOwnProperty.call(value, symToStringTag), tag = value[symToStringTag];
    let unmasked = false;
    try {
      value[symToStringTag] = undefined;
      unmasked = true;
    } catch {}
    const result = nativeObjectToString.call(value);
    if (unmasked) {
      if (isOwn) {
        value[symToStringTag] = tag;
      } else {
        delete value[symToStringTag];
      }
    }
    return result;
  }
  function objectToString(value) {
    return nativeObjectToString.call(value);
  }
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/utils/merge.js
var require_merge = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.merge = undefined;
  var lodash_merge_1 = require_lodash_merge();
  var MAX_LEVEL = 20;
  function merge(...args) {
    let result = args.shift();
    const objects = new WeakMap;
    while (args.length > 0) {
      result = mergeTwoObjects(result, args.shift(), 0, objects);
    }
    return result;
  }
  exports.merge = merge;
  function takeValue(value) {
    if (isArray(value)) {
      return value.slice();
    }
    return value;
  }
  function mergeTwoObjects(one, two, level = 0, objects) {
    let result;
    if (level > MAX_LEVEL) {
      return;
    }
    level++;
    if (isPrimitive(one) || isPrimitive(two) || isFunction(two)) {
      result = takeValue(two);
    } else if (isArray(one)) {
      result = one.slice();
      if (isArray(two)) {
        for (let i = 0, j = two.length;i < j; i++) {
          result.push(takeValue(two[i]));
        }
      } else if (isObject(two)) {
        const keys = Object.keys(two);
        for (let i = 0, j = keys.length;i < j; i++) {
          const key = keys[i];
          if (key === "__proto__" || key === "constructor" || key === "prototype") {
            continue;
          }
          result[key] = takeValue(two[key]);
        }
      }
    } else if (isObject(one)) {
      if (isObject(two)) {
        if (!shouldMerge(one, two)) {
          return two;
        }
        result = Object.assign({}, one);
        const keys = Object.keys(two);
        for (let i = 0, j = keys.length;i < j; i++) {
          const key = keys[i];
          if (key === "__proto__" || key === "constructor" || key === "prototype") {
            continue;
          }
          const twoValue = two[key];
          if (isPrimitive(twoValue)) {
            if (typeof twoValue === "undefined") {
              delete result[key];
            } else {
              result[key] = twoValue;
            }
          } else {
            const obj1 = result[key];
            const obj2 = twoValue;
            if (wasObjectReferenced(one, key, objects) || wasObjectReferenced(two, key, objects)) {
              delete result[key];
            } else {
              if (isObject(obj1) && isObject(obj2)) {
                const arr1 = objects.get(obj1) || [];
                const arr2 = objects.get(obj2) || [];
                arr1.push({ obj: one, key });
                arr2.push({ obj: two, key });
                objects.set(obj1, arr1);
                objects.set(obj2, arr2);
              }
              result[key] = mergeTwoObjects(result[key], twoValue, level, objects);
            }
          }
        }
      } else {
        result = two;
      }
    }
    return result;
  }
  function wasObjectReferenced(obj, key, objects) {
    const arr = objects.get(obj[key]) || [];
    for (let i = 0, j = arr.length;i < j; i++) {
      const info = arr[i];
      if (info.key === key && info.obj === obj) {
        return true;
      }
    }
    return false;
  }
  function isArray(value) {
    return Array.isArray(value);
  }
  function isFunction(value) {
    return typeof value === "function";
  }
  function isObject(value) {
    return !isPrimitive(value) && !isArray(value) && !isFunction(value) && typeof value === "object";
  }
  function isPrimitive(value) {
    return typeof value === "string" || typeof value === "number" || typeof value === "boolean" || typeof value === "undefined" || value instanceof Date || value instanceof RegExp || value === null;
  }
  function shouldMerge(one, two) {
    if (!(0, lodash_merge_1.isPlainObject)(one) || !(0, lodash_merge_1.isPlainObject)(two)) {
      return false;
    }
    return true;
  }
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/utils/timeout.js
var require_timeout = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.callWithTimeout = exports.TimeoutError = undefined;

  class TimeoutError extends Error {
    constructor(message) {
      super(message);
      Object.setPrototypeOf(this, TimeoutError.prototype);
    }
  }
  exports.TimeoutError = TimeoutError;
  function callWithTimeout(promise, timeout) {
    let timeoutHandle;
    const timeoutPromise = new Promise(function timeoutFunction(_resolve, reject) {
      timeoutHandle = setTimeout(function timeoutHandler() {
        reject(new TimeoutError("Operation timed out."));
      }, timeout);
    });
    return Promise.race([promise, timeoutPromise]).then((result) => {
      clearTimeout(timeoutHandle);
      return result;
    }, (reason) => {
      clearTimeout(timeoutHandle);
      throw reason;
    });
  }
  exports.callWithTimeout = callWithTimeout;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/utils/url.js
var require_url = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.isUrlIgnored = exports.urlMatches = undefined;
  function urlMatches(url, urlToMatch) {
    if (typeof urlToMatch === "string") {
      return url === urlToMatch;
    } else {
      return !!url.match(urlToMatch);
    }
  }
  exports.urlMatches = urlMatches;
  function isUrlIgnored(url, ignoredUrls) {
    if (!ignoredUrls) {
      return false;
    }
    for (const ignoreUrl of ignoredUrls) {
      if (urlMatches(url, ignoreUrl)) {
        return true;
      }
    }
    return false;
  }
  exports.isUrlIgnored = isUrlIgnored;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/utils/promise.js
var require_promise = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.Deferred = undefined;

  class Deferred {
    _promise;
    _resolve;
    _reject;
    constructor() {
      this._promise = new Promise((resolve, reject) => {
        this._resolve = resolve;
        this._reject = reject;
      });
    }
    get promise() {
      return this._promise;
    }
    resolve(val) {
      this._resolve(val);
    }
    reject(err) {
      this._reject(err);
    }
  }
  exports.Deferred = Deferred;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/utils/callback.js
var require_callback = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.BindOnceFuture = undefined;
  var promise_1 = require_promise();

  class BindOnceFuture {
    _isCalled = false;
    _deferred = new promise_1.Deferred;
    _callback;
    _that;
    constructor(callback, that) {
      this._callback = callback;
      this._that = that;
    }
    get isCalled() {
      return this._isCalled;
    }
    get promise() {
      return this._deferred.promise;
    }
    call(...args) {
      if (!this._isCalled) {
        this._isCalled = true;
        try {
          Promise.resolve(this._callback.call(this._that, ...args)).then((val) => this._deferred.resolve(val), (err) => this._deferred.reject(err));
        } catch (err) {
          this._deferred.reject(err);
        }
      }
      return this._deferred.promise;
    }
  }
  exports.BindOnceFuture = BindOnceFuture;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/utils/configuration.js
var require_configuration = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.diagLogLevelFromString = undefined;
  var api_1 = require_src();
  var logLevelMap = {
    ALL: api_1.DiagLogLevel.ALL,
    VERBOSE: api_1.DiagLogLevel.VERBOSE,
    DEBUG: api_1.DiagLogLevel.DEBUG,
    INFO: api_1.DiagLogLevel.INFO,
    WARN: api_1.DiagLogLevel.WARN,
    ERROR: api_1.DiagLogLevel.ERROR,
    NONE: api_1.DiagLogLevel.NONE
  };
  function diagLogLevelFromString(value) {
    if (value == null) {
      return;
    }
    const resolvedLogLevel = logLevelMap[value.toUpperCase()];
    if (resolvedLogLevel == null) {
      api_1.diag.warn(`Unknown log level "${value}", expected one of ${Object.keys(logLevelMap)}, using default`);
      return api_1.DiagLogLevel.INFO;
    }
    return resolvedLogLevel;
  }
  exports.diagLogLevelFromString = diagLogLevelFromString;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/internal/exporter.js
var require_exporter = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports._export = undefined;
  var api_1 = require_src();
  var suppress_tracing_1 = require_suppress_tracing();
  function _export(exporter, arg) {
    return new Promise((resolve) => {
      api_1.context.with((0, suppress_tracing_1.suppressTracing)(api_1.context.active()), () => {
        exporter.export(arg, resolve);
      });
    });
  }
  exports._export = _export;
});

// node_modules/.bun/@opentelemetry+core@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/core/build/src/index.js
var require_src5 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.diagLogLevelFromString = exports.BindOnceFuture = exports.urlMatches = exports.isUrlIgnored = exports.callWithTimeout = exports.TimeoutError = exports.merge = exports.TraceState = exports.unsuppressTracing = exports.suppressTracing = exports.isTracingSuppressed = exports.setRPCMetadata = exports.getRPCMetadata = exports.deleteRPCMetadata = exports.RPCType = exports.parseTraceParent = exports.W3CTraceContextPropagator = exports.TRACE_STATE_HEADER = exports.TRACE_PARENT_HEADER = exports.CompositePropagator = exports.otperformance = exports.getStringListFromEnv = exports.getNumberFromEnv = exports.getBooleanFromEnv = exports.getStringFromEnv = exports._globalThis = exports.SDK_INFO = exports.parseKeyPairsIntoRecord = exports.ExportResultCode = exports.unrefTimer = exports.timeInputToHrTime = exports.millisToHrTime = exports.isTimeInputHrTime = exports.isTimeInput = exports.hrTimeToTimeStamp = exports.hrTimeToSeconds = exports.hrTimeToNanoseconds = exports.hrTimeToMilliseconds = exports.hrTimeToMicroseconds = exports.hrTimeDuration = exports.hrTime = exports.getTimeOrigin = exports.addHrTimes = exports.loggingErrorHandler = exports.setGlobalErrorHandler = exports.globalErrorHandler = exports.sanitizeAttributes = exports.isAttributeValue = exports.AnchoredClock = exports.W3CBaggagePropagator = undefined;
  exports.internal = undefined;
  var W3CBaggagePropagator_1 = require_W3CBaggagePropagator();
  Object.defineProperty(exports, "W3CBaggagePropagator", { enumerable: true, get: function() {
    return W3CBaggagePropagator_1.W3CBaggagePropagator;
  } });
  var anchored_clock_1 = require_anchored_clock();
  Object.defineProperty(exports, "AnchoredClock", { enumerable: true, get: function() {
    return anchored_clock_1.AnchoredClock;
  } });
  var attributes_1 = require_attributes();
  Object.defineProperty(exports, "isAttributeValue", { enumerable: true, get: function() {
    return attributes_1.isAttributeValue;
  } });
  Object.defineProperty(exports, "sanitizeAttributes", { enumerable: true, get: function() {
    return attributes_1.sanitizeAttributes;
  } });
  var global_error_handler_1 = require_global_error_handler();
  Object.defineProperty(exports, "globalErrorHandler", { enumerable: true, get: function() {
    return global_error_handler_1.globalErrorHandler;
  } });
  Object.defineProperty(exports, "setGlobalErrorHandler", { enumerable: true, get: function() {
    return global_error_handler_1.setGlobalErrorHandler;
  } });
  var logging_error_handler_1 = require_logging_error_handler();
  Object.defineProperty(exports, "loggingErrorHandler", { enumerable: true, get: function() {
    return logging_error_handler_1.loggingErrorHandler;
  } });
  var time_1 = require_time();
  Object.defineProperty(exports, "addHrTimes", { enumerable: true, get: function() {
    return time_1.addHrTimes;
  } });
  Object.defineProperty(exports, "getTimeOrigin", { enumerable: true, get: function() {
    return time_1.getTimeOrigin;
  } });
  Object.defineProperty(exports, "hrTime", { enumerable: true, get: function() {
    return time_1.hrTime;
  } });
  Object.defineProperty(exports, "hrTimeDuration", { enumerable: true, get: function() {
    return time_1.hrTimeDuration;
  } });
  Object.defineProperty(exports, "hrTimeToMicroseconds", { enumerable: true, get: function() {
    return time_1.hrTimeToMicroseconds;
  } });
  Object.defineProperty(exports, "hrTimeToMilliseconds", { enumerable: true, get: function() {
    return time_1.hrTimeToMilliseconds;
  } });
  Object.defineProperty(exports, "hrTimeToNanoseconds", { enumerable: true, get: function() {
    return time_1.hrTimeToNanoseconds;
  } });
  Object.defineProperty(exports, "hrTimeToSeconds", { enumerable: true, get: function() {
    return time_1.hrTimeToSeconds;
  } });
  Object.defineProperty(exports, "hrTimeToTimeStamp", { enumerable: true, get: function() {
    return time_1.hrTimeToTimeStamp;
  } });
  Object.defineProperty(exports, "isTimeInput", { enumerable: true, get: function() {
    return time_1.isTimeInput;
  } });
  Object.defineProperty(exports, "isTimeInputHrTime", { enumerable: true, get: function() {
    return time_1.isTimeInputHrTime;
  } });
  Object.defineProperty(exports, "millisToHrTime", { enumerable: true, get: function() {
    return time_1.millisToHrTime;
  } });
  Object.defineProperty(exports, "timeInputToHrTime", { enumerable: true, get: function() {
    return time_1.timeInputToHrTime;
  } });
  var timer_util_1 = require_timer_util();
  Object.defineProperty(exports, "unrefTimer", { enumerable: true, get: function() {
    return timer_util_1.unrefTimer;
  } });
  var ExportResult_1 = require_ExportResult();
  Object.defineProperty(exports, "ExportResultCode", { enumerable: true, get: function() {
    return ExportResult_1.ExportResultCode;
  } });
  var utils_1 = require_utils();
  Object.defineProperty(exports, "parseKeyPairsIntoRecord", { enumerable: true, get: function() {
    return utils_1.parseKeyPairsIntoRecord;
  } });
  var platform_1 = require_platform();
  Object.defineProperty(exports, "SDK_INFO", { enumerable: true, get: function() {
    return platform_1.SDK_INFO;
  } });
  Object.defineProperty(exports, "_globalThis", { enumerable: true, get: function() {
    return platform_1._globalThis;
  } });
  Object.defineProperty(exports, "getStringFromEnv", { enumerable: true, get: function() {
    return platform_1.getStringFromEnv;
  } });
  Object.defineProperty(exports, "getBooleanFromEnv", { enumerable: true, get: function() {
    return platform_1.getBooleanFromEnv;
  } });
  Object.defineProperty(exports, "getNumberFromEnv", { enumerable: true, get: function() {
    return platform_1.getNumberFromEnv;
  } });
  Object.defineProperty(exports, "getStringListFromEnv", { enumerable: true, get: function() {
    return platform_1.getStringListFromEnv;
  } });
  Object.defineProperty(exports, "otperformance", { enumerable: true, get: function() {
    return platform_1.otperformance;
  } });
  var composite_1 = require_composite();
  Object.defineProperty(exports, "CompositePropagator", { enumerable: true, get: function() {
    return composite_1.CompositePropagator;
  } });
  var W3CTraceContextPropagator_1 = require_W3CTraceContextPropagator();
  Object.defineProperty(exports, "TRACE_PARENT_HEADER", { enumerable: true, get: function() {
    return W3CTraceContextPropagator_1.TRACE_PARENT_HEADER;
  } });
  Object.defineProperty(exports, "TRACE_STATE_HEADER", { enumerable: true, get: function() {
    return W3CTraceContextPropagator_1.TRACE_STATE_HEADER;
  } });
  Object.defineProperty(exports, "W3CTraceContextPropagator", { enumerable: true, get: function() {
    return W3CTraceContextPropagator_1.W3CTraceContextPropagator;
  } });
  Object.defineProperty(exports, "parseTraceParent", { enumerable: true, get: function() {
    return W3CTraceContextPropagator_1.parseTraceParent;
  } });
  var rpc_metadata_1 = require_rpc_metadata();
  Object.defineProperty(exports, "RPCType", { enumerable: true, get: function() {
    return rpc_metadata_1.RPCType;
  } });
  Object.defineProperty(exports, "deleteRPCMetadata", { enumerable: true, get: function() {
    return rpc_metadata_1.deleteRPCMetadata;
  } });
  Object.defineProperty(exports, "getRPCMetadata", { enumerable: true, get: function() {
    return rpc_metadata_1.getRPCMetadata;
  } });
  Object.defineProperty(exports, "setRPCMetadata", { enumerable: true, get: function() {
    return rpc_metadata_1.setRPCMetadata;
  } });
  var suppress_tracing_1 = require_suppress_tracing();
  Object.defineProperty(exports, "isTracingSuppressed", { enumerable: true, get: function() {
    return suppress_tracing_1.isTracingSuppressed;
  } });
  Object.defineProperty(exports, "suppressTracing", { enumerable: true, get: function() {
    return suppress_tracing_1.suppressTracing;
  } });
  Object.defineProperty(exports, "unsuppressTracing", { enumerable: true, get: function() {
    return suppress_tracing_1.unsuppressTracing;
  } });
  var TraceState_1 = require_TraceState();
  Object.defineProperty(exports, "TraceState", { enumerable: true, get: function() {
    return TraceState_1.TraceState;
  } });
  var merge_1 = require_merge();
  Object.defineProperty(exports, "merge", { enumerable: true, get: function() {
    return merge_1.merge;
  } });
  var timeout_1 = require_timeout();
  Object.defineProperty(exports, "TimeoutError", { enumerable: true, get: function() {
    return timeout_1.TimeoutError;
  } });
  Object.defineProperty(exports, "callWithTimeout", { enumerable: true, get: function() {
    return timeout_1.callWithTimeout;
  } });
  var url_1 = require_url();
  Object.defineProperty(exports, "isUrlIgnored", { enumerable: true, get: function() {
    return url_1.isUrlIgnored;
  } });
  Object.defineProperty(exports, "urlMatches", { enumerable: true, get: function() {
    return url_1.urlMatches;
  } });
  var callback_1 = require_callback();
  Object.defineProperty(exports, "BindOnceFuture", { enumerable: true, get: function() {
    return callback_1.BindOnceFuture;
  } });
  var configuration_1 = require_configuration();
  Object.defineProperty(exports, "diagLogLevelFromString", { enumerable: true, get: function() {
    return configuration_1.diagLogLevelFromString;
  } });
  var exporter_1 = require_exporter();
  exports.internal = {
    _export: exporter_1._export
  };
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/OTLPExporterBase.js
var require_OTLPExporterBase = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPExporterBase = undefined;

  class OTLPExporterBase {
    _delegate;
    constructor(delegate) {
      this._delegate = delegate;
    }
    export(items, resultCallback) {
      this._delegate.export(items, resultCallback);
    }
    forceFlush() {
      return this._delegate.forceFlush();
    }
    shutdown() {
      return this._delegate.shutdown();
    }
    setMetrics(metrics) {
      this._delegate.setMetrics(metrics);
    }
  }
  exports.OTLPExporterBase = OTLPExporterBase;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/types.js
var require_types = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPExporterError = undefined;

  class OTLPExporterError extends Error {
    code;
    name = "OTLPExporterError";
    data;
    constructor(message, code, data) {
      super(message);
      this.data = data;
      this.code = code;
    }
  }
  exports.OTLPExporterError = OTLPExporterError;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/configuration/shared-configuration.js
var require_shared_configuration = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.getSharedConfigurationDefaults = exports.mergeOtlpSharedConfigurationWithDefaults = exports.wrapStaticHeadersInFunction = exports.validateTimeoutMillis = undefined;
  function validateTimeoutMillis(timeoutMillis) {
    if (Number.isFinite(timeoutMillis) && timeoutMillis > 0) {
      return timeoutMillis;
    }
    throw new Error(`Configuration: timeoutMillis is invalid, expected number greater than 0 (actual: '${timeoutMillis}')`);
  }
  exports.validateTimeoutMillis = validateTimeoutMillis;
  function wrapStaticHeadersInFunction(headers) {
    if (headers == null) {
      return;
    }
    return async () => headers;
  }
  exports.wrapStaticHeadersInFunction = wrapStaticHeadersInFunction;
  function mergeOtlpSharedConfigurationWithDefaults(userProvidedConfiguration, fallbackConfiguration, defaultConfiguration) {
    return {
      timeoutMillis: validateTimeoutMillis(userProvidedConfiguration.timeoutMillis ?? fallbackConfiguration.timeoutMillis ?? defaultConfiguration.timeoutMillis),
      concurrencyLimit: userProvidedConfiguration.concurrencyLimit ?? fallbackConfiguration.concurrencyLimit ?? defaultConfiguration.concurrencyLimit,
      compression: userProvidedConfiguration.compression ?? fallbackConfiguration.compression ?? defaultConfiguration.compression
    };
  }
  exports.mergeOtlpSharedConfigurationWithDefaults = mergeOtlpSharedConfigurationWithDefaults;
  function getSharedConfigurationDefaults() {
    return {
      timeoutMillis: 1e4,
      concurrencyLimit: 30,
      compression: "none"
    };
  }
  exports.getSharedConfigurationDefaults = getSharedConfigurationDefaults;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/configuration/legacy-node-configuration.js
var require_legacy_node_configuration = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.CompressionAlgorithm = undefined;
  var CompressionAlgorithm;
  (function(CompressionAlgorithm) {
    CompressionAlgorithm["NONE"] = "none";
    CompressionAlgorithm["GZIP"] = "gzip";
  })(CompressionAlgorithm || (exports.CompressionAlgorithm = CompressionAlgorithm = {}));
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/bounded-queue-export-promise-handler.js
var require_bounded_queue_export_promise_handler = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.createBoundedQueueExportPromiseHandler = undefined;

  class BoundedQueueExportPromiseHandler {
    _concurrencyLimit;
    _sendingPromises = [];
    constructor(concurrencyLimit) {
      this._concurrencyLimit = concurrencyLimit;
    }
    pushPromise(promise) {
      if (this.hasReachedLimit()) {
        throw new Error("Concurrency Limit reached");
      }
      this._sendingPromises.push(promise);
      const popPromise = () => {
        const index = this._sendingPromises.indexOf(promise);
        this._sendingPromises.splice(index, 1);
      };
      promise.then(popPromise, popPromise);
    }
    hasReachedLimit() {
      return this._sendingPromises.length >= this._concurrencyLimit;
    }
    async awaitAll() {
      await Promise.all(this._sendingPromises);
    }
  }
  function createBoundedQueueExportPromiseHandler(options) {
    return new BoundedQueueExportPromiseHandler(options.concurrencyLimit);
  }
  exports.createBoundedQueueExportPromiseHandler = createBoundedQueueExportPromiseHandler;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/logging-response-handler.js
var require_logging_response_handler = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.createLoggingPartialSuccessResponseHandler = undefined;
  var api_1 = require_src();
  function isPartialSuccessResponse(response) {
    return Object.prototype.hasOwnProperty.call(response, "partialSuccess");
  }
  function createLoggingPartialSuccessResponseHandler() {
    return {
      handleResponse(response) {
        if (response == null || !isPartialSuccessResponse(response) || response.partialSuccess == null || Object.keys(response.partialSuccess).length === 0) {
          return;
        }
        api_1.diag.warn("Received Partial Success response:", JSON.stringify(response.partialSuccess));
      }
    };
  }
  exports.createLoggingPartialSuccessResponseHandler = createLoggingPartialSuccessResponseHandler;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/otlp-export-delegate.js
var require_otlp_export_delegate = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.createOtlpExportDelegate = undefined;
  var core_1 = require_src5();
  var types_1 = require_types();
  var logging_response_handler_1 = require_logging_response_handler();
  var api_1 = require_src();

  class OTLPExportDelegate {
    _metrics;
    _diagLogger;
    _transport;
    _serializer;
    _responseHandler;
    _promiseQueue;
    _timeout;
    constructor(transport, serializer, responseHandler, promiseQueue, metrics, timeout) {
      this._transport = transport;
      this._serializer = serializer;
      this._responseHandler = responseHandler;
      this._promiseQueue = promiseQueue;
      this._timeout = timeout;
      this._diagLogger = api_1.diag.createComponentLogger({
        namespace: "OTLPExportDelegate"
      });
      this._metrics = metrics;
    }
    export(internalRepresentation, resultCallback) {
      this._diagLogger.debug("items to be sent", internalRepresentation);
      if (this._promiseQueue.hasReachedLimit()) {
        resultCallback({
          code: core_1.ExportResultCode.FAILED,
          error: new Error("Concurrent export limit reached")
        });
        return;
      }
      const serializedRequest = this._serializer.serializeRequest(internalRepresentation);
      if (serializedRequest == null) {
        resultCallback({
          code: core_1.ExportResultCode.FAILED,
          error: new Error("Nothing to send")
        });
        return;
      }
      const finishExport = this._metrics.startExport(internalRepresentation);
      this._promiseQueue.pushPromise(this._transport.send(serializedRequest, this._timeout).then((response) => {
        if (response.status === "success") {
          finishExport(undefined);
          if (response.data != null) {
            try {
              this._responseHandler.handleResponse(this._serializer.deserializeResponse(response.data));
            } catch (e) {
              this._diagLogger.warn("Export succeeded but could not deserialize response - is the response specification compliant?", e, response.data);
            }
          }
          resultCallback({
            code: core_1.ExportResultCode.SUCCESS
          });
          return;
        } else if (response.status === "failure" && response.error) {
          finishExport(response.error);
          resultCallback({
            code: core_1.ExportResultCode.FAILED,
            error: response.error
          });
          return;
        } else if (response.status === "retryable") {
          finishExport("export_max_retries");
          resultCallback({
            code: core_1.ExportResultCode.FAILED,
            error: response.error ?? new types_1.OTLPExporterError("Export failed with retryable status")
          });
        } else {
          finishExport("export_failed");
          resultCallback({
            code: core_1.ExportResultCode.FAILED,
            error: new types_1.OTLPExporterError("Export failed with unknown error")
          });
        }
      }, (reason) => {
        finishExport(reason);
        resultCallback({
          code: core_1.ExportResultCode.FAILED,
          error: reason
        });
      }));
    }
    forceFlush() {
      return this._promiseQueue.awaitAll();
    }
    setMetrics(metrics) {
      this._metrics = metrics;
    }
    async shutdown() {
      this._diagLogger.debug("shutdown started");
      await this.forceFlush();
      this._transport.shutdown();
    }
  }
  function createOtlpExportDelegate(components, settings) {
    return new OTLPExportDelegate(components.transport, components.serializer, (0, logging_response_handler_1.createLoggingPartialSuccessResponseHandler)(), components.promiseHandler, components.metrics, settings.timeout);
  }
  exports.createOtlpExportDelegate = createOtlpExportDelegate;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/otlp-network-export-delegate.js
var require_otlp_network_export_delegate = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.createOtlpNetworkExportDelegate = undefined;
  var bounded_queue_export_promise_handler_1 = require_bounded_queue_export_promise_handler();
  var otlp_export_delegate_1 = require_otlp_export_delegate();
  function createOtlpNetworkExportDelegate(options, serializer, metrics, transport) {
    return (0, otlp_export_delegate_1.createOtlpExportDelegate)({
      transport,
      serializer,
      promiseHandler: (0, bounded_queue_export_promise_handler_1.createBoundedQueueExportPromiseHandler)(options),
      metrics
    }, { timeout: options.timeoutMillis });
  }
  exports.createOtlpNetworkExportDelegate = createOtlpNetworkExportDelegate;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/semconv.js
var require_semconv2 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ATTR_ERROR_TYPE = exports.ATTR_SERVER_PORT = exports.ATTR_SERVER_ADDRESS = exports.ATTR_OTEL_COMPONENT_TYPE = exports.ATTR_OTEL_COMPONENT_NAME = exports.ATTR_HTTP_RESPONSE_STATUS_CODE = undefined;
  exports.ATTR_HTTP_RESPONSE_STATUS_CODE = "http.response.status_code";
  exports.ATTR_OTEL_COMPONENT_NAME = "otel.component.name";
  exports.ATTR_OTEL_COMPONENT_TYPE = "otel.component.type";
  exports.ATTR_SERVER_ADDRESS = "server.address";
  exports.ATTR_SERVER_PORT = "server.port";
  exports.ATTR_ERROR_TYPE = "error.type";
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/version.js
var require_version2 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.VERSION = undefined;
  exports.VERSION = "0.222.0";
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/ExporterMetrics.js
var require_ExporterMetrics = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ExporterMetrics = undefined;
  var api_1 = require_src();
  var core_1 = require_src5();
  var semconv_1 = require_semconv2();
  var version_1 = require_version2();
  var componentCounter = new Map;

  class ExporterMetrics {
    inflight;
    exported;
    duration;
    standardAttrs;
    responseAttributesFromError;
    helper;
    constructor(options) {
      const { componentType, metricsHelper, meterProvider, url, responseAttributesFromError } = options;
      this.responseAttributesFromError = responseAttributesFromError;
      const meter = meterProvider ? meterProvider.getMeter("@opentelemetry/otlp-exporter", version_1.VERSION) : (0, api_1.createNoopMeter)();
      const counter = componentCounter.get(componentType) ?? 0;
      componentCounter.set(componentType, counter + 1);
      this.standardAttrs = {
        [semconv_1.ATTR_OTEL_COMPONENT_TYPE]: componentType,
        [semconv_1.ATTR_OTEL_COMPONENT_NAME]: `${componentType}/${counter}`
      };
      if (url) {
        let urlToParse = url;
        if (!url.includes("://")) {
          urlToParse = `http://${url}`;
        }
        try {
          const parsedUrl = new URL(urlToParse);
          this.standardAttrs[semconv_1.ATTR_SERVER_ADDRESS] = parsedUrl.hostname;
          let port = undefined;
          if (parsedUrl.port) {
            port = Number(parsedUrl.port);
          } else if (parsedUrl.protocol === "http:") {
            port = 80;
          } else if (parsedUrl.protocol === "https:") {
            port = 443;
          }
          if (typeof port === "number") {
            this.standardAttrs[semconv_1.ATTR_SERVER_PORT] = port;
          }
        } catch {}
      }
      this.helper = metricsHelper;
      this.inflight = meter.createUpDownCounter(`otel.sdk.exporter.${this.helper.name}.inflight`, {
        unit: `{${this.helper.name}}`,
        description: `The number of ${this.helper.name}s which were passed to the exporter, but that have not been exported yet (neither successful, nor failed).`
      });
      this.exported = meter.createCounter(`otel.sdk.exporter.${this.helper.name}.exported`, {
        unit: `{${this.helper.name}}`,
        description: `The number of ${this.helper.name}s for which the export has finished, either successful or failed.`
      });
      this.duration = meter.createHistogram("otel.sdk.exporter.operation.duration", {
        unit: "s",
        description: "The duration of exporting a batch of telemetry records.",
        advice: {
          explicitBucketBoundaries: []
        }
      });
    }
    startExport(request) {
      const numItems = this.helper.countItems(request);
      const startTime = (0, core_1.hrTime)();
      this.inflight.add(numItems, this.standardAttrs);
      return (error) => {
        const endTime = (0, core_1.hrTime)();
        this.inflight.add(-numItems, this.standardAttrs);
        const exportedAttrs = error ? {
          ...this.standardAttrs,
          [semconv_1.ATTR_ERROR_TYPE]: error instanceof Error ? error.name : "export_failed"
        } : this.standardAttrs;
        this.exported.add(numItems, exportedAttrs);
        const durationAttrs = {
          ...exportedAttrs,
          ...this.responseAttributesFromError(error)
        };
        const duration = (0, core_1.hrTimeToMilliseconds)((0, core_1.hrTimeDuration)(startTime, endTime)) / 1000;
        this.duration.record(duration, durationAttrs);
      };
    }
  }
  exports.ExporterMetrics = ExporterMetrics;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/index.js
var require_src6 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ExporterMetrics = exports.createOtlpNetworkExportDelegate = exports.CompressionAlgorithm = exports.getSharedConfigurationDefaults = exports.mergeOtlpSharedConfigurationWithDefaults = exports.OTLPExporterError = exports.OTLPExporterBase = undefined;
  var OTLPExporterBase_1 = require_OTLPExporterBase();
  Object.defineProperty(exports, "OTLPExporterBase", { enumerable: true, get: function() {
    return OTLPExporterBase_1.OTLPExporterBase;
  } });
  var types_1 = require_types();
  Object.defineProperty(exports, "OTLPExporterError", { enumerable: true, get: function() {
    return types_1.OTLPExporterError;
  } });
  var shared_configuration_1 = require_shared_configuration();
  Object.defineProperty(exports, "mergeOtlpSharedConfigurationWithDefaults", { enumerable: true, get: function() {
    return shared_configuration_1.mergeOtlpSharedConfigurationWithDefaults;
  } });
  Object.defineProperty(exports, "getSharedConfigurationDefaults", { enumerable: true, get: function() {
    return shared_configuration_1.getSharedConfigurationDefaults;
  } });
  var legacy_node_configuration_1 = require_legacy_node_configuration();
  Object.defineProperty(exports, "CompressionAlgorithm", { enumerable: true, get: function() {
    return legacy_node_configuration_1.CompressionAlgorithm;
  } });
  var otlp_network_export_delegate_1 = require_otlp_network_export_delegate();
  Object.defineProperty(exports, "createOtlpNetworkExportDelegate", { enumerable: true, get: function() {
    return otlp_network_export_delegate_1.createOtlpNetworkExportDelegate;
  } });
  var ExporterMetrics_1 = require_ExporterMetrics();
  Object.defineProperty(exports, "ExporterMetrics", { enumerable: true, get: function() {
    return ExporterMetrics_1.ExporterMetrics;
  } });
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/metrics/index.js
var require_metrics = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.MetricsExporterMetricsHelper = undefined;
  exports.MetricsExporterMetricsHelper = {
    name: "metric_data_point",
    countItems: (request) => {
      let count = 0;
      for (const scopeMetrics of request.scopeMetrics) {
        for (const metric of scopeMetrics.metrics) {
          count += metric.dataPoints.length;
        }
      }
      return count;
    }
  };
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/trace/index.js
var require_trace2 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.TraceExporterMetricsHelper = undefined;
  exports.TraceExporterMetricsHelper = {
    name: "span",
    countItems: (request) => request.length
  };
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/logs/index.js
var require_logs = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.LogsExporterMetricsHelper = undefined;
  exports.LogsExporterMetricsHelper = {
    name: "log",
    countItems: (request) => request.length
  };
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/common/protobuf/utils.js
var require_utils3 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.estimateVarintSize = undefined;
  function estimateVarintSize(v) {
    if (v < 0)
      return 10;
    if (v < 128)
      return 1;
    if (v < 16384)
      return 2;
    if (v < 2097152)
      return 3;
    if (v < 268435456)
      return 4;
    if (v < 34359738368)
      return 5;
    if (v < 4398046511104)
      return 6;
    if (v < 562949953421312)
      return 7;
    if (v < 72057594037927940)
      return 8;
    return 9;
  }
  exports.estimateVarintSize = estimateVarintSize;
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/common/protobuf/protobuf-writer.js
var require_protobuf_writer = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ProtobufWriter = exports.GROWING_BUFFER_DEBUG_MESSAGE = undefined;
  var api_1 = require_src();
  var utils_1 = require_utils3();
  exports.GROWING_BUFFER_DEBUG_MESSAGE = "ProtobufWriter: estimated size was too small, growing buffer.";
  var RESERVED_LENGTH_BYTES = 1;

  class ProtobufWriter {
    _buffer;
    _textEncoder;
    _dataView;
    pos = 0;
    constructor(estimatedSize = 65536) {
      this._buffer = new Uint8Array(estimatedSize);
      this._textEncoder = new TextEncoder;
      this._dataView = new DataView(this._buffer.buffer, this._buffer.byteOffset);
    }
    _ensureCapacity(size) {
      const needed = this.pos + size;
      if (needed <= this._buffer.length) {
        return;
      }
      api_1.diag.debug(exports.GROWING_BUFFER_DEBUG_MESSAGE);
      let newSize = this._buffer.length * 2;
      while (newSize < needed) {
        newSize *= 2;
      }
      const newBuffer = new Uint8Array(newSize);
      newBuffer.set(this._buffer);
      this._buffer = newBuffer;
      this._dataView = new DataView(this._buffer.buffer, this._buffer.byteOffset);
    }
    finish() {
      return this._buffer.subarray(0, this.pos);
    }
    startLengthDelimited() {
      const lengthPos = this.pos;
      this._ensureCapacity(RESERVED_LENGTH_BYTES);
      this.pos += RESERVED_LENGTH_BYTES;
      return lengthPos;
    }
    finishLengthDelimited(pos, length) {
      const v = length >>> 0;
      const varintSize = (0, utils_1.estimateVarintSize)(v);
      if (varintSize > RESERVED_LENGTH_BYTES) {
        const additionalBytes = varintSize - RESERVED_LENGTH_BYTES;
        this._ensureCapacity(additionalBytes);
        this._buffer.copyWithin(pos + varintSize, pos + RESERVED_LENGTH_BYTES, this.pos);
        this.pos += additionalBytes;
      }
      let writePos = pos;
      if (v < 128) {
        this._buffer[writePos] = v;
      } else if (v < 16384) {
        this._buffer[writePos++] = v & 127 | 128;
        this._buffer[writePos] = v >>> 7;
      } else if (v < 2097152) {
        this._buffer[writePos++] = v & 127 | 128;
        this._buffer[writePos++] = v >>> 7 & 127 | 128;
        this._buffer[writePos] = v >>> 14;
      } else if (v < 268435456) {
        this._buffer[writePos++] = v & 127 | 128;
        this._buffer[writePos++] = v >>> 7 & 127 | 128;
        this._buffer[writePos++] = v >>> 14 & 127 | 128;
        this._buffer[writePos] = v >>> 21;
      } else {
        this._buffer[writePos++] = v & 127 | 128;
        this._buffer[writePos++] = v >>> 7 & 127 | 128;
        this._buffer[writePos++] = v >>> 14 & 127 | 128;
        this._buffer[writePos++] = v >>> 21 & 127 | 128;
        this._buffer[writePos] = v >>> 28;
      }
    }
    writeSint32(value) {
      this.writeVarint((value << 1 ^ value >> 31) >>> 0);
    }
    writeSfixed64(value) {
      let low;
      let high;
      if (value >= 0) {
        low = value >>> 0;
        high = value / 4294967296 >>> 0;
      } else {
        const abs = Math.abs(value);
        low = abs >>> 0;
        high = abs / 4294967296 >>> 0;
        low = ~low >>> 0;
        high = ~high >>> 0;
        low = low + 1 >>> 0;
        if (low === 0) {
          high = high + 1 >>> 0;
        }
      }
      this.writeFixed64(low, high);
    }
    writeVarint(value) {
      this._ensureCapacity((0, utils_1.estimateVarintSize)(value));
      if (value >= 0 && value <= 4294967295) {
        let v = value >>> 0;
        while (v > 127) {
          this._buffer[this.pos++] = v & 127 | 128;
          v >>>= 7;
        }
        this._buffer[this.pos++] = v;
      } else {
        let low;
        let high;
        if (value >= 0) {
          low = value >>> 0;
          high = value / 4294967296 >>> 0;
        } else {
          const abs = Math.abs(value);
          low = abs >>> 0;
          high = abs / 4294967296 >>> 0;
          low = ~low >>> 0;
          high = ~high >>> 0;
          low = low + 1 >>> 0;
          if (low === 0) {
            high = high + 1 >>> 0;
          }
        }
        while (high > 0 || low > 127) {
          this._buffer[this.pos++] = low & 127 | 128;
          low = (low >>> 7 | high << 25) >>> 0;
          high >>>= 7;
        }
        this._buffer[this.pos++] = low & 127;
      }
    }
    writeFixed32(value) {
      this._ensureCapacity(4);
      const v = value >>> 0;
      this._buffer[this.pos++] = v & 255;
      this._buffer[this.pos++] = v >>> 8 & 255;
      this._buffer[this.pos++] = v >>> 16 & 255;
      this._buffer[this.pos++] = v >>> 24 & 255;
    }
    writeFixed64(low, high) {
      this._ensureCapacity(8);
      const l = low >>> 0;
      const h = high >>> 0;
      this._buffer[this.pos++] = l & 255;
      this._buffer[this.pos++] = l >>> 8 & 255;
      this._buffer[this.pos++] = l >>> 16 & 255;
      this._buffer[this.pos++] = l >>> 24 & 255;
      this._buffer[this.pos++] = h & 255;
      this._buffer[this.pos++] = h >>> 8 & 255;
      this._buffer[this.pos++] = h >>> 16 & 255;
      this._buffer[this.pos++] = h >>> 24 & 255;
    }
    writeBytes(bytes) {
      this.writeVarint(bytes.length);
      this._ensureCapacity(bytes.length);
      this._buffer.set(bytes, this.pos);
      this.pos += bytes.length;
    }
    writeTag(fieldNumber, wireType) {
      this.writeVarint(fieldNumber << 3 | wireType);
    }
    writeDouble(value) {
      this._ensureCapacity(8);
      this._dataView.setFloat64(this.pos, value, true);
      this.pos += 8;
    }
    writeString(str) {
      let isAscii = true;
      const len = str.length;
      for (let i = 0;i < len; i++) {
        if (str.charCodeAt(i) > 127) {
          isAscii = false;
          break;
        }
      }
      if (isAscii) {
        this.writeVarint(len);
        this._ensureCapacity(len);
        for (let i = 0;i < len; i++) {
          this._buffer[this.pos++] = str.charCodeAt(i);
        }
      } else {
        const bytes = this._textEncoder.encode(str);
        this.writeBytes(bytes);
      }
    }
  }
  exports.ProtobufWriter = ProtobufWriter;
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/common/hex-to-binary.js
var require_hex_to_binary = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.hexToBinary = undefined;
  function intValue(charCode) {
    if (charCode >= 48 && charCode <= 57) {
      return charCode - 48;
    }
    if (charCode >= 97 && charCode <= 102) {
      return charCode - 87;
    }
    return charCode - 55;
  }
  function hexToBinary(hexStr) {
    const buf = new Uint8Array(hexStr.length / 2);
    let offset = 0;
    for (let i = 0;i < hexStr.length; i += 2) {
      const hi = intValue(hexStr.charCodeAt(i));
      const lo = intValue(hexStr.charCodeAt(i + 1));
      buf[offset++] = hi << 4 | lo;
    }
    return buf;
  }
  exports.hexToBinary = hexToBinary;
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/common/protobuf/common-serializer.js
var require_common_serializer = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.writeResource = exports.writeInstrumentationScope = exports.writeAnyValue = exports.writeKeyValue = exports.writeAttributes = exports.writeHrTimeAsFixed64 = undefined;
  function writeHrTimeAsFixed64(serializer, hrTime) {
    const seconds = hrTime[0];
    const nanos = hrTime[1];
    const nanosPerSecond = 1e9;
    const secondsLower16Bits = seconds & 65535;
    const secondsUpperBits = seconds / 65536 >>> 0;
    const nanosFromLower16Bits = secondsLower16Bits * nanosPerSecond;
    const nanosFromUpperBits = secondsUpperBits * nanosPerSecond;
    const lower16ContributionLow32 = nanosFromLower16Bits >>> 0;
    const lower16ContributionHigh32 = Math.floor(nanosFromLower16Bits / 4294967296);
    const upperBitsContributionLow32 = (nanosFromUpperBits & 65535) * 65536 >>> 0;
    const upperBitsContributionHigh32 = nanosFromUpperBits / 65536 >>> 0;
    const low32WithCarry = lower16ContributionLow32 + upperBitsContributionLow32 + nanos;
    const totalLow = low32WithCarry >>> 0;
    const carry = Math.floor(low32WithCarry / 4294967296);
    const totalHigh = lower16ContributionHigh32 + upperBitsContributionHigh32 + carry >>> 0;
    serializer.writeFixed64(totalLow, totalHigh);
  }
  exports.writeHrTimeAsFixed64 = writeHrTimeAsFixed64;
  function writeAttributes(writer, attributes, fieldNumber) {
    for (const key in attributes) {
      if (!Object.prototype.hasOwnProperty.call(attributes, key)) {
        continue;
      }
      const value = attributes[key];
      writer.writeTag(fieldNumber, 2);
      const kvStart = writer.startLengthDelimited();
      const startPos = writer.pos;
      writeKeyValue(writer, key, value);
      writer.finishLengthDelimited(kvStart, writer.pos - startPos);
    }
  }
  exports.writeAttributes = writeAttributes;
  function writeKeyValue(writer, key, value) {
    writer.writeTag(1, 2);
    writer.writeString(key);
    writer.writeTag(2, 2);
    const valueStart = writer.startLengthDelimited();
    const startPos = writer.pos;
    writeAnyValue(writer, value);
    writer.finishLengthDelimited(valueStart, writer.pos - startPos);
  }
  exports.writeKeyValue = writeKeyValue;
  var MIN_64_BIT_INT = -(2 ** 63);
  var MAX_64_BIT_INT = 2 ** 63;
  function writeAnyValue(writer, value) {
    const t = typeof value;
    if (t === "string") {
      writer.writeTag(1, 2);
      writer.writeString(value);
    } else if (t === "boolean") {
      writer.writeTag(2, 0);
      writer.writeVarint(value ? 1 : 0);
    } else if (t === "number") {
      const numValue = value;
      if (Number.isInteger(numValue) && numValue >= MIN_64_BIT_INT && numValue < MAX_64_BIT_INT) {
        writer.writeTag(3, 0);
        writer.writeVarint(numValue);
      } else {
        writer.writeTag(4, 1);
        writer.writeDouble(numValue);
      }
    } else if (value instanceof Uint8Array) {
      writer.writeTag(7, 2);
      writer.writeBytes(value);
    } else if (Array.isArray(value)) {
      writer.writeTag(5, 2);
      const arrayStart = writer.startLengthDelimited();
      const arrayStartPos = writer.pos;
      for (const item of value) {
        writer.writeTag(1, 2);
        const itemStart = writer.startLengthDelimited();
        const itemStartPos = writer.pos;
        writeAnyValue(writer, item);
        writer.finishLengthDelimited(itemStart, writer.pos - itemStartPos);
      }
      writer.finishLengthDelimited(arrayStart, writer.pos - arrayStartPos);
    } else if (t === "object" && value != null) {
      writer.writeTag(6, 2);
      const kvlistStart = writer.startLengthDelimited();
      const kvlistStartPos = writer.pos;
      const obj = value;
      for (const k in obj) {
        if (!Object.prototype.hasOwnProperty.call(obj, k)) {
          continue;
        }
        const v = obj[k];
        writer.writeTag(1, 2);
        const kvStart = writer.startLengthDelimited();
        const kvStartPos = writer.pos;
        writer.writeTag(1, 2);
        writer.writeString(k);
        writer.writeTag(2, 2);
        const valueStart = writer.startLengthDelimited();
        const valueStartPos = writer.pos;
        writeAnyValue(writer, v);
        writer.finishLengthDelimited(valueStart, writer.pos - valueStartPos);
        writer.finishLengthDelimited(kvStart, writer.pos - kvStartPos);
      }
      writer.finishLengthDelimited(kvlistStart, writer.pos - kvlistStartPos);
    }
  }
  exports.writeAnyValue = writeAnyValue;
  function writeInstrumentationScope(writer, scope, fieldNumber) {
    writer.writeTag(fieldNumber, 2);
    const start = writer.startLengthDelimited();
    const startPos = writer.pos;
    writer.writeTag(1, 2);
    writer.writeString(scope.name);
    if (scope.version) {
      writer.writeTag(2, 2);
      writer.writeString(scope.version);
    }
    if (scope.attributes) {
      writeAttributes(writer, scope.attributes, 3);
    }
    if (scope.droppedAttributesCount) {
      writer.writeTag(4, 0);
      writer.writeVarint(scope.droppedAttributesCount);
    }
    writer.finishLengthDelimited(start, writer.pos - startPos);
  }
  exports.writeInstrumentationScope = writeInstrumentationScope;
  function writeResource(writer, resource, fieldNumber) {
    writer.writeTag(fieldNumber, 2);
    const resourceStart = writer.startLengthDelimited();
    const resourceStartPos = writer.pos;
    if (resource.attributes) {
      writeAttributes(writer, resource.attributes, 1);
    }
    writer.writeTag(2, 0);
    writer.writeVarint(0);
    writer.finishLengthDelimited(resourceStart, writer.pos - resourceStartPos);
  }
  exports.writeResource = writeResource;
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/common/protobuf/protobuf-size-estimator.js
var require_protobuf_size_estimator = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ProtobufSizeEstimator = undefined;
  var utils_1 = require_utils3();
  function utf8ByteLength(str) {
    const len = str.length;
    let byteLen = 0;
    for (let i = 0;i < len; i++) {
      const code = str.charCodeAt(i);
      if (code < 128) {
        byteLen += 1;
      } else if (code < 2048) {
        byteLen += 2;
      } else if (code < 55296 || code >= 57344) {
        byteLen += 3;
      } else {
        i++;
        byteLen += 4;
      }
    }
    return byteLen;
  }

  class ProtobufSizeEstimator {
    pos = 0;
    startLengthDelimited() {
      return this.pos;
    }
    finishLengthDelimited(_, length) {
      this.pos += (0, utils_1.estimateVarintSize)(length);
    }
    writeVarint(value) {
      this.pos += (0, utils_1.estimateVarintSize)(value);
    }
    writeSint32(value) {
      this.pos += (0, utils_1.estimateVarintSize)((value << 1 ^ value >> 31) >>> 0);
    }
    writeSfixed64(_value) {
      this.pos += 8;
    }
    writeFixed32(_value) {
      this.pos += 4;
    }
    writeFixed64(_low, _high) {
      this.pos += 8;
    }
    writeBytes(bytes) {
      this.pos += (0, utils_1.estimateVarintSize)(bytes.length);
      this.pos += bytes.length;
    }
    writeTag(fieldNumber, wireType) {
      this.writeVarint(fieldNumber << 3 | wireType);
    }
    writeDouble(_value) {
      this.pos += 8;
    }
    writeString(str) {
      const byteLen = utf8ByteLength(str);
      this.pos += (0, utils_1.estimateVarintSize)(byteLen);
      this.pos += byteLen;
    }
  }
  exports.ProtobufSizeEstimator = ProtobufSizeEstimator;
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/logs/protobuf/logs-serializer.js
var require_logs_serializer = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.serializeLogsExportRequest = undefined;
  var protobuf_writer_1 = require_protobuf_writer();
  var hex_to_binary_1 = require_hex_to_binary();
  var api_logs_1 = require_src2();
  var common_serializer_1 = require_common_serializer();
  var protobuf_size_estimator_1 = require_protobuf_size_estimator();
  function serializeLogRecord(writer, logRecord) {
    const logStart = writer.startLengthDelimited();
    const logStartPos = writer.pos;
    writer.writeTag(1, 1);
    (0, common_serializer_1.writeHrTimeAsFixed64)(writer, logRecord.hrTime);
    if (logRecord.severityNumber !== undefined && logRecord.severityNumber !== api_logs_1.SeverityNumber.UNSPECIFIED) {
      writer.writeTag(2, 0);
      writer.writeVarint(logRecord.severityNumber);
    }
    if (logRecord.severityText) {
      writer.writeTag(3, 2);
      writer.writeString(logRecord.severityText);
    }
    if (logRecord.body !== undefined) {
      writer.writeTag(5, 2);
      const bodyStart = writer.startLengthDelimited();
      const bodyStartPos = writer.pos;
      (0, common_serializer_1.writeAnyValue)(writer, logRecord.body);
      writer.finishLengthDelimited(bodyStart, writer.pos - bodyStartPos);
    }
    if (logRecord.attributes) {
      (0, common_serializer_1.writeAttributes)(writer, logRecord.attributes, 6);
    }
    writer.writeTag(7, 0);
    writer.writeVarint(logRecord.droppedAttributesCount);
    if (logRecord.spanContext?.traceFlags) {
      writer.writeTag(8, 5);
      writer.writeFixed32(logRecord.spanContext.traceFlags);
    }
    if (logRecord.spanContext?.traceId) {
      writer.writeTag(9, 2);
      writer.writeBytes((0, hex_to_binary_1.hexToBinary)(logRecord.spanContext.traceId));
    }
    if (logRecord.spanContext?.spanId) {
      writer.writeTag(10, 2);
      writer.writeBytes((0, hex_to_binary_1.hexToBinary)(logRecord.spanContext.spanId));
    }
    writer.writeTag(11, 1);
    (0, common_serializer_1.writeHrTimeAsFixed64)(writer, logRecord.hrTimeObserved);
    if (logRecord.eventName) {
      writer.writeTag(12, 2);
      writer.writeString(logRecord.eventName);
    }
    writer.finishLengthDelimited(logStart, writer.pos - logStartPos);
  }
  function serializeScopeLogs(writer, scope, logRecords) {
    const scopeLogsStart = writer.startLengthDelimited();
    const scopeLogsStartPos = writer.pos;
    (0, common_serializer_1.writeInstrumentationScope)(writer, scope, 1);
    for (const logRecord of logRecords) {
      writer.writeTag(2, 2);
      serializeLogRecord(writer, logRecord);
    }
    if (scope.schemaUrl) {
      writer.writeTag(3, 2);
      writer.writeString(scope.schemaUrl);
    }
    writer.finishLengthDelimited(scopeLogsStart, writer.pos - scopeLogsStartPos);
  }
  function serializeResourceLogs(writer, resource, scopeMap) {
    const resourceLogsStart = writer.startLengthDelimited();
    const resourceLogsStartPos = writer.pos;
    (0, common_serializer_1.writeResource)(writer, resource, 1);
    for (const scopeLogs of scopeMap.values()) {
      writer.writeTag(2, 2);
      const scope = scopeLogs[0].instrumentationScope;
      serializeScopeLogs(writer, scope, scopeLogs);
    }
    if (resource.schemaUrl) {
      writer.writeTag(3, 2);
      writer.writeString(resource.schemaUrl);
    }
    writer.finishLengthDelimited(resourceLogsStart, writer.pos - resourceLogsStartPos);
  }
  function createResourceMap(logRecords) {
    const resourceMap = new Map;
    for (const record of logRecords) {
      const resource = record.resource;
      const scope = record.instrumentationScope;
      let ismMap = resourceMap.get(resource);
      if (!ismMap) {
        ismMap = new Map;
        resourceMap.set(resource, ismMap);
      }
      let records = ismMap.get(scope);
      if (!records) {
        records = [];
        ismMap.set(scope, records);
      }
      records.push(record);
    }
    return resourceMap;
  }
  function serializeLogsExportRequest(logRecords) {
    const resourceMap = createResourceMap(logRecords);
    const estimator = new protobuf_size_estimator_1.ProtobufSizeEstimator;
    for (const [resource, scopeMap] of resourceMap) {
      estimator.writeTag(1, 2);
      serializeResourceLogs(estimator, resource, scopeMap);
    }
    const writer = new protobuf_writer_1.ProtobufWriter(estimator.pos);
    for (const [resource, scopeMap] of resourceMap) {
      writer.writeTag(1, 2);
      serializeResourceLogs(writer, resource, scopeMap);
    }
    return writer.finish();
  }
  exports.serializeLogsExportRequest = serializeLogsExportRequest;
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/common/protobuf/protobuf-reader.js
var require_protobuf_reader = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ProtobufReader = undefined;

  class ProtobufReader {
    pos = 0;
    _buf;
    _textDecoder;
    constructor(buf) {
      this._buf = buf;
      this._textDecoder = new TextDecoder;
    }
    isAtEnd() {
      return this.pos >= this._buf.length;
    }
    readTag() {
      const raw = this.readVarint();
      return { fieldNumber: raw >>> 3, wireType: raw & 7 };
    }
    readVarint() {
      let result = 0;
      let shift = 0;
      let terminated = false;
      while (this.pos < this._buf.length) {
        const b = this._buf[this.pos++];
        result += (b & 127) * Math.pow(2, shift);
        shift += 7;
        if ((b & 128) === 0) {
          terminated = true;
          break;
        }
      }
      if (!terminated) {
        throw new Error("Truncated buffer: unexpected end of data while reading varint");
      }
      return result;
    }
    readBytes() {
      const len = this.readVarint();
      if (this.pos + len > this._buf.length) {
        throw new Error(`Truncated buffer: expected ${len} bytes at position ${this.pos}, but only ${this._buf.length - this.pos} available`);
      }
      const slice = this._buf.subarray(this.pos, this.pos + len);
      this.pos += len;
      return slice;
    }
    readString() {
      return this._textDecoder.decode(this.readBytes());
    }
    skip(wireType) {
      switch (wireType) {
        case 0:
          this.readVarint();
          break;
        case 1:
          this.pos += 8;
          break;
        case 2:
          this.readBytes();
          break;
        case 5:
          this.pos += 4;
          break;
        default:
          throw new Error(`Unknown wire type ${wireType}, cannot safely skip`);
      }
    }
  }
  exports.ProtobufReader = ProtobufReader;
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/logs/protobuf/response-deserializer.js
var require_response_deserializer = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.deserializeExportLogsServiceResponse = undefined;
  var protobuf_reader_1 = require_protobuf_reader();
  function deserializePartialSuccess(data) {
    const reader = new protobuf_reader_1.ProtobufReader(data);
    const result = {};
    while (!reader.isAtEnd()) {
      const { fieldNumber, wireType } = reader.readTag();
      switch (fieldNumber) {
        case 1:
          if (wireType === 0) {
            result.rejectedLogRecords = reader.readVarint();
          } else {
            reader.skip(wireType);
          }
          break;
        case 2:
          if (wireType === 2) {
            result.errorMessage = reader.readString();
          } else {
            reader.skip(wireType);
          }
          break;
        default:
          reader.skip(wireType);
          break;
      }
    }
    return result;
  }
  function deserializeExportLogsServiceResponse(data) {
    const reader = new protobuf_reader_1.ProtobufReader(data);
    const result = {};
    while (!reader.isAtEnd()) {
      const { fieldNumber, wireType } = reader.readTag();
      switch (fieldNumber) {
        case 1:
          if (wireType === 2) {
            result.partialSuccess = deserializePartialSuccess(reader.readBytes());
          } else {
            reader.skip(wireType);
          }
          break;
        default:
          reader.skip(wireType);
          break;
      }
    }
    return result;
  }
  exports.deserializeExportLogsServiceResponse = deserializeExportLogsServiceResponse;
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/logs/protobuf/logs.js
var require_logs2 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ProtobufLogsSerializer = undefined;
  var logs_serializer_1 = require_logs_serializer();
  var response_deserializer_1 = require_response_deserializer();
  exports.ProtobufLogsSerializer = {
    serializeRequest: (arg) => {
      return (0, logs_serializer_1.serializeLogsExportRequest)(arg);
    },
    deserializeResponse: (arg) => {
      return (0, response_deserializer_1.deserializeExportLogsServiceResponse)(arg);
    }
  };
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/logs/protobuf/index.js
var require_protobuf = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ProtobufLogsSerializer = undefined;
  var logs_1 = require_logs2();
  Object.defineProperty(exports, "ProtobufLogsSerializer", { enumerable: true, get: function() {
    return logs_1.ProtobufLogsSerializer;
  } });
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/export/AggregationTemporality.js
var require_AggregationTemporality = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.AggregationTemporality = undefined;
  var AggregationTemporality;
  (function(AggregationTemporality) {
    AggregationTemporality[AggregationTemporality["DELTA"] = 0] = "DELTA";
    AggregationTemporality[AggregationTemporality["CUMULATIVE"] = 1] = "CUMULATIVE";
  })(AggregationTemporality || (exports.AggregationTemporality = AggregationTemporality = {}));
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/export/MetricData.js
var require_MetricData = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.DataPointType = exports.InstrumentType = undefined;
  var InstrumentType;
  (function(InstrumentType) {
    InstrumentType["COUNTER"] = "COUNTER";
    InstrumentType["GAUGE"] = "GAUGE";
    InstrumentType["HISTOGRAM"] = "HISTOGRAM";
    InstrumentType["UP_DOWN_COUNTER"] = "UP_DOWN_COUNTER";
    InstrumentType["OBSERVABLE_COUNTER"] = "OBSERVABLE_COUNTER";
    InstrumentType["OBSERVABLE_GAUGE"] = "OBSERVABLE_GAUGE";
    InstrumentType["OBSERVABLE_UP_DOWN_COUNTER"] = "OBSERVABLE_UP_DOWN_COUNTER";
  })(InstrumentType || (exports.InstrumentType = InstrumentType = {}));
  var DataPointType;
  (function(DataPointType) {
    DataPointType[DataPointType["HISTOGRAM"] = 0] = "HISTOGRAM";
    DataPointType[DataPointType["EXPONENTIAL_HISTOGRAM"] = 1] = "EXPONENTIAL_HISTOGRAM";
    DataPointType[DataPointType["GAUGE"] = 2] = "GAUGE";
    DataPointType[DataPointType["SUM"] = 3] = "SUM";
  })(DataPointType || (exports.DataPointType = DataPointType = {}));
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/utils.js
var require_utils4 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.equalsCaseInsensitive = exports.binarySearchUB = exports.setEquals = exports.callWithTimeout = exports.TimeoutError = exports.instrumentationScopeId = exports.hashAttributes = undefined;
  function hashAttributes(attributes) {
    let keys = Object.keys(attributes);
    if (keys.length === 0)
      return "";
    keys = keys.sort();
    return JSON.stringify(keys.map((key) => [key, attributes[key]]));
  }
  exports.hashAttributes = hashAttributes;
  function instrumentationScopeId(instrumentationScope) {
    return `${instrumentationScope.name}:${instrumentationScope.version ?? ""}:${instrumentationScope.schemaUrl ?? ""}`;
  }
  exports.instrumentationScopeId = instrumentationScopeId;

  class TimeoutError extends Error {
    constructor(message) {
      super(message);
      Object.setPrototypeOf(this, TimeoutError.prototype);
    }
  }
  exports.TimeoutError = TimeoutError;
  function callWithTimeout(promise, timeout) {
    let timeoutHandle;
    const timeoutPromise = new Promise(function timeoutFunction(_resolve, reject) {
      timeoutHandle = setTimeout(function timeoutHandler() {
        reject(new TimeoutError("Operation timed out."));
      }, timeout);
    });
    return Promise.race([promise, timeoutPromise]).then((result) => {
      clearTimeout(timeoutHandle);
      return result;
    }, (reason) => {
      clearTimeout(timeoutHandle);
      throw reason;
    });
  }
  exports.callWithTimeout = callWithTimeout;
  function setEquals(lhs, rhs) {
    if (lhs.size !== rhs.size) {
      return false;
    }
    for (const item of lhs) {
      if (!rhs.has(item)) {
        return false;
      }
    }
    return true;
  }
  exports.setEquals = setEquals;
  function binarySearchUB(arr, value) {
    let lo = 0;
    let hi = arr.length - 1;
    let ret = arr.length;
    while (hi >= lo) {
      const mid = lo + Math.trunc((hi - lo) / 2);
      if (arr[mid] < value) {
        lo = mid + 1;
      } else {
        ret = mid;
        hi = mid - 1;
      }
    }
    return ret;
  }
  exports.binarySearchUB = binarySearchUB;
  function equalsCaseInsensitive(lhs, rhs) {
    return lhs.toLowerCase() === rhs.toLowerCase();
  }
  exports.equalsCaseInsensitive = equalsCaseInsensitive;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/aggregator/types.js
var require_types2 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.AggregatorKind = undefined;
  var AggregatorKind;
  (function(AggregatorKind) {
    AggregatorKind[AggregatorKind["DROP"] = 0] = "DROP";
    AggregatorKind[AggregatorKind["SUM"] = 1] = "SUM";
    AggregatorKind[AggregatorKind["LAST_VALUE"] = 2] = "LAST_VALUE";
    AggregatorKind[AggregatorKind["HISTOGRAM"] = 3] = "HISTOGRAM";
    AggregatorKind[AggregatorKind["EXPONENTIAL_HISTOGRAM"] = 4] = "EXPONENTIAL_HISTOGRAM";
  })(AggregatorKind || (exports.AggregatorKind = AggregatorKind = {}));
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/aggregator/Drop.js
var require_Drop = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.DropAggregator = undefined;
  var types_1 = require_types2();

  class DropAggregator {
    kind = types_1.AggregatorKind.DROP;
    createAccumulation() {
      return;
    }
    merge(_previous, _delta) {
      return;
    }
    diff(_previous, _current) {
      return;
    }
    toMetricData(_descriptor, _aggregationTemporality, _accumulationByAttributes, _endTime) {
      return;
    }
  }
  exports.DropAggregator = DropAggregator;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/aggregator/Histogram.js
var require_Histogram = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.HistogramAggregator = exports.HistogramAccumulation = undefined;
  var types_1 = require_types2();
  var MetricData_1 = require_MetricData();
  var utils_1 = require_utils4();
  function createNewEmptyCheckpoint(boundaries) {
    const counts = boundaries.map(() => 0);
    counts.push(0);
    return {
      buckets: {
        boundaries,
        counts
      },
      sum: 0,
      count: 0,
      hasMinMax: false,
      min: Infinity,
      max: -Infinity
    };
  }

  class HistogramAccumulation {
    startTime;
    _boundaries;
    _recordMinMax;
    _current;
    constructor(startTime, boundaries, recordMinMax = true, current = createNewEmptyCheckpoint(boundaries)) {
      this.startTime = startTime;
      this._boundaries = boundaries;
      this._recordMinMax = recordMinMax;
      this._current = current;
    }
    record(value) {
      if (Number.isNaN(value)) {
        return;
      }
      this._current.count += 1;
      this._current.sum += value;
      if (this._recordMinMax) {
        this._current.min = Math.min(value, this._current.min);
        this._current.max = Math.max(value, this._current.max);
        this._current.hasMinMax = true;
      }
      const idx = (0, utils_1.binarySearchUB)(this._boundaries, value);
      this._current.buckets.counts[idx] += 1;
    }
    setStartTime(startTime) {
      this.startTime = startTime;
    }
    toPointValue() {
      return this._current;
    }
  }
  exports.HistogramAccumulation = HistogramAccumulation;

  class HistogramAggregator {
    kind = types_1.AggregatorKind.HISTOGRAM;
    _boundaries;
    _recordMinMax;
    constructor(boundaries, recordMinMax) {
      this._boundaries = boundaries;
      this._recordMinMax = recordMinMax;
    }
    createAccumulation(startTime) {
      return new HistogramAccumulation(startTime, this._boundaries, this._recordMinMax);
    }
    merge(previous, delta) {
      const previousValue = previous.toPointValue();
      const deltaValue = delta.toPointValue();
      const previousCounts = previousValue.buckets.counts;
      const deltaCounts = deltaValue.buckets.counts;
      const mergedCounts = new Array(previousCounts.length);
      for (let idx = 0;idx < previousCounts.length; idx++) {
        mergedCounts[idx] = previousCounts[idx] + deltaCounts[idx];
      }
      let min = Infinity;
      let max = -Infinity;
      if (this._recordMinMax) {
        if (previousValue.hasMinMax && deltaValue.hasMinMax) {
          min = Math.min(previousValue.min, deltaValue.min);
          max = Math.max(previousValue.max, deltaValue.max);
        } else if (previousValue.hasMinMax) {
          min = previousValue.min;
          max = previousValue.max;
        } else if (deltaValue.hasMinMax) {
          min = deltaValue.min;
          max = deltaValue.max;
        }
      }
      return new HistogramAccumulation(previous.startTime, previousValue.buckets.boundaries, this._recordMinMax, {
        buckets: {
          boundaries: previousValue.buckets.boundaries,
          counts: mergedCounts
        },
        count: previousValue.count + deltaValue.count,
        sum: previousValue.sum + deltaValue.sum,
        hasMinMax: this._recordMinMax && (previousValue.hasMinMax || deltaValue.hasMinMax),
        min,
        max
      });
    }
    diff(previous, current) {
      const previousValue = previous.toPointValue();
      const currentValue = current.toPointValue();
      const previousCounts = previousValue.buckets.counts;
      const currentCounts = currentValue.buckets.counts;
      const diffedCounts = new Array(previousCounts.length);
      for (let idx = 0;idx < previousCounts.length; idx++) {
        diffedCounts[idx] = currentCounts[idx] - previousCounts[idx];
      }
      return new HistogramAccumulation(current.startTime, previousValue.buckets.boundaries, this._recordMinMax, {
        buckets: {
          boundaries: previousValue.buckets.boundaries,
          counts: diffedCounts
        },
        count: currentValue.count - previousValue.count,
        sum: currentValue.sum - previousValue.sum,
        hasMinMax: false,
        min: Infinity,
        max: -Infinity
      });
    }
    toMetricData(descriptor, aggregationTemporality, accumulationByAttributes, endTime) {
      return {
        descriptor,
        aggregationTemporality,
        dataPointType: MetricData_1.DataPointType.HISTOGRAM,
        dataPoints: accumulationByAttributes.map(([attributes, accumulation]) => {
          const pointValue = accumulation.toPointValue();
          const allowsNegativeValues = descriptor.type === MetricData_1.InstrumentType.GAUGE || descriptor.type === MetricData_1.InstrumentType.UP_DOWN_COUNTER || descriptor.type === MetricData_1.InstrumentType.OBSERVABLE_GAUGE || descriptor.type === MetricData_1.InstrumentType.OBSERVABLE_UP_DOWN_COUNTER;
          return {
            attributes,
            startTime: accumulation.startTime,
            endTime,
            value: {
              min: pointValue.hasMinMax ? pointValue.min : undefined,
              max: pointValue.hasMinMax ? pointValue.max : undefined,
              sum: !allowsNegativeValues ? pointValue.sum : undefined,
              buckets: pointValue.buckets,
              count: pointValue.count
            }
          };
        })
      };
    }
  }
  exports.HistogramAggregator = HistogramAggregator;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/aggregator/exponential-histogram/Buckets.js
var require_Buckets = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.Buckets = undefined;

  class Buckets {
    backing;
    indexBase;
    indexStart;
    indexEnd;
    constructor(backing = new BucketsBacking, indexBase = 0, indexStart = 0, indexEnd = 0) {
      this.backing = backing;
      this.indexBase = indexBase;
      this.indexStart = indexStart;
      this.indexEnd = indexEnd;
    }
    get offset() {
      return this.indexStart;
    }
    get length() {
      if (this.backing.length === 0) {
        return 0;
      }
      if (this.indexEnd === this.indexStart && this.at(0) === 0) {
        return 0;
      }
      return this.indexEnd - this.indexStart + 1;
    }
    counts() {
      return Array.from({ length: this.length }, (_, i) => this.at(i));
    }
    at(position) {
      const bias = this.indexBase - this.indexStart;
      if (position < bias) {
        position += this.backing.length;
      }
      position -= bias;
      return this.backing.countAt(position);
    }
    incrementBucket(bucketIndex, increment) {
      this.backing.increment(bucketIndex, increment);
    }
    decrementBucket(bucketIndex, decrement) {
      this.backing.decrement(bucketIndex, decrement);
    }
    trim() {
      for (let i = 0;i < this.length; i++) {
        if (this.at(i) !== 0) {
          this.indexStart += i;
          break;
        } else if (i === this.length - 1) {
          this.indexStart = this.indexEnd = this.indexBase = 0;
          return;
        }
      }
      for (let i = this.length - 1;i >= 0; i--) {
        if (this.at(i) !== 0) {
          this.indexEnd -= this.length - i - 1;
          break;
        }
      }
      this._rotate();
    }
    downscale(by) {
      this._rotate();
      const size = 1 + this.indexEnd - this.indexStart;
      const each = 1 << by;
      let inpos = 0;
      let outpos = 0;
      for (let pos = this.indexStart;pos <= this.indexEnd; ) {
        let mod = pos % each;
        if (mod < 0) {
          mod += each;
        }
        for (let i = mod;i < each && inpos < size; i++) {
          this._relocateBucket(outpos, inpos);
          inpos++;
          pos++;
        }
        outpos++;
      }
      this.indexStart >>= by;
      this.indexEnd >>= by;
      this.indexBase = this.indexStart;
    }
    clone() {
      return new Buckets(this.backing.clone(), this.indexBase, this.indexStart, this.indexEnd);
    }
    _rotate() {
      const bias = this.indexBase - this.indexStart;
      if (bias === 0) {
        return;
      } else if (bias > 0) {
        this.backing.reverse(0, this.backing.length);
        this.backing.reverse(0, bias);
        this.backing.reverse(bias, this.backing.length);
      } else {
        this.backing.reverse(0, this.backing.length);
        this.backing.reverse(0, this.backing.length + bias);
      }
      this.indexBase = this.indexStart;
    }
    _relocateBucket(dest, src) {
      if (dest === src) {
        return;
      }
      this.incrementBucket(dest, this.backing.emptyBucket(src));
    }
  }
  exports.Buckets = Buckets;

  class BucketsBacking {
    _counts;
    constructor(counts = [0]) {
      this._counts = counts;
    }
    get length() {
      return this._counts.length;
    }
    countAt(pos) {
      return this._counts[pos];
    }
    growTo(newSize, oldPositiveLimit, newPositiveLimit) {
      const tmp = new Array(newSize).fill(0);
      tmp.splice(newPositiveLimit, this._counts.length - oldPositiveLimit, ...this._counts.slice(oldPositiveLimit));
      tmp.splice(0, oldPositiveLimit, ...this._counts.slice(0, oldPositiveLimit));
      this._counts = tmp;
    }
    reverse(from, limit) {
      const num = Math.floor((from + limit) / 2) - from;
      for (let i = 0;i < num; i++) {
        const tmp = this._counts[from + i];
        this._counts[from + i] = this._counts[limit - i - 1];
        this._counts[limit - i - 1] = tmp;
      }
    }
    emptyBucket(src) {
      const tmp = this._counts[src];
      this._counts[src] = 0;
      return tmp;
    }
    increment(bucketIndex, increment) {
      this._counts[bucketIndex] += increment;
    }
    decrement(bucketIndex, decrement) {
      if (this._counts[bucketIndex] >= decrement) {
        this._counts[bucketIndex] -= decrement;
      } else {
        this._counts[bucketIndex] = 0;
      }
    }
    clone() {
      return new BucketsBacking([...this._counts]);
    }
  }
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/aggregator/exponential-histogram/mapping/ieee754.js
var require_ieee754 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.isPowerOfTwo = exports.getNormalBase2 = exports.MIN_VALUE = exports.MAX_NORMAL_EXPONENT = exports.MIN_NORMAL_EXPONENT = undefined;
  var EXPONENT_MASK = 2146435072;
  var SIGNIFICAND_MASK = 1048575;
  var EXPONENT_BIAS = 1023;
  exports.MIN_NORMAL_EXPONENT = -EXPONENT_BIAS + 1;
  exports.MAX_NORMAL_EXPONENT = EXPONENT_BIAS;
  exports.MIN_VALUE = Math.pow(2, -1022);
  var dv = new DataView(new ArrayBuffer(8));
  function floatBits(value) {
    dv.setFloat64(0, value);
    return { hi: dv.getUint32(0), lo: dv.getUint32(4) };
  }
  function getNormalBase2(value) {
    const { hi } = floatBits(value);
    return ((hi & EXPONENT_MASK) >> 20) - EXPONENT_BIAS;
  }
  exports.getNormalBase2 = getNormalBase2;
  function isPowerOfTwo(value) {
    const { hi, lo } = floatBits(value);
    return (hi & SIGNIFICAND_MASK) === 0 && lo === 0;
  }
  exports.isPowerOfTwo = isPowerOfTwo;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/aggregator/exponential-histogram/util.js
var require_util = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.nextGreaterSquare = exports.ldexp = undefined;
  function ldexp(frac, exp) {
    if (frac === 0 || frac === Number.POSITIVE_INFINITY || frac === Number.NEGATIVE_INFINITY || Number.isNaN(frac)) {
      return frac;
    }
    return frac * Math.pow(2, exp);
  }
  exports.ldexp = ldexp;
  function nextGreaterSquare(v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    return v;
  }
  exports.nextGreaterSquare = nextGreaterSquare;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/aggregator/exponential-histogram/mapping/types.js
var require_types3 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.MappingError = undefined;

  class MappingError extends Error {
  }
  exports.MappingError = MappingError;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/aggregator/exponential-histogram/mapping/ExponentMapping.js
var require_ExponentMapping = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ExponentMapping = undefined;
  var ieee754 = require_ieee754();
  var util = require_util();
  var types_1 = require_types3();

  class ExponentMapping {
    _shift;
    constructor(scale) {
      this._shift = -scale;
    }
    mapToIndex(value) {
      if (value < ieee754.MIN_VALUE) {
        return this._minNormalLowerBoundaryIndex();
      }
      const exp = ieee754.getNormalBase2(value);
      const correction = ieee754.isPowerOfTwo(value) ? -1 : 0;
      return exp + correction >> this._shift;
    }
    lowerBoundary(index) {
      const minIndex = this._minNormalLowerBoundaryIndex();
      if (index < minIndex) {
        throw new types_1.MappingError(`underflow: ${index} is < minimum lower boundary: ${minIndex}`);
      }
      const maxIndex = this._maxNormalLowerBoundaryIndex();
      if (index > maxIndex) {
        throw new types_1.MappingError(`overflow: ${index} is > maximum lower boundary: ${maxIndex}`);
      }
      return util.ldexp(1, index << this._shift);
    }
    get scale() {
      if (this._shift === 0) {
        return 0;
      }
      return -this._shift;
    }
    _minNormalLowerBoundaryIndex() {
      let index = ieee754.MIN_NORMAL_EXPONENT >> this._shift;
      if (this._shift < 2) {
        index--;
      }
      return index;
    }
    _maxNormalLowerBoundaryIndex() {
      return ieee754.MAX_NORMAL_EXPONENT >> this._shift;
    }
  }
  exports.ExponentMapping = ExponentMapping;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/aggregator/exponential-histogram/mapping/LogarithmMapping.js
var require_LogarithmMapping = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.LogarithmMapping = undefined;
  var ieee754 = require_ieee754();
  var util = require_util();
  var types_1 = require_types3();

  class LogarithmMapping {
    _scale;
    _scaleFactor;
    _inverseFactor;
    constructor(scale) {
      this._scale = scale;
      this._scaleFactor = util.ldexp(Math.LOG2E, scale);
      this._inverseFactor = util.ldexp(Math.LN2, -scale);
    }
    mapToIndex(value) {
      if (value <= ieee754.MIN_VALUE) {
        return this._minNormalLowerBoundaryIndex() - 1;
      }
      if (ieee754.isPowerOfTwo(value)) {
        const exp = ieee754.getNormalBase2(value);
        return (exp << this._scale) - 1;
      }
      const index = Math.floor(Math.log(value) * this._scaleFactor);
      const maxIndex = this._maxNormalLowerBoundaryIndex();
      if (index >= maxIndex) {
        return maxIndex;
      }
      return index;
    }
    lowerBoundary(index) {
      const maxIndex = this._maxNormalLowerBoundaryIndex();
      if (index >= maxIndex) {
        if (index === maxIndex) {
          return 2 * Math.exp((index - (1 << this._scale)) / this._scaleFactor);
        }
        throw new types_1.MappingError(`overflow: ${index} is > maximum lower boundary: ${maxIndex}`);
      }
      const minIndex = this._minNormalLowerBoundaryIndex();
      if (index <= minIndex) {
        if (index === minIndex) {
          return ieee754.MIN_VALUE;
        } else if (index === minIndex - 1) {
          return Math.exp((index + (1 << this._scale)) / this._scaleFactor) / 2;
        }
        throw new types_1.MappingError(`overflow: ${index} is < minimum lower boundary: ${minIndex}`);
      }
      return Math.exp(index * this._inverseFactor);
    }
    get scale() {
      return this._scale;
    }
    _minNormalLowerBoundaryIndex() {
      return ieee754.MIN_NORMAL_EXPONENT << this._scale;
    }
    _maxNormalLowerBoundaryIndex() {
      return (ieee754.MAX_NORMAL_EXPONENT + 1 << this._scale) - 1;
    }
  }
  exports.LogarithmMapping = LogarithmMapping;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/aggregator/exponential-histogram/mapping/getMapping.js
var require_getMapping = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.getMapping = undefined;
  var ExponentMapping_1 = require_ExponentMapping();
  var LogarithmMapping_1 = require_LogarithmMapping();
  var types_1 = require_types3();
  var MIN_SCALE = -10;
  var MAX_SCALE = 20;
  var PREBUILT_MAPPINGS = Array.from({ length: 31 }, (_, i) => {
    if (i > 10) {
      return new LogarithmMapping_1.LogarithmMapping(i - 10);
    }
    return new ExponentMapping_1.ExponentMapping(i - 10);
  });
  function getMapping(scale) {
    if (scale > MAX_SCALE || scale < MIN_SCALE) {
      throw new types_1.MappingError(`expected scale >= ${MIN_SCALE} && <= ${MAX_SCALE}, got: ${scale}`);
    }
    return PREBUILT_MAPPINGS[scale + 10];
  }
  exports.getMapping = getMapping;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/aggregator/ExponentialHistogram.js
var require_ExponentialHistogram = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ExponentialHistogramAggregator = exports.ExponentialHistogramAccumulation = undefined;
  var types_1 = require_types2();
  var MetricData_1 = require_MetricData();
  var api_1 = require_src();
  var Buckets_1 = require_Buckets();
  var getMapping_1 = require_getMapping();
  var util_1 = require_util();

  class HighLow {
    static combine(h1, h2) {
      return new HighLow(Math.min(h1.low, h2.low), Math.max(h1.high, h2.high));
    }
    low;
    high;
    constructor(low, high) {
      this.low = low;
      this.high = high;
    }
  }
  var MAX_SCALE = 20;
  var DEFAULT_MAX_SIZE = 160;
  var MIN_MAX_SIZE = 2;

  class ExponentialHistogramAccumulation {
    startTime;
    _maxSize;
    _recordMinMax;
    _sum;
    _count;
    _zeroCount;
    _min;
    _max;
    _positive;
    _negative;
    _mapping;
    constructor(startTime, maxSize = DEFAULT_MAX_SIZE, recordMinMax = true, sum = 0, count = 0, zeroCount = 0, min = Number.POSITIVE_INFINITY, max = Number.NEGATIVE_INFINITY, positive = new Buckets_1.Buckets, negative = new Buckets_1.Buckets, mapping = (0, getMapping_1.getMapping)(MAX_SCALE)) {
      this.startTime = startTime;
      this._maxSize = maxSize;
      this._recordMinMax = recordMinMax;
      this._sum = sum;
      this._count = count;
      this._zeroCount = zeroCount;
      this._min = min;
      this._max = max;
      this._positive = positive;
      this._negative = negative;
      this._mapping = mapping;
      if (this._maxSize < MIN_MAX_SIZE) {
        api_1.diag.warn(`Exponential Histogram Max Size set to ${this._maxSize},                 changing to the minimum size of: ${MIN_MAX_SIZE}`);
        this._maxSize = MIN_MAX_SIZE;
      }
    }
    record(value) {
      this.updateByIncrement(value, 1);
    }
    setStartTime(startTime) {
      this.startTime = startTime;
    }
    toPointValue() {
      return {
        hasMinMax: this._recordMinMax,
        min: this.min,
        max: this.max,
        sum: this.sum,
        positive: {
          offset: this.positive.offset,
          bucketCounts: this.positive.counts()
        },
        negative: {
          offset: this.negative.offset,
          bucketCounts: this.negative.counts()
        },
        count: this.count,
        scale: this.scale,
        zeroCount: this.zeroCount
      };
    }
    get sum() {
      return this._sum;
    }
    get min() {
      return this._min;
    }
    get max() {
      return this._max;
    }
    get count() {
      return this._count;
    }
    get zeroCount() {
      return this._zeroCount;
    }
    get scale() {
      if (this._count === this._zeroCount) {
        return 0;
      }
      return this._mapping.scale;
    }
    get positive() {
      return this._positive;
    }
    get negative() {
      return this._negative;
    }
    updateByIncrement(value, increment) {
      if (!Number.isFinite(value)) {
        return;
      }
      if (value > this._max) {
        this._max = value;
      }
      if (value < this._min) {
        this._min = value;
      }
      this._count += increment;
      if (value === 0) {
        this._zeroCount += increment;
        return;
      }
      this._sum += value * increment;
      if (value > 0) {
        this._updateBuckets(this._positive, value, increment);
      } else {
        this._updateBuckets(this._negative, -value, increment);
      }
    }
    merge(previous) {
      if (this._count === 0) {
        this._min = previous.min;
        this._max = previous.max;
      } else if (previous.count !== 0) {
        if (previous.min < this.min) {
          this._min = previous.min;
        }
        if (previous.max > this.max) {
          this._max = previous.max;
        }
      }
      this.startTime = previous.startTime;
      this._sum += previous.sum;
      this._count += previous.count;
      this._zeroCount += previous.zeroCount;
      const minScale = this._minScale(previous);
      this._downscale(this.scale - minScale);
      this._mergeBuckets(this.positive, previous, previous.positive, minScale);
      this._mergeBuckets(this.negative, previous, previous.negative, minScale);
    }
    diff(other) {
      this._min = Infinity;
      this._max = -Infinity;
      this._sum -= other.sum;
      this._count -= other.count;
      this._zeroCount -= other.zeroCount;
      const minScale = this._minScale(other);
      this._downscale(this.scale - minScale);
      this._diffBuckets(this.positive, other, other.positive, minScale);
      this._diffBuckets(this.negative, other, other.negative, minScale);
    }
    clone() {
      return new ExponentialHistogramAccumulation(this.startTime, this._maxSize, this._recordMinMax, this._sum, this._count, this._zeroCount, this._min, this._max, this.positive.clone(), this.negative.clone(), this._mapping);
    }
    _updateBuckets(buckets, value, increment) {
      let index = this._mapping.mapToIndex(value);
      let rescalingNeeded = false;
      let high = 0;
      let low = 0;
      if (buckets.length === 0) {
        buckets.indexStart = index;
        buckets.indexEnd = buckets.indexStart;
        buckets.indexBase = buckets.indexStart;
      } else if (index < buckets.indexStart && buckets.indexEnd - index >= this._maxSize) {
        rescalingNeeded = true;
        low = index;
        high = buckets.indexEnd;
      } else if (index > buckets.indexEnd && index - buckets.indexStart >= this._maxSize) {
        rescalingNeeded = true;
        low = buckets.indexStart;
        high = index;
      }
      if (rescalingNeeded) {
        const change = this._changeScale(high, low);
        this._downscale(change);
        index = this._mapping.mapToIndex(value);
      }
      this._incrementIndexBy(buckets, index, increment);
    }
    _incrementIndexBy(buckets, index, increment) {
      if (increment === 0) {
        return;
      }
      if (buckets.length === 0) {
        buckets.indexStart = buckets.indexEnd = buckets.indexBase = index;
      }
      if (index < buckets.indexStart) {
        const span = buckets.indexEnd - index;
        if (span >= buckets.backing.length) {
          this._grow(buckets, span + 1);
        }
        buckets.indexStart = index;
      } else if (index > buckets.indexEnd) {
        const span = index - buckets.indexStart;
        if (span >= buckets.backing.length) {
          this._grow(buckets, span + 1);
        }
        buckets.indexEnd = index;
      }
      let bucketIndex = index - buckets.indexBase;
      if (bucketIndex < 0) {
        bucketIndex += buckets.backing.length;
      }
      buckets.incrementBucket(bucketIndex, increment);
    }
    _grow(buckets, needed) {
      const size = buckets.backing.length;
      const bias = buckets.indexBase - buckets.indexStart;
      const oldPositiveLimit = size - bias;
      let newSize = (0, util_1.nextGreaterSquare)(needed);
      if (newSize > this._maxSize) {
        newSize = this._maxSize;
      }
      const newPositiveLimit = newSize - bias;
      buckets.backing.growTo(newSize, oldPositiveLimit, newPositiveLimit);
    }
    _changeScale(high, low) {
      let change = 0;
      while (high - low >= this._maxSize) {
        high >>= 1;
        low >>= 1;
        change++;
      }
      return change;
    }
    _downscale(change) {
      if (change === 0) {
        return;
      }
      if (change < 0) {
        throw new Error(`impossible change of scale: ${this.scale}`);
      }
      const newScale = this._mapping.scale - change;
      this._positive.downscale(change);
      this._negative.downscale(change);
      this._mapping = (0, getMapping_1.getMapping)(newScale);
    }
    _minScale(other) {
      const minScale = Math.min(this.scale, other.scale);
      const highLowPos = HighLow.combine(this._highLowAtScale(this.positive, this.scale, minScale), this._highLowAtScale(other.positive, other.scale, minScale));
      const highLowNeg = HighLow.combine(this._highLowAtScale(this.negative, this.scale, minScale), this._highLowAtScale(other.negative, other.scale, minScale));
      return Math.min(minScale - this._changeScale(highLowPos.high, highLowPos.low), minScale - this._changeScale(highLowNeg.high, highLowNeg.low));
    }
    _highLowAtScale(buckets, currentScale, newScale) {
      if (buckets.length === 0) {
        return new HighLow(0, -1);
      }
      const shift = currentScale - newScale;
      return new HighLow(buckets.indexStart >> shift, buckets.indexEnd >> shift);
    }
    _mergeBuckets(ours, other, theirs, scale) {
      const theirOffset = theirs.offset;
      const theirChange = other.scale - scale;
      for (let i = 0;i < theirs.length; i++) {
        this._incrementIndexBy(ours, theirOffset + i >> theirChange, theirs.at(i));
      }
    }
    _diffBuckets(ours, other, theirs, scale) {
      const theirOffset = theirs.offset;
      const theirChange = other.scale - scale;
      for (let i = 0;i < theirs.length; i++) {
        const ourIndex = theirOffset + i >> theirChange;
        let bucketIndex = ourIndex - ours.indexBase;
        if (bucketIndex < 0) {
          bucketIndex += ours.backing.length;
        }
        ours.decrementBucket(bucketIndex, theirs.at(i));
      }
      ours.trim();
    }
  }
  exports.ExponentialHistogramAccumulation = ExponentialHistogramAccumulation;

  class ExponentialHistogramAggregator {
    kind = types_1.AggregatorKind.EXPONENTIAL_HISTOGRAM;
    _maxSize;
    _recordMinMax;
    constructor(maxSize, recordMinMax) {
      this._maxSize = maxSize;
      this._recordMinMax = recordMinMax;
    }
    createAccumulation(startTime) {
      return new ExponentialHistogramAccumulation(startTime, this._maxSize, this._recordMinMax);
    }
    merge(previous, delta) {
      const result = delta.clone();
      result.merge(previous);
      return result;
    }
    diff(previous, current) {
      const result = current.clone();
      result.diff(previous);
      return result;
    }
    toMetricData(descriptor, aggregationTemporality, accumulationByAttributes, endTime) {
      return {
        descriptor,
        aggregationTemporality,
        dataPointType: MetricData_1.DataPointType.EXPONENTIAL_HISTOGRAM,
        dataPoints: accumulationByAttributes.map(([attributes, accumulation]) => {
          const pointValue = accumulation.toPointValue();
          const allowsNegativeValues = descriptor.type === MetricData_1.InstrumentType.GAUGE || descriptor.type === MetricData_1.InstrumentType.UP_DOWN_COUNTER || descriptor.type === MetricData_1.InstrumentType.OBSERVABLE_GAUGE || descriptor.type === MetricData_1.InstrumentType.OBSERVABLE_UP_DOWN_COUNTER;
          return {
            attributes,
            startTime: accumulation.startTime,
            endTime,
            value: {
              min: pointValue.hasMinMax ? pointValue.min : undefined,
              max: pointValue.hasMinMax ? pointValue.max : undefined,
              sum: !allowsNegativeValues ? pointValue.sum : undefined,
              positive: {
                offset: pointValue.positive.offset,
                bucketCounts: pointValue.positive.bucketCounts
              },
              negative: {
                offset: pointValue.negative.offset,
                bucketCounts: pointValue.negative.bucketCounts
              },
              count: pointValue.count,
              scale: pointValue.scale,
              zeroCount: pointValue.zeroCount
            }
          };
        })
      };
    }
  }
  exports.ExponentialHistogramAggregator = ExponentialHistogramAggregator;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/aggregator/LastValue.js
var require_LastValue = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.LastValueAggregator = exports.LastValueAccumulation = undefined;
  var types_1 = require_types2();
  var core_1 = require_src5();
  var MetricData_1 = require_MetricData();

  class LastValueAccumulation {
    startTime;
    _current;
    sampleTime;
    constructor(startTime, current = 0, sampleTime = [0, 0]) {
      this.startTime = startTime;
      this._current = current;
      this.sampleTime = sampleTime;
    }
    record(value) {
      this._current = value;
      this.sampleTime = (0, core_1.millisToHrTime)(Date.now());
    }
    setStartTime(startTime) {
      this.startTime = startTime;
    }
    toPointValue() {
      return this._current;
    }
  }
  exports.LastValueAccumulation = LastValueAccumulation;

  class LastValueAggregator {
    kind = types_1.AggregatorKind.LAST_VALUE;
    createAccumulation(startTime) {
      return new LastValueAccumulation(startTime);
    }
    merge(previous, delta) {
      const latestAccumulation = (0, core_1.hrTimeToMicroseconds)(delta.sampleTime) >= (0, core_1.hrTimeToMicroseconds)(previous.sampleTime) ? delta : previous;
      return new LastValueAccumulation(previous.startTime, latestAccumulation.toPointValue(), latestAccumulation.sampleTime);
    }
    diff(previous, current) {
      const latestAccumulation = (0, core_1.hrTimeToMicroseconds)(current.sampleTime) >= (0, core_1.hrTimeToMicroseconds)(previous.sampleTime) ? current : previous;
      return new LastValueAccumulation(current.startTime, latestAccumulation.toPointValue(), latestAccumulation.sampleTime);
    }
    toMetricData(descriptor, aggregationTemporality, accumulationByAttributes, endTime) {
      return {
        descriptor,
        aggregationTemporality,
        dataPointType: MetricData_1.DataPointType.GAUGE,
        dataPoints: accumulationByAttributes.map(([attributes, accumulation]) => {
          return {
            attributes,
            startTime: accumulation.startTime,
            endTime,
            value: accumulation.toPointValue()
          };
        })
      };
    }
  }
  exports.LastValueAggregator = LastValueAggregator;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/aggregator/Sum.js
var require_Sum = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.SumAggregator = exports.SumAccumulation = undefined;
  var types_1 = require_types2();
  var MetricData_1 = require_MetricData();

  class SumAccumulation {
    startTime;
    monotonic;
    _current;
    reset;
    constructor(startTime, monotonic, current = 0, reset = false) {
      this.startTime = startTime;
      this.monotonic = monotonic;
      this._current = current;
      this.reset = reset;
    }
    record(value) {
      if (this.monotonic && value < 0) {
        return;
      }
      this._current += value;
    }
    setStartTime(startTime) {
      this.startTime = startTime;
    }
    toPointValue() {
      return this._current;
    }
  }
  exports.SumAccumulation = SumAccumulation;

  class SumAggregator {
    kind = types_1.AggregatorKind.SUM;
    monotonic;
    constructor(monotonic) {
      this.monotonic = monotonic;
    }
    createAccumulation(startTime) {
      return new SumAccumulation(startTime, this.monotonic);
    }
    merge(previous, delta) {
      const prevPv = previous.toPointValue();
      const deltaPv = delta.toPointValue();
      if (delta.reset) {
        return new SumAccumulation(delta.startTime, this.monotonic, deltaPv, delta.reset);
      }
      return new SumAccumulation(previous.startTime, this.monotonic, prevPv + deltaPv);
    }
    diff(previous, current) {
      const prevPv = previous.toPointValue();
      const currPv = current.toPointValue();
      if (this.monotonic && prevPv > currPv) {
        return new SumAccumulation(current.startTime, this.monotonic, currPv, true);
      }
      return new SumAccumulation(current.startTime, this.monotonic, currPv - prevPv);
    }
    toMetricData(descriptor, aggregationTemporality, accumulationByAttributes, endTime) {
      return {
        descriptor,
        aggregationTemporality,
        dataPointType: MetricData_1.DataPointType.SUM,
        dataPoints: accumulationByAttributes.map(([attributes, accumulation]) => {
          return {
            attributes,
            startTime: accumulation.startTime,
            endTime,
            value: accumulation.toPointValue()
          };
        }),
        isMonotonic: this.monotonic
      };
    }
  }
  exports.SumAggregator = SumAggregator;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/aggregator/index.js
var require_aggregator = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.SumAggregator = exports.SumAccumulation = exports.LastValueAggregator = exports.LastValueAccumulation = exports.ExponentialHistogramAggregator = exports.ExponentialHistogramAccumulation = exports.HistogramAggregator = exports.HistogramAccumulation = exports.DropAggregator = undefined;
  var Drop_1 = require_Drop();
  Object.defineProperty(exports, "DropAggregator", { enumerable: true, get: function() {
    return Drop_1.DropAggregator;
  } });
  var Histogram_1 = require_Histogram();
  Object.defineProperty(exports, "HistogramAccumulation", { enumerable: true, get: function() {
    return Histogram_1.HistogramAccumulation;
  } });
  Object.defineProperty(exports, "HistogramAggregator", { enumerable: true, get: function() {
    return Histogram_1.HistogramAggregator;
  } });
  var ExponentialHistogram_1 = require_ExponentialHistogram();
  Object.defineProperty(exports, "ExponentialHistogramAccumulation", { enumerable: true, get: function() {
    return ExponentialHistogram_1.ExponentialHistogramAccumulation;
  } });
  Object.defineProperty(exports, "ExponentialHistogramAggregator", { enumerable: true, get: function() {
    return ExponentialHistogram_1.ExponentialHistogramAggregator;
  } });
  var LastValue_1 = require_LastValue();
  Object.defineProperty(exports, "LastValueAccumulation", { enumerable: true, get: function() {
    return LastValue_1.LastValueAccumulation;
  } });
  Object.defineProperty(exports, "LastValueAggregator", { enumerable: true, get: function() {
    return LastValue_1.LastValueAggregator;
  } });
  var Sum_1 = require_Sum();
  Object.defineProperty(exports, "SumAccumulation", { enumerable: true, get: function() {
    return Sum_1.SumAccumulation;
  } });
  Object.defineProperty(exports, "SumAggregator", { enumerable: true, get: function() {
    return Sum_1.SumAggregator;
  } });
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/view/Aggregation.js
var require_Aggregation = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.DEFAULT_AGGREGATION = exports.EXPONENTIAL_HISTOGRAM_AGGREGATION = exports.HISTOGRAM_AGGREGATION = exports.LAST_VALUE_AGGREGATION = exports.SUM_AGGREGATION = exports.DROP_AGGREGATION = exports.DefaultAggregation = exports.ExponentialHistogramAggregation = exports.ExplicitBucketHistogramAggregation = exports.HistogramAggregation = exports.LastValueAggregation = exports.SumAggregation = exports.DropAggregation = undefined;
  var api = require_src();
  var aggregator_1 = require_aggregator();
  var MetricData_1 = require_MetricData();

  class DropAggregation {
    static DEFAULT_INSTANCE = new aggregator_1.DropAggregator;
    createAggregator(_instrument) {
      return DropAggregation.DEFAULT_INSTANCE;
    }
  }
  exports.DropAggregation = DropAggregation;

  class SumAggregation {
    static MONOTONIC_INSTANCE = new aggregator_1.SumAggregator(true);
    static NON_MONOTONIC_INSTANCE = new aggregator_1.SumAggregator(false);
    createAggregator(instrument) {
      switch (instrument.type) {
        case MetricData_1.InstrumentType.COUNTER:
        case MetricData_1.InstrumentType.OBSERVABLE_COUNTER:
        case MetricData_1.InstrumentType.HISTOGRAM: {
          return SumAggregation.MONOTONIC_INSTANCE;
        }
        default: {
          return SumAggregation.NON_MONOTONIC_INSTANCE;
        }
      }
    }
  }
  exports.SumAggregation = SumAggregation;

  class LastValueAggregation {
    static DEFAULT_INSTANCE = new aggregator_1.LastValueAggregator;
    createAggregator(_instrument) {
      return LastValueAggregation.DEFAULT_INSTANCE;
    }
  }
  exports.LastValueAggregation = LastValueAggregation;

  class HistogramAggregation {
    static DEFAULT_INSTANCE = new aggregator_1.HistogramAggregator([0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 1e4], true);
    createAggregator(_instrument) {
      return HistogramAggregation.DEFAULT_INSTANCE;
    }
  }
  exports.HistogramAggregation = HistogramAggregation;

  class ExplicitBucketHistogramAggregation {
    _boundaries;
    _recordMinMax;
    constructor(boundaries, recordMinMax = true) {
      if (boundaries == null) {
        throw new Error("ExplicitBucketHistogramAggregation should be created with explicit boundaries, if a single bucket histogram is required, please pass an empty array");
      }
      boundaries = boundaries.concat();
      boundaries = boundaries.sort((a, b) => a - b);
      const minusInfinityIndex = boundaries.lastIndexOf(-Infinity);
      let infinityIndex = boundaries.indexOf(Infinity);
      if (infinityIndex === -1) {
        infinityIndex = undefined;
      }
      this._boundaries = boundaries.slice(minusInfinityIndex + 1, infinityIndex);
      this._recordMinMax = recordMinMax;
    }
    createAggregator(_instrument) {
      return new aggregator_1.HistogramAggregator(this._boundaries, this._recordMinMax);
    }
  }
  exports.ExplicitBucketHistogramAggregation = ExplicitBucketHistogramAggregation;

  class ExponentialHistogramAggregation {
    _maxSize;
    _recordMinMax;
    constructor(maxSize = 160, recordMinMax = true) {
      this._maxSize = maxSize;
      this._recordMinMax = recordMinMax;
    }
    createAggregator(_instrument) {
      return new aggregator_1.ExponentialHistogramAggregator(this._maxSize, this._recordMinMax);
    }
  }
  exports.ExponentialHistogramAggregation = ExponentialHistogramAggregation;

  class DefaultAggregation {
    _resolve(instrument) {
      switch (instrument.type) {
        case MetricData_1.InstrumentType.COUNTER:
        case MetricData_1.InstrumentType.UP_DOWN_COUNTER:
        case MetricData_1.InstrumentType.OBSERVABLE_COUNTER:
        case MetricData_1.InstrumentType.OBSERVABLE_UP_DOWN_COUNTER: {
          return exports.SUM_AGGREGATION;
        }
        case MetricData_1.InstrumentType.GAUGE:
        case MetricData_1.InstrumentType.OBSERVABLE_GAUGE: {
          return exports.LAST_VALUE_AGGREGATION;
        }
        case MetricData_1.InstrumentType.HISTOGRAM: {
          if (instrument.advice.explicitBucketBoundaries) {
            return new ExplicitBucketHistogramAggregation(instrument.advice.explicitBucketBoundaries);
          }
          return exports.HISTOGRAM_AGGREGATION;
        }
      }
      api.diag.warn(`Unable to recognize instrument type: ${instrument.type}`);
      return exports.DROP_AGGREGATION;
    }
    createAggregator(instrument) {
      return this._resolve(instrument).createAggregator(instrument);
    }
  }
  exports.DefaultAggregation = DefaultAggregation;
  exports.DROP_AGGREGATION = new DropAggregation;
  exports.SUM_AGGREGATION = new SumAggregation;
  exports.LAST_VALUE_AGGREGATION = new LastValueAggregation;
  exports.HISTOGRAM_AGGREGATION = new HistogramAggregation;
  exports.EXPONENTIAL_HISTOGRAM_AGGREGATION = new ExponentialHistogramAggregation;
  exports.DEFAULT_AGGREGATION = new DefaultAggregation;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/view/AggregationOption.js
var require_AggregationOption = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.toAggregation = exports.AggregationType = undefined;
  var Aggregation_1 = require_Aggregation();
  var AggregationType;
  (function(AggregationType) {
    AggregationType[AggregationType["DEFAULT"] = 0] = "DEFAULT";
    AggregationType[AggregationType["DROP"] = 1] = "DROP";
    AggregationType[AggregationType["SUM"] = 2] = "SUM";
    AggregationType[AggregationType["LAST_VALUE"] = 3] = "LAST_VALUE";
    AggregationType[AggregationType["EXPLICIT_BUCKET_HISTOGRAM"] = 4] = "EXPLICIT_BUCKET_HISTOGRAM";
    AggregationType[AggregationType["EXPONENTIAL_HISTOGRAM"] = 5] = "EXPONENTIAL_HISTOGRAM";
  })(AggregationType || (exports.AggregationType = AggregationType = {}));
  function toAggregation(option) {
    switch (option.type) {
      case AggregationType.DEFAULT:
        return Aggregation_1.DEFAULT_AGGREGATION;
      case AggregationType.DROP:
        return Aggregation_1.DROP_AGGREGATION;
      case AggregationType.SUM:
        return Aggregation_1.SUM_AGGREGATION;
      case AggregationType.LAST_VALUE:
        return Aggregation_1.LAST_VALUE_AGGREGATION;
      case AggregationType.EXPONENTIAL_HISTOGRAM: {
        const expOption = option;
        return new Aggregation_1.ExponentialHistogramAggregation(expOption.options?.maxSize, expOption.options?.recordMinMax);
      }
      case AggregationType.EXPLICIT_BUCKET_HISTOGRAM: {
        const expOption = option;
        if (expOption.options == null) {
          return Aggregation_1.HISTOGRAM_AGGREGATION;
        } else {
          return new Aggregation_1.ExplicitBucketHistogramAggregation(expOption.options?.boundaries, expOption.options?.recordMinMax);
        }
      }
      default:
        throw new Error("Unsupported Aggregation");
    }
  }
  exports.toAggregation = toAggregation;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/export/AggregationSelector.js
var require_AggregationSelector = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.DEFAULT_AGGREGATION_TEMPORALITY_SELECTOR = exports.DEFAULT_AGGREGATION_SELECTOR = undefined;
  var AggregationTemporality_1 = require_AggregationTemporality();
  var AggregationOption_1 = require_AggregationOption();
  var DEFAULT_AGGREGATION_SELECTOR = (_instrumentType) => {
    return {
      type: AggregationOption_1.AggregationType.DEFAULT
    };
  };
  exports.DEFAULT_AGGREGATION_SELECTOR = DEFAULT_AGGREGATION_SELECTOR;
  var DEFAULT_AGGREGATION_TEMPORALITY_SELECTOR = (_instrumentType) => AggregationTemporality_1.AggregationTemporality.CUMULATIVE;
  exports.DEFAULT_AGGREGATION_TEMPORALITY_SELECTOR = DEFAULT_AGGREGATION_TEMPORALITY_SELECTOR;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/semconv.js
var require_semconv3 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ATTR_ERROR_TYPE = exports.METRIC_OTEL_SDK_METRIC_READER_COLLECTION_DURATION = exports.OTEL_COMPONENT_TYPE_VALUE_PERIODIC_METRIC_READER = exports.ATTR_OTEL_COMPONENT_TYPE = exports.ATTR_OTEL_COMPONENT_NAME = undefined;
  exports.ATTR_OTEL_COMPONENT_NAME = "otel.component.name";
  exports.ATTR_OTEL_COMPONENT_TYPE = "otel.component.type";
  exports.OTEL_COMPONENT_TYPE_VALUE_PERIODIC_METRIC_READER = "periodic_metric_reader";
  exports.METRIC_OTEL_SDK_METRIC_READER_COLLECTION_DURATION = "otel.sdk.metric_reader.collection.duration";
  exports.ATTR_ERROR_TYPE = "error.type";
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/export/MetricReaderMetrics.js
var require_MetricReaderMetrics = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.MetricReaderMetrics = undefined;
  var semconv_1 = require_semconv3();
  var componentCounter = new Map;

  class MetricReaderMetrics {
    collectionDuration;
    standardAttrs;
    constructor(componentType, meter) {
      const counter = componentCounter.get(componentType) ?? 0;
      componentCounter.set(componentType, counter + 1);
      this.standardAttrs = {
        [semconv_1.ATTR_OTEL_COMPONENT_TYPE]: componentType,
        [semconv_1.ATTR_OTEL_COMPONENT_NAME]: `${componentType}/${counter}`
      };
      this.collectionDuration = meter.createHistogram(semconv_1.METRIC_OTEL_SDK_METRIC_READER_COLLECTION_DURATION, {
        unit: "s",
        description: "The duration of the collect operation of the metric reader.",
        advice: {
          explicitBucketBoundaries: []
        }
      });
    }
    recordCollection(durationSecs, error) {
      const attrs = error ? { ...this.standardAttrs, [semconv_1.ATTR_ERROR_TYPE]: error } : this.standardAttrs;
      this.collectionDuration.record(durationSecs, attrs);
    }
  }
  exports.MetricReaderMetrics = MetricReaderMetrics;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/version.js
var require_version3 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.VERSION = undefined;
  exports.VERSION = "2.11.0";
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/export/MetricReader.js
var require_MetricReader = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.MetricReader = undefined;
  var api = require_src();
  var utils_1 = require_utils4();
  var AggregationSelector_1 = require_AggregationSelector();
  var MetricReaderMetrics_1 = require_MetricReaderMetrics();
  var version_1 = require_version3();
  var core_1 = require_src5();

  class MetricReader {
    _shutdown = false;
    _metricProducers;
    _sdkMetricProducer;
    _selfObsMetrics;
    _aggregationTemporalitySelector;
    _aggregationSelector;
    _cardinalitySelector;
    _otelComponentType;
    constructor(options) {
      this._aggregationSelector = options?.aggregationSelector ?? AggregationSelector_1.DEFAULT_AGGREGATION_SELECTOR;
      this._aggregationTemporalitySelector = options?.aggregationTemporalitySelector ?? AggregationSelector_1.DEFAULT_AGGREGATION_TEMPORALITY_SELECTOR;
      this._metricProducers = options?.metricProducers ?? [];
      this._cardinalitySelector = options?.cardinalitySelector;
      this._otelComponentType = options?.otelComponentType ?? this.constructor.name;
      this._selfObsMetrics = new MetricReaderMetrics_1.MetricReaderMetrics(this._otelComponentType, api.createNoopMeter());
    }
    setMetricProducer(metricProducer) {
      if (this._sdkMetricProducer) {
        throw new Error("MetricReader can not be bound to a MeterProvider again.");
      }
      this._sdkMetricProducer = metricProducer;
      this.onInitialized();
    }
    _setSelfObsMeterProvider(meterProvider) {
      const meter = meterProvider.getMeter("@opentelemetry/sdk-metrics", version_1.VERSION);
      this._selfObsMetrics = new MetricReaderMetrics_1.MetricReaderMetrics(this._otelComponentType, meter);
    }
    selectAggregation(instrumentType) {
      return this._aggregationSelector(instrumentType);
    }
    selectAggregationTemporality(instrumentType) {
      return this._aggregationTemporalitySelector(instrumentType);
    }
    selectCardinalityLimit(instrumentType) {
      return this._cardinalitySelector ? this._cardinalitySelector(instrumentType) : 2000;
    }
    onInitialized() {}
    async collect(options) {
      if (this._sdkMetricProducer === undefined) {
        throw new Error("MetricReader is not bound to a MetricProducer");
      }
      if (this._shutdown) {
        throw new Error("MetricReader is shutdown");
      }
      const startTime = (0, core_1.hrTime)();
      const [sdkCollectionResults, ...additionalCollectionResults] = await Promise.all([
        this._sdkMetricProducer.collect({
          timeoutMillis: options?.timeoutMillis
        }),
        ...this._metricProducers.map((producer) => producer.collect({
          timeoutMillis: options?.timeoutMillis
        }))
      ]);
      const endTime = (0, core_1.hrTime)();
      const errors = sdkCollectionResults.errors.concat(additionalCollectionResults.flatMap((result) => result.errors));
      const collectDuration = (0, core_1.hrTimeToSeconds)((0, core_1.hrTimeDuration)(startTime, endTime));
      this._selfObsMetrics.recordCollection(collectDuration, errors.length > 0 ? errors[0].name ?? "collect_error" : undefined);
      const resource = sdkCollectionResults.resourceMetrics.resource;
      const scopeMetrics = sdkCollectionResults.resourceMetrics.scopeMetrics.concat(additionalCollectionResults.flatMap((result) => result.resourceMetrics.scopeMetrics));
      return {
        resourceMetrics: {
          resource,
          scopeMetrics
        },
        errors
      };
    }
    async shutdown(options) {
      if (this._shutdown) {
        api.diag.error("Cannot call shutdown twice.");
        return;
      }
      if (options?.timeoutMillis == null) {
        await this.onShutdown();
      } else {
        await (0, utils_1.callWithTimeout)(this.onShutdown(), options.timeoutMillis);
      }
      this._shutdown = true;
    }
    async forceFlush(options) {
      if (this._shutdown) {
        api.diag.warn("Cannot forceFlush on already shutdown MetricReader.");
        return;
      }
      if (options?.timeoutMillis == null) {
        await this.onForceFlush();
        return;
      }
      await (0, utils_1.callWithTimeout)(this.onForceFlush(), options.timeoutMillis);
    }
  }
  exports.MetricReader = MetricReader;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/export/MetricDataSplitter.js
var require_MetricDataSplitter = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.splitMetricData = undefined;
  function splitMetricData(resourceMetrics, maxExportBatchSize) {
    if (!Number.isInteger(maxExportBatchSize) || maxExportBatchSize <= 0) {
      throw new Error("maxExportBatchSize must be a positive integer");
    }
    const batches = [];
    let currentBatchPoints = 0;
    let currentScopeMetrics = [];
    function flush() {
      if (currentScopeMetrics.length > 0) {
        batches.push({
          resource: resourceMetrics.resource,
          scopeMetrics: currentScopeMetrics
        });
        currentScopeMetrics = [];
        currentBatchPoints = 0;
      }
    }
    for (const scopeMetric of resourceMetrics.scopeMetrics) {
      let scopeMetricCopy = null;
      for (const metric of scopeMetric.metrics) {
        const dataPoints = metric.dataPoints;
        if (dataPoints.length === 0) {
          if (!scopeMetricCopy) {
            scopeMetricCopy = { scope: scopeMetric.scope, metrics: [] };
            currentScopeMetrics.push(scopeMetricCopy);
          }
          scopeMetricCopy.metrics.push(metric);
          continue;
        }
        let offset = 0;
        while (offset < dataPoints.length) {
          const spaceLeft = maxExportBatchSize - currentBatchPoints;
          const take = Math.min(spaceLeft, dataPoints.length - offset);
          if (!scopeMetricCopy) {
            scopeMetricCopy = { scope: scopeMetric.scope, metrics: [] };
            currentScopeMetrics.push(scopeMetricCopy);
          }
          const metricCopy = {
            ...metric,
            dataPoints: dataPoints.slice(offset, offset + take)
          };
          scopeMetricCopy.metrics.push(metricCopy);
          offset += take;
          currentBatchPoints += take;
          if (currentBatchPoints === maxExportBatchSize) {
            flush();
            scopeMetricCopy = null;
          }
        }
      }
    }
    flush();
    return batches;
  }
  exports.splitMetricData = splitMetricData;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/export/PeriodicExportingMetricReader.js
var require_PeriodicExportingMetricReader = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.PeriodicExportingMetricReader = undefined;
  var api = require_src();
  var core_1 = require_src5();
  var MetricReader_1 = require_MetricReader();
  var utils_1 = require_utils4();
  var MetricData_1 = require_MetricData();
  var MetricDataSplitter_1 = require_MetricDataSplitter();
  var semconv_1 = require_semconv3();

  class PeriodicExportingMetricReader extends MetricReader_1.MetricReader {
    _interval;
    _exporter;
    _exportInterval;
    _exportTimeout;
    _maxExportBatchSize;
    _ongoingExportPromise = null;
    constructor(options) {
      const { exporter, exportIntervalMillis = 60000, metricProducers, cardinalityLimits, maxExportBatchSize } = options;
      let { exportTimeoutMillis = 30000 } = options;
      super({
        aggregationSelector: exporter.selectAggregation?.bind(exporter),
        aggregationTemporalitySelector: exporter.selectAggregationTemporality?.bind(exporter),
        otelComponentType: semconv_1.OTEL_COMPONENT_TYPE_VALUE_PERIODIC_METRIC_READER,
        metricProducers,
        cardinalitySelector: (instrumentType) => {
          const limits = {
            default: 2000,
            ...cardinalityLimits
          };
          switch (instrumentType) {
            case MetricData_1.InstrumentType.COUNTER:
              return limits.counter ?? limits.default;
            case MetricData_1.InstrumentType.GAUGE:
              return limits.gauge ?? limits.default;
            case MetricData_1.InstrumentType.HISTOGRAM:
              return limits.histogram ?? limits.default;
            case MetricData_1.InstrumentType.OBSERVABLE_COUNTER:
              return limits.observableCounter ?? limits.default;
            case MetricData_1.InstrumentType.OBSERVABLE_UP_DOWN_COUNTER:
              return limits.observableUpDownCounter ?? limits.default;
            case MetricData_1.InstrumentType.OBSERVABLE_GAUGE:
              return limits.observableGauge ?? limits.default;
            case MetricData_1.InstrumentType.UP_DOWN_COUNTER:
              return limits.upDownCounter ?? limits.default;
            default:
              return limits.default;
          }
        }
      });
      if (exportIntervalMillis <= 0) {
        throw Error("exportIntervalMillis must be greater than 0");
      }
      if (exportTimeoutMillis <= 0) {
        throw Error("exportTimeoutMillis must be greater than 0");
      }
      if (maxExportBatchSize !== undefined && (!Number.isInteger(maxExportBatchSize) || maxExportBatchSize <= 0)) {
        throw Error("maxExportBatchSize must be a positive integer");
      }
      if (exportIntervalMillis < exportTimeoutMillis) {
        if ("exportIntervalMillis" in options && "exportTimeoutMillis" in options) {
          throw Error("exportIntervalMillis must be greater than or equal to exportTimeoutMillis");
        } else {
          api.diag.info(`Timeout of ${exportTimeoutMillis} exceeds the interval of ${exportIntervalMillis}. Clamping timeout to interval duration.`);
          exportTimeoutMillis = exportIntervalMillis;
        }
      }
      this._exportInterval = exportIntervalMillis;
      this._exportTimeout = exportTimeoutMillis;
      this._exporter = exporter;
      this._maxExportBatchSize = maxExportBatchSize;
    }
    async _runOnce() {
      try {
        await this._doRun();
      } catch (err) {
        (0, core_1.globalErrorHandler)(err);
      }
    }
    async _doRun() {
      if (this._ongoingExportPromise) {
        api.diag.debug("PeriodicExportingMetricReader: export already in progress, skipping");
        return;
      }
      const currentRun = async () => {
        const { resourceMetrics, errors } = await this.collect({
          timeoutMillis: this._exportTimeout
        });
        if (errors.length > 0) {
          api.diag.error("PeriodicExportingMetricReader: metrics collection errors", ...errors);
        }
        if (resourceMetrics.resource.asyncAttributesPending) {
          try {
            await resourceMetrics.resource.waitForAsyncAttributes?.();
          } catch (e) {
            api.diag.debug("Error while resolving async portion of resource: ", e);
            (0, core_1.globalErrorHandler)(e);
          }
        }
        if (resourceMetrics.scopeMetrics.length === 0) {
          return;
        }
        const batches = this._maxExportBatchSize ? (0, MetricDataSplitter_1.splitMetricData)(resourceMetrics, this._maxExportBatchSize) : [resourceMetrics];
        let anyErr = null;
        for (const batch of batches) {
          try {
            const result = await (0, utils_1.callWithTimeout)(core_1.internal._export(this._exporter, batch), this._exportTimeout);
            if (result.code !== core_1.ExportResultCode.SUCCESS) {
              const err = new Error(`PeriodicExportingMetricReader: metrics export failed (error ${result.error})`);
              anyErr = err;
            }
          } catch (e) {
            if (e instanceof utils_1.TimeoutError) {
              api.diag.error(`PeriodicExportingMetricReader: metrics export timed out after ${this._exportTimeout}ms`);
              break;
            } else {
              api.diag.error("PeriodicExportingMetricReader: metrics export threw error", e);
              anyErr = e instanceof Error ? e : new Error(String(e));
            }
          }
        }
        if (anyErr) {
          throw anyErr;
        }
      };
      this._ongoingExportPromise = currentRun();
      try {
        await this._ongoingExportPromise;
      } finally {
        this._ongoingExportPromise = null;
      }
    }
    onInitialized() {
      this._interval = setInterval(() => {
        this._runOnce();
      }, this._exportInterval);
      if (typeof this._interval !== "number") {
        this._interval.unref();
      }
    }
    async onForceFlush() {
      await this._awaitOngoingExport();
      if (this._ongoingExportPromise) {
        await this._awaitOngoingExport();
      } else {
        await this._runOnce();
      }
      await this._exporter.forceFlush();
    }
    async _awaitOngoingExport() {
      if (this._ongoingExportPromise) {
        api.diag.debug("PeriodicExportingMetricReader: export already in progress, awaiting ongoing export");
        try {
          await this._ongoingExportPromise;
        } catch {}
      }
    }
    async onShutdown() {
      if (this._interval) {
        clearInterval(this._interval);
      }
      await this.onForceFlush();
      await this._exporter.shutdown();
    }
  }
  exports.PeriodicExportingMetricReader = PeriodicExportingMetricReader;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/export/InMemoryMetricExporter.js
var require_InMemoryMetricExporter = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.InMemoryMetricExporter = undefined;
  var core_1 = require_src5();

  class InMemoryMetricExporter {
    _shutdown = false;
    _aggregationTemporality;
    _metrics = [];
    constructor(aggregationTemporality) {
      this._aggregationTemporality = aggregationTemporality;
    }
    export(metrics, resultCallback) {
      if (this._shutdown) {
        setTimeout(() => resultCallback({ code: core_1.ExportResultCode.FAILED }), 0);
        return;
      }
      this._metrics.push(metrics);
      setTimeout(() => resultCallback({ code: core_1.ExportResultCode.SUCCESS }), 0);
    }
    getMetrics() {
      return this._metrics;
    }
    forceFlush() {
      return Promise.resolve();
    }
    reset() {
      this._metrics = [];
    }
    selectAggregationTemporality(_instrumentType) {
      return this._aggregationTemporality;
    }
    shutdown() {
      this._shutdown = true;
      return Promise.resolve();
    }
  }
  exports.InMemoryMetricExporter = InMemoryMetricExporter;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/export/ConsoleMetricExporter.js
var require_ConsoleMetricExporter = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ConsoleMetricExporter = undefined;
  var core_1 = require_src5();
  var AggregationSelector_1 = require_AggregationSelector();

  class ConsoleMetricExporter {
    _shutdown = false;
    _temporalitySelector;
    constructor(options) {
      this._temporalitySelector = options?.temporalitySelector ?? AggregationSelector_1.DEFAULT_AGGREGATION_TEMPORALITY_SELECTOR;
    }
    export(metrics, resultCallback) {
      if (this._shutdown) {
        resultCallback({ code: core_1.ExportResultCode.FAILED });
        return;
      }
      return ConsoleMetricExporter._sendMetrics(metrics, resultCallback);
    }
    forceFlush() {
      return Promise.resolve();
    }
    selectAggregationTemporality(_instrumentType) {
      return this._temporalitySelector(_instrumentType);
    }
    shutdown() {
      this._shutdown = true;
      return Promise.resolve();
    }
    static _sendMetrics(metrics, done) {
      for (const scopeMetrics of metrics.scopeMetrics) {
        for (const metric of scopeMetrics.metrics) {
          console.dir({
            descriptor: metric.descriptor,
            dataPointType: metric.dataPointType,
            dataPoints: metric.dataPoints
          }, { depth: null });
        }
      }
      done({ code: core_1.ExportResultCode.SUCCESS });
    }
  }
  exports.ConsoleMetricExporter = ConsoleMetricExporter;
});

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/default-service-name.js
var require_default_service_name = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports._clearDefaultServiceNameCache = exports.defaultServiceName = undefined;
  var serviceName;
  function defaultServiceName() {
    if (serviceName === undefined) {
      try {
        const argv0 = globalThis.process.argv0;
        serviceName = argv0 ? `unknown_service:${argv0}` : "unknown_service";
      } catch {
        serviceName = "unknown_service";
      }
    }
    return serviceName;
  }
  exports.defaultServiceName = defaultServiceName;
  function _clearDefaultServiceNameCache() {
    serviceName = undefined;
  }
  exports._clearDefaultServiceNameCache = _clearDefaultServiceNameCache;
});

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/utils.js
var require_utils5 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.isPromiseLike = undefined;
  var isPromiseLike = (val) => {
    return val !== null && typeof val === "object" && typeof val.then === "function";
  };
  exports.isPromiseLike = isPromiseLike;
});

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/ResourceImpl.js
var require_ResourceImpl = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.defaultResource = exports.emptyResource = exports.resourceFromDetectedResource = exports.resourceFromAttributes = undefined;
  var api_1 = require_src();
  var core_1 = require_src5();
  var semantic_conventions_1 = require_src4();
  var default_service_name_1 = require_default_service_name();
  var utils_1 = require_utils5();

  class ResourceImpl {
    _rawAttributes;
    _asyncAttributesPending = false;
    _schemaUrl;
    _memoizedAttributes;
    static FromAttributeList(attributes, options) {
      const res = new ResourceImpl({}, options);
      res._rawAttributes = guardedRawAttributes(attributes);
      res._asyncAttributesPending = attributes.filter(([_, val]) => (0, utils_1.isPromiseLike)(val)).length > 0;
      return res;
    }
    constructor(resource, options) {
      const attributes = resource.attributes ?? {};
      this._rawAttributes = Object.entries(attributes).map(([k, v]) => {
        if ((0, utils_1.isPromiseLike)(v)) {
          this._asyncAttributesPending = true;
        }
        return [k, v];
      });
      this._rawAttributes = guardedRawAttributes(this._rawAttributes);
      this._schemaUrl = validateSchemaUrl(options?.schemaUrl);
    }
    get asyncAttributesPending() {
      return this._asyncAttributesPending;
    }
    async waitForAsyncAttributes() {
      if (!this.asyncAttributesPending) {
        return;
      }
      for (let i = 0;i < this._rawAttributes.length; i++) {
        const [k, v] = this._rawAttributes[i];
        this._rawAttributes[i] = [k, (0, utils_1.isPromiseLike)(v) ? await v : v];
      }
      this._asyncAttributesPending = false;
    }
    get attributes() {
      if (this.asyncAttributesPending) {
        api_1.diag.error("Accessing resource attributes before async attributes settled");
      }
      if (this._memoizedAttributes) {
        return this._memoizedAttributes;
      }
      const attrs = {};
      for (const [k, v] of this._rawAttributes) {
        if ((0, utils_1.isPromiseLike)(v)) {
          api_1.diag.debug(`Unsettled resource attribute ${k} skipped`);
          continue;
        }
        if (v != null) {
          attrs[k] ??= v;
        }
      }
      if (!this._asyncAttributesPending) {
        this._memoizedAttributes = attrs;
      }
      return attrs;
    }
    getRawAttributes() {
      return this._rawAttributes;
    }
    get schemaUrl() {
      return this._schemaUrl;
    }
    merge(resource) {
      if (resource == null)
        return this;
      const mergedSchemaUrl = mergeSchemaUrl(this, resource);
      const mergedOptions = mergedSchemaUrl ? { schemaUrl: mergedSchemaUrl } : undefined;
      return ResourceImpl.FromAttributeList([...resource.getRawAttributes(), ...this.getRawAttributes()], mergedOptions);
    }
  }
  function resourceFromAttributes(attributes, options) {
    return ResourceImpl.FromAttributeList(Object.entries(attributes), options);
  }
  exports.resourceFromAttributes = resourceFromAttributes;
  function resourceFromDetectedResource(detectedResource, options) {
    return new ResourceImpl(detectedResource, options);
  }
  exports.resourceFromDetectedResource = resourceFromDetectedResource;
  function emptyResource() {
    return resourceFromAttributes({});
  }
  exports.emptyResource = emptyResource;
  function defaultResource() {
    return resourceFromAttributes({
      [semantic_conventions_1.ATTR_SERVICE_NAME]: (0, default_service_name_1.defaultServiceName)(),
      [semantic_conventions_1.ATTR_TELEMETRY_SDK_LANGUAGE]: core_1.SDK_INFO[semantic_conventions_1.ATTR_TELEMETRY_SDK_LANGUAGE],
      [semantic_conventions_1.ATTR_TELEMETRY_SDK_NAME]: core_1.SDK_INFO[semantic_conventions_1.ATTR_TELEMETRY_SDK_NAME],
      [semantic_conventions_1.ATTR_TELEMETRY_SDK_VERSION]: core_1.SDK_INFO[semantic_conventions_1.ATTR_TELEMETRY_SDK_VERSION]
    });
  }
  exports.defaultResource = defaultResource;
  function guardedRawAttributes(attributes) {
    return attributes.map(([k, v]) => {
      if ((0, utils_1.isPromiseLike)(v)) {
        return [
          k,
          v.catch((err) => {
            api_1.diag.debug("promise rejection for resource attribute: %s - %s", k, err);
            return;
          })
        ];
      }
      return [k, v];
    });
  }
  function validateSchemaUrl(schemaUrl) {
    if (typeof schemaUrl === "string" || schemaUrl === undefined) {
      return schemaUrl;
    }
    api_1.diag.warn("Schema URL must be string or undefined, got %s. Schema URL will be ignored.", schemaUrl);
    return;
  }
  function mergeSchemaUrl(old, updating) {
    const oldSchemaUrl = old?.schemaUrl;
    const updatingSchemaUrl = updating?.schemaUrl;
    const isOldEmpty = oldSchemaUrl === undefined || oldSchemaUrl === "";
    const isUpdatingEmpty = updatingSchemaUrl === undefined || updatingSchemaUrl === "";
    if (isOldEmpty) {
      return updatingSchemaUrl;
    }
    if (isUpdatingEmpty) {
      return oldSchemaUrl;
    }
    if (oldSchemaUrl === updatingSchemaUrl) {
      return oldSchemaUrl;
    }
    api_1.diag.warn('Schema URL merge conflict: old resource has "%s", updating resource has "%s". Resulting resource will have undefined Schema URL.', oldSchemaUrl, updatingSchemaUrl);
    return;
  }
});

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/detect-resources.js
var require_detect_resources = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.detectResources = undefined;
  var api_1 = require_src();
  var ResourceImpl_1 = require_ResourceImpl();
  var detectResources = (config = {}) => {
    const resources = (config.detectors || []).map((d) => {
      try {
        const resource = (0, ResourceImpl_1.resourceFromDetectedResource)(d.detect(config));
        api_1.diag.debug(`${d.constructor.name} found resource.`, resource);
        return resource;
      } catch (e) {
        api_1.diag.debug(`${d.constructor.name} failed: ${e.message}`);
        return (0, ResourceImpl_1.emptyResource)();
      }
    });
    return resources.reduce((acc, resource) => acc.merge(resource), (0, ResourceImpl_1.emptyResource)());
  };
  exports.detectResources = detectResources;
});

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/detectors/EnvDetector.js
var require_EnvDetector = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.envDetector = undefined;
  var api_1 = require_src();
  var semantic_conventions_1 = require_src4();
  var core_1 = require_src5();

  class EnvDetector {
    _MAX_LENGTH = 255;
    _COMMA_SEPARATOR = ",";
    _LABEL_KEY_VALUE_SPLITTER = "=";
    detect(_config) {
      const attributes = {};
      const rawAttributes = (0, core_1.getStringFromEnv)("OTEL_RESOURCE_ATTRIBUTES");
      const serviceName = (0, core_1.getStringFromEnv)("OTEL_SERVICE_NAME");
      if (rawAttributes) {
        try {
          const parsedAttributes = this._parseResourceAttributes(rawAttributes);
          Object.assign(attributes, parsedAttributes);
        } catch (e) {
          api_1.diag.debug(`EnvDetector failed: ${e instanceof Error ? e.message : e}`);
        }
      }
      if (serviceName) {
        attributes[semantic_conventions_1.ATTR_SERVICE_NAME] = serviceName;
      }
      return { attributes };
    }
    _parseResourceAttributes(rawEnvAttributes) {
      if (!rawEnvAttributes)
        return {};
      const attributes = {};
      const rawAttributes = rawEnvAttributes.split(this._COMMA_SEPARATOR).filter((attr) => attr.trim() !== "");
      for (const rawAttribute of rawAttributes) {
        const keyValuePair = rawAttribute.split(this._LABEL_KEY_VALUE_SPLITTER);
        if (keyValuePair.length !== 2) {
          throw new Error(`Invalid format for OTEL_RESOURCE_ATTRIBUTES: "${rawAttribute}". ` + "Expected format: key=value. The ',' and '=' characters must be percent-encoded in keys and values.");
        }
        const [rawKey, rawValue] = keyValuePair;
        const key = rawKey.trim();
        const value = rawValue.trim();
        if (key.length === 0) {
          throw new Error(`Invalid OTEL_RESOURCE_ATTRIBUTES: empty attribute key in "${rawAttribute}".`);
        }
        let decodedKey;
        let decodedValue;
        try {
          decodedKey = decodeURIComponent(key);
          decodedValue = decodeURIComponent(value);
        } catch (e) {
          throw new Error(`Failed to percent-decode OTEL_RESOURCE_ATTRIBUTES entry "${rawAttribute}": ${e instanceof Error ? e.message : e}`, { cause: e });
        }
        if (decodedKey.length > this._MAX_LENGTH) {
          throw new Error(`Attribute key exceeds the maximum length of ${this._MAX_LENGTH} characters: "${decodedKey}".`);
        }
        if (decodedValue.length > this._MAX_LENGTH) {
          throw new Error(`Attribute value exceeds the maximum length of ${this._MAX_LENGTH} characters for key "${decodedKey}".`);
        }
        attributes[decodedKey] = decodedValue;
      }
      return attributes;
    }
  }
  exports.envDetector = new EnvDetector;
});

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/semconv.js
var require_semconv4 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ATTR_WEBENGINE_VERSION = exports.ATTR_WEBENGINE_NAME = exports.ATTR_WEBENGINE_DESCRIPTION = exports.ATTR_SERVICE_NAMESPACE = exports.ATTR_SERVICE_INSTANCE_ID = exports.ATTR_PROCESS_RUNTIME_VERSION = exports.ATTR_PROCESS_RUNTIME_NAME = exports.ATTR_PROCESS_RUNTIME_DESCRIPTION = exports.ATTR_PROCESS_PID = exports.ATTR_PROCESS_OWNER = exports.ATTR_PROCESS_EXECUTABLE_PATH = exports.ATTR_PROCESS_EXECUTABLE_NAME = exports.ATTR_PROCESS_COMMAND_ARGS = exports.ATTR_PROCESS_COMMAND = exports.ATTR_OS_VERSION = exports.ATTR_OS_TYPE = exports.ATTR_K8S_POD_NAME = exports.ATTR_K8S_NAMESPACE_NAME = exports.ATTR_K8S_DEPLOYMENT_NAME = exports.ATTR_K8S_CLUSTER_NAME = exports.ATTR_HOST_TYPE = exports.ATTR_HOST_NAME = exports.ATTR_HOST_IMAGE_VERSION = exports.ATTR_HOST_IMAGE_NAME = exports.ATTR_HOST_IMAGE_ID = exports.ATTR_HOST_ID = exports.ATTR_HOST_ARCH = exports.ATTR_CONTAINER_NAME = exports.ATTR_CONTAINER_IMAGE_TAGS = exports.ATTR_CONTAINER_IMAGE_NAME = exports.ATTR_CONTAINER_ID = exports.ATTR_CLOUD_REGION = exports.ATTR_CLOUD_PROVIDER = exports.ATTR_CLOUD_AVAILABILITY_ZONE = exports.ATTR_CLOUD_ACCOUNT_ID = undefined;
  exports.ATTR_CLOUD_ACCOUNT_ID = "cloud.account.id";
  exports.ATTR_CLOUD_AVAILABILITY_ZONE = "cloud.availability_zone";
  exports.ATTR_CLOUD_PROVIDER = "cloud.provider";
  exports.ATTR_CLOUD_REGION = "cloud.region";
  exports.ATTR_CONTAINER_ID = "container.id";
  exports.ATTR_CONTAINER_IMAGE_NAME = "container.image.name";
  exports.ATTR_CONTAINER_IMAGE_TAGS = "container.image.tags";
  exports.ATTR_CONTAINER_NAME = "container.name";
  exports.ATTR_HOST_ARCH = "host.arch";
  exports.ATTR_HOST_ID = "host.id";
  exports.ATTR_HOST_IMAGE_ID = "host.image.id";
  exports.ATTR_HOST_IMAGE_NAME = "host.image.name";
  exports.ATTR_HOST_IMAGE_VERSION = "host.image.version";
  exports.ATTR_HOST_NAME = "host.name";
  exports.ATTR_HOST_TYPE = "host.type";
  exports.ATTR_K8S_CLUSTER_NAME = "k8s.cluster.name";
  exports.ATTR_K8S_DEPLOYMENT_NAME = "k8s.deployment.name";
  exports.ATTR_K8S_NAMESPACE_NAME = "k8s.namespace.name";
  exports.ATTR_K8S_POD_NAME = "k8s.pod.name";
  exports.ATTR_OS_TYPE = "os.type";
  exports.ATTR_OS_VERSION = "os.version";
  exports.ATTR_PROCESS_COMMAND = "process.command";
  exports.ATTR_PROCESS_COMMAND_ARGS = "process.command_args";
  exports.ATTR_PROCESS_EXECUTABLE_NAME = "process.executable.name";
  exports.ATTR_PROCESS_EXECUTABLE_PATH = "process.executable.path";
  exports.ATTR_PROCESS_OWNER = "process.owner";
  exports.ATTR_PROCESS_PID = "process.pid";
  exports.ATTR_PROCESS_RUNTIME_DESCRIPTION = "process.runtime.description";
  exports.ATTR_PROCESS_RUNTIME_NAME = "process.runtime.name";
  exports.ATTR_PROCESS_RUNTIME_VERSION = "process.runtime.version";
  exports.ATTR_SERVICE_INSTANCE_ID = "service.instance.id";
  exports.ATTR_SERVICE_NAMESPACE = "service.namespace";
  exports.ATTR_WEBENGINE_DESCRIPTION = "webengine.description";
  exports.ATTR_WEBENGINE_NAME = "webengine.name";
  exports.ATTR_WEBENGINE_VERSION = "webengine.version";
});

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/detectors/platform/node/machine-id/getMachineId.js
var require_getMachineId = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.getMachineId = undefined;
  var process2 = __require("process");
  var getMachineIdImpl;
  async function getMachineId() {
    if (!getMachineIdImpl) {
      switch (process2.platform) {
        case "darwin":
          getMachineIdImpl = (await import("./getMachineId-darwin-vyzzs39k.js").then((m)=>__toESM(m.default))).getMachineId;
          break;
        case "linux":
          getMachineIdImpl = (await import("./getMachineId-linux-2y4ve64v.js").then((m)=>__toESM(m.default))).getMachineId;
          break;
        case "freebsd":
          getMachineIdImpl = (await import("./getMachineId-bsd-rw5txavt.js").then((m)=>__toESM(m.default))).getMachineId;
          break;
        case "win32":
          getMachineIdImpl = (await import("./getMachineId-win-py6x26zf.js").then((m)=>__toESM(m.default))).getMachineId;
          break;
        default:
          getMachineIdImpl = (await import("./getMachineId-unsupported-2y8yye54.js").then((m)=>__toESM(m.default))).getMachineId;
          break;
      }
    }
    return getMachineIdImpl();
  }
  exports.getMachineId = getMachineId;
});

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/detectors/platform/node/utils.js
var require_utils6 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.normalizeType = exports.normalizeArch = undefined;
  var normalizeArch = (nodeArchString) => {
    switch (nodeArchString) {
      case "arm":
        return "arm32";
      case "ppc":
        return "ppc32";
      case "x64":
        return "amd64";
      default:
        return nodeArchString;
    }
  };
  exports.normalizeArch = normalizeArch;
  var normalizeType = (nodePlatform) => {
    switch (nodePlatform) {
      case "sunos":
        return "solaris";
      case "win32":
        return "windows";
      default:
        return nodePlatform;
    }
  };
  exports.normalizeType = normalizeType;
});

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/detectors/platform/node/HostDetector.js
var require_HostDetector = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.hostDetector = undefined;
  var semconv_1 = require_semconv4();
  var os_1 = __require("os");
  var getMachineId_1 = require_getMachineId();
  var utils_1 = require_utils6();

  class HostDetector {
    detect(_config) {
      const attributes = {
        [semconv_1.ATTR_HOST_NAME]: (0, os_1.hostname)(),
        [semconv_1.ATTR_HOST_ARCH]: (0, utils_1.normalizeArch)((0, os_1.arch)()),
        [semconv_1.ATTR_HOST_ID]: (0, getMachineId_1.getMachineId)()
      };
      return { attributes };
    }
  }
  exports.hostDetector = new HostDetector;
});

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/detectors/platform/node/OSDetector.js
var require_OSDetector = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.osDetector = undefined;
  var semconv_1 = require_semconv4();
  var os_1 = __require("os");
  var utils_1 = require_utils6();

  class OSDetector {
    detect(_config) {
      const attributes = {
        [semconv_1.ATTR_OS_TYPE]: (0, utils_1.normalizeType)((0, os_1.platform)()),
        [semconv_1.ATTR_OS_VERSION]: (0, os_1.release)()
      };
      return { attributes };
    }
  }
  exports.osDetector = new OSDetector;
});

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/detectors/platform/node/ProcessDetector.js
var require_ProcessDetector = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.processDetector = undefined;
  var api_1 = require_src();
  var semconv_1 = require_semconv4();
  var os = __require("os");

  class ProcessDetector {
    detect(_config) {
      const attributes = {
        [semconv_1.ATTR_PROCESS_PID]: process.pid,
        [semconv_1.ATTR_PROCESS_EXECUTABLE_NAME]: process.title,
        [semconv_1.ATTR_PROCESS_EXECUTABLE_PATH]: process.execPath,
        [semconv_1.ATTR_PROCESS_COMMAND_ARGS]: [
          process.argv[0],
          ...process.execArgv,
          ...process.argv.slice(1)
        ],
        [semconv_1.ATTR_PROCESS_RUNTIME_VERSION]: process.versions.node,
        [semconv_1.ATTR_PROCESS_RUNTIME_NAME]: "nodejs",
        [semconv_1.ATTR_PROCESS_RUNTIME_DESCRIPTION]: "Node.js"
      };
      if (process.argv.length > 1) {
        attributes[semconv_1.ATTR_PROCESS_COMMAND] = process.argv[1];
      }
      try {
        const userInfo2 = os.userInfo();
        attributes[semconv_1.ATTR_PROCESS_OWNER] = userInfo2.username;
      } catch (e) {
        api_1.diag.debug(`error obtaining process owner: ${e}`);
      }
      return { attributes };
    }
  }
  exports.processDetector = new ProcessDetector;
});

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/detectors/platform/node/ServiceInstanceIdDetector.js
var require_ServiceInstanceIdDetector = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.serviceInstanceIdDetector = undefined;
  var semconv_1 = require_semconv4();
  var crypto_1 = __require("crypto");

  class ServiceInstanceIdDetector {
    _serviceInstanceId;
    detect(_config) {
      if (!this._serviceInstanceId) {
        this._serviceInstanceId = (0, crypto_1.randomUUID)();
      }
      return {
        attributes: {
          [semconv_1.ATTR_SERVICE_INSTANCE_ID]: this._serviceInstanceId
        }
      };
    }
  }
  exports.serviceInstanceIdDetector = new ServiceInstanceIdDetector;
});

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/detectors/platform/node/index.js
var require_node2 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.serviceInstanceIdDetector = exports.processDetector = exports.osDetector = exports.hostDetector = undefined;
  var HostDetector_1 = require_HostDetector();
  Object.defineProperty(exports, "hostDetector", { enumerable: true, get: function() {
    return HostDetector_1.hostDetector;
  } });
  var OSDetector_1 = require_OSDetector();
  Object.defineProperty(exports, "osDetector", { enumerable: true, get: function() {
    return OSDetector_1.osDetector;
  } });
  var ProcessDetector_1 = require_ProcessDetector();
  Object.defineProperty(exports, "processDetector", { enumerable: true, get: function() {
    return ProcessDetector_1.processDetector;
  } });
  var ServiceInstanceIdDetector_1 = require_ServiceInstanceIdDetector();
  Object.defineProperty(exports, "serviceInstanceIdDetector", { enumerable: true, get: function() {
    return ServiceInstanceIdDetector_1.serviceInstanceIdDetector;
  } });
});

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/detectors/platform/index.js
var require_platform2 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.serviceInstanceIdDetector = exports.processDetector = exports.osDetector = exports.hostDetector = undefined;
  var node_1 = require_node2();
  Object.defineProperty(exports, "hostDetector", { enumerable: true, get: function() {
    return node_1.hostDetector;
  } });
  Object.defineProperty(exports, "osDetector", { enumerable: true, get: function() {
    return node_1.osDetector;
  } });
  Object.defineProperty(exports, "processDetector", { enumerable: true, get: function() {
    return node_1.processDetector;
  } });
  Object.defineProperty(exports, "serviceInstanceIdDetector", { enumerable: true, get: function() {
    return node_1.serviceInstanceIdDetector;
  } });
});

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/detectors/NoopDetector.js
var require_NoopDetector = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.noopDetector = exports.NoopDetector = undefined;

  class NoopDetector {
    detect() {
      return {
        attributes: {}
      };
    }
  }
  exports.NoopDetector = NoopDetector;
  exports.noopDetector = new NoopDetector;
});

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/detectors/index.js
var require_detectors = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.noopDetector = exports.serviceInstanceIdDetector = exports.processDetector = exports.osDetector = exports.hostDetector = exports.envDetector = undefined;
  var EnvDetector_1 = require_EnvDetector();
  Object.defineProperty(exports, "envDetector", { enumerable: true, get: function() {
    return EnvDetector_1.envDetector;
  } });
  var platform_1 = require_platform2();
  Object.defineProperty(exports, "hostDetector", { enumerable: true, get: function() {
    return platform_1.hostDetector;
  } });
  Object.defineProperty(exports, "osDetector", { enumerable: true, get: function() {
    return platform_1.osDetector;
  } });
  Object.defineProperty(exports, "processDetector", { enumerable: true, get: function() {
    return platform_1.processDetector;
  } });
  Object.defineProperty(exports, "serviceInstanceIdDetector", { enumerable: true, get: function() {
    return platform_1.serviceInstanceIdDetector;
  } });
  var NoopDetector_1 = require_NoopDetector();
  Object.defineProperty(exports, "noopDetector", { enumerable: true, get: function() {
    return NoopDetector_1.noopDetector;
  } });
});

// node_modules/.bun/@opentelemetry+resources@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/resources/build/src/index.js
var require_src7 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.defaultServiceName = exports.emptyResource = exports.defaultResource = exports.resourceFromAttributes = exports.serviceInstanceIdDetector = exports.processDetector = exports.osDetector = exports.hostDetector = exports.envDetector = exports.detectResources = undefined;
  var detect_resources_1 = require_detect_resources();
  Object.defineProperty(exports, "detectResources", { enumerable: true, get: function() {
    return detect_resources_1.detectResources;
  } });
  var detectors_1 = require_detectors();
  Object.defineProperty(exports, "envDetector", { enumerable: true, get: function() {
    return detectors_1.envDetector;
  } });
  Object.defineProperty(exports, "hostDetector", { enumerable: true, get: function() {
    return detectors_1.hostDetector;
  } });
  Object.defineProperty(exports, "osDetector", { enumerable: true, get: function() {
    return detectors_1.osDetector;
  } });
  Object.defineProperty(exports, "processDetector", { enumerable: true, get: function() {
    return detectors_1.processDetector;
  } });
  Object.defineProperty(exports, "serviceInstanceIdDetector", { enumerable: true, get: function() {
    return detectors_1.serviceInstanceIdDetector;
  } });
  var ResourceImpl_1 = require_ResourceImpl();
  Object.defineProperty(exports, "resourceFromAttributes", { enumerable: true, get: function() {
    return ResourceImpl_1.resourceFromAttributes;
  } });
  Object.defineProperty(exports, "defaultResource", { enumerable: true, get: function() {
    return ResourceImpl_1.defaultResource;
  } });
  Object.defineProperty(exports, "emptyResource", { enumerable: true, get: function() {
    return ResourceImpl_1.emptyResource;
  } });
  var default_service_name_1 = require_default_service_name();
  Object.defineProperty(exports, "defaultServiceName", { enumerable: true, get: function() {
    return default_service_name_1.defaultServiceName;
  } });
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/view/ViewRegistry.js
var require_ViewRegistry = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ViewRegistry = undefined;

  class ViewRegistry {
    _registeredViews = [];
    addView(view) {
      this._registeredViews.push(view);
    }
    findViews(instrument, meter) {
      const views = this._registeredViews.filter((registeredView) => {
        return this._matchInstrument(registeredView.instrumentSelector, instrument) && this._matchMeter(registeredView.meterSelector, meter);
      });
      return views;
    }
    _matchInstrument(selector, instrument) {
      return (selector.getType() === undefined || instrument.type === selector.getType()) && selector.getNameFilter().match(instrument.name) && selector.getUnitFilter().match(instrument.unit);
    }
    _matchMeter(selector, meter) {
      return selector.getNameFilter().match(meter.name) && (meter.version === undefined || selector.getVersionFilter().match(meter.version)) && (meter.schemaUrl === undefined || selector.getSchemaUrlFilter().match(meter.schemaUrl));
    }
  }
  exports.ViewRegistry = ViewRegistry;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/InstrumentDescriptor.js
var require_InstrumentDescriptor = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.isValidName = exports.isDescriptorCompatibleWith = exports.createInstrumentDescriptorWithView = exports.createInstrumentDescriptor = undefined;
  var api_1 = require_src();
  var utils_1 = require_utils4();
  function createInstrumentDescriptor(name, type, options) {
    if (!isValidName(name)) {
      api_1.diag.warn(`Invalid metric name: "${name}". The metric name should be a ASCII string with a length no greater than 255 characters.`);
    }
    return {
      name,
      type,
      description: options?.description ?? "",
      unit: options?.unit ?? "",
      valueType: options?.valueType ?? api_1.ValueType.DOUBLE,
      advice: options?.advice ?? {}
    };
  }
  exports.createInstrumentDescriptor = createInstrumentDescriptor;
  function createInstrumentDescriptorWithView(view, instrument) {
    return {
      name: view.name ?? instrument.name,
      description: view.description ?? instrument.description,
      type: instrument.type,
      unit: instrument.unit,
      valueType: instrument.valueType,
      advice: instrument.advice
    };
  }
  exports.createInstrumentDescriptorWithView = createInstrumentDescriptorWithView;
  function isDescriptorCompatibleWith(descriptor, otherDescriptor) {
    return (0, utils_1.equalsCaseInsensitive)(descriptor.name, otherDescriptor.name) && descriptor.unit === otherDescriptor.unit && descriptor.type === otherDescriptor.type && descriptor.valueType === otherDescriptor.valueType;
  }
  exports.isDescriptorCompatibleWith = isDescriptorCompatibleWith;
  var NAME_REGEXP = /^[a-z][a-z0-9_.\-/]{0,254}$/i;
  function isValidName(name) {
    return NAME_REGEXP.test(name);
  }
  exports.isValidName = isValidName;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/Instruments.js
var require_Instruments = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.isObservableInstrument = exports.ObservableUpDownCounterInstrument = exports.ObservableGaugeInstrument = exports.ObservableCounterInstrument = exports.ObservableInstrument = exports.HistogramInstrument = exports.GaugeInstrument = exports.CounterInstrument = exports.UpDownCounterInstrument = exports.SyncInstrument = undefined;
  var api_1 = require_src();

  class SyncInstrument {
    _writableMetricStorage;
    _descriptor;
    constructor(writableMetricStorage, descriptor) {
      this._writableMetricStorage = writableMetricStorage;
      this._descriptor = descriptor;
    }
    _record(value, attributes = {}, context) {
      if (typeof value !== "number") {
        api_1.diag.warn(`non-number value provided to metric ${this._descriptor.name}: ${value}`);
        return;
      }
      if (this._descriptor.valueType === api_1.ValueType.INT && !Number.isInteger(value)) {
        api_1.diag.warn(`INT value type cannot accept a floating-point value for ${this._descriptor.name}, ignoring the fractional digits.`);
        value = Math.trunc(value);
        if (!Number.isInteger(value)) {
          return;
        }
      }
      this._writableMetricStorage.record(value, attributes, context, Date.now());
    }
  }
  exports.SyncInstrument = SyncInstrument;

  class UpDownCounterInstrument extends SyncInstrument {
    add(value, attributes, ctx) {
      this._record(value, attributes, ctx);
    }
  }
  exports.UpDownCounterInstrument = UpDownCounterInstrument;

  class CounterInstrument extends SyncInstrument {
    add(value, attributes, ctx) {
      if (value < 0) {
        api_1.diag.warn(`negative value provided to counter ${this._descriptor.name}: ${value}`);
        return;
      }
      this._record(value, attributes, ctx);
    }
  }
  exports.CounterInstrument = CounterInstrument;

  class GaugeInstrument extends SyncInstrument {
    record(value, attributes, ctx) {
      this._record(value, attributes, ctx);
    }
  }
  exports.GaugeInstrument = GaugeInstrument;

  class HistogramInstrument extends SyncInstrument {
    record(value, attributes, ctx) {
      if (value < 0) {
        api_1.diag.warn(`negative value provided to histogram ${this._descriptor.name}: ${value}`);
        return;
      }
      this._record(value, attributes, ctx);
    }
  }
  exports.HistogramInstrument = HistogramInstrument;

  class ObservableInstrument {
    _metricStorages;
    _descriptor;
    _observableRegistry;
    constructor(descriptor, metricStorages, observableRegistry) {
      this._descriptor = descriptor;
      this._metricStorages = metricStorages;
      this._observableRegistry = observableRegistry;
    }
    addCallback(callback) {
      this._observableRegistry.addCallback(callback, this);
    }
    removeCallback(callback) {
      this._observableRegistry.removeCallback(callback, this);
    }
  }
  exports.ObservableInstrument = ObservableInstrument;

  class ObservableCounterInstrument extends ObservableInstrument {
  }
  exports.ObservableCounterInstrument = ObservableCounterInstrument;

  class ObservableGaugeInstrument extends ObservableInstrument {
  }
  exports.ObservableGaugeInstrument = ObservableGaugeInstrument;

  class ObservableUpDownCounterInstrument extends ObservableInstrument {
  }
  exports.ObservableUpDownCounterInstrument = ObservableUpDownCounterInstrument;
  function isObservableInstrument(it) {
    return it instanceof ObservableInstrument;
  }
  exports.isObservableInstrument = isObservableInstrument;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/Meter.js
var require_Meter = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.Meter = undefined;
  var InstrumentDescriptor_1 = require_InstrumentDescriptor();
  var Instruments_1 = require_Instruments();
  var MetricData_1 = require_MetricData();

  class Meter {
    _meterSharedState;
    constructor(meterSharedState) {
      this._meterSharedState = meterSharedState;
    }
    createGauge(name, options) {
      const descriptor = (0, InstrumentDescriptor_1.createInstrumentDescriptor)(name, MetricData_1.InstrumentType.GAUGE, options);
      const storage = this._meterSharedState.registerMetricStorage(descriptor);
      return new Instruments_1.GaugeInstrument(storage, descriptor);
    }
    createHistogram(name, options) {
      const descriptor = (0, InstrumentDescriptor_1.createInstrumentDescriptor)(name, MetricData_1.InstrumentType.HISTOGRAM, options);
      const storage = this._meterSharedState.registerMetricStorage(descriptor);
      return new Instruments_1.HistogramInstrument(storage, descriptor);
    }
    createCounter(name, options) {
      const descriptor = (0, InstrumentDescriptor_1.createInstrumentDescriptor)(name, MetricData_1.InstrumentType.COUNTER, options);
      const storage = this._meterSharedState.registerMetricStorage(descriptor);
      return new Instruments_1.CounterInstrument(storage, descriptor);
    }
    createUpDownCounter(name, options) {
      const descriptor = (0, InstrumentDescriptor_1.createInstrumentDescriptor)(name, MetricData_1.InstrumentType.UP_DOWN_COUNTER, options);
      const storage = this._meterSharedState.registerMetricStorage(descriptor);
      return new Instruments_1.UpDownCounterInstrument(storage, descriptor);
    }
    createObservableGauge(name, options) {
      const descriptor = (0, InstrumentDescriptor_1.createInstrumentDescriptor)(name, MetricData_1.InstrumentType.OBSERVABLE_GAUGE, options);
      const storages = this._meterSharedState.registerAsyncMetricStorage(descriptor);
      return new Instruments_1.ObservableGaugeInstrument(descriptor, storages, this._meterSharedState.observableRegistry);
    }
    createObservableCounter(name, options) {
      const descriptor = (0, InstrumentDescriptor_1.createInstrumentDescriptor)(name, MetricData_1.InstrumentType.OBSERVABLE_COUNTER, options);
      const storages = this._meterSharedState.registerAsyncMetricStorage(descriptor);
      return new Instruments_1.ObservableCounterInstrument(descriptor, storages, this._meterSharedState.observableRegistry);
    }
    createObservableUpDownCounter(name, options) {
      const descriptor = (0, InstrumentDescriptor_1.createInstrumentDescriptor)(name, MetricData_1.InstrumentType.OBSERVABLE_UP_DOWN_COUNTER, options);
      const storages = this._meterSharedState.registerAsyncMetricStorage(descriptor);
      return new Instruments_1.ObservableUpDownCounterInstrument(descriptor, storages, this._meterSharedState.observableRegistry);
    }
    addBatchObservableCallback(callback, observables) {
      this._meterSharedState.observableRegistry.addBatchCallback(callback, observables);
    }
    removeBatchObservableCallback(callback, observables) {
      this._meterSharedState.observableRegistry.removeBatchCallback(callback, observables);
    }
  }
  exports.Meter = Meter;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/state/MetricStorage.js
var require_MetricStorage = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.MetricStorage = undefined;
  var InstrumentDescriptor_1 = require_InstrumentDescriptor();

  class MetricStorage {
    _instrumentDescriptor;
    constructor(instrumentDescriptor) {
      this._instrumentDescriptor = instrumentDescriptor;
    }
    getInstrumentDescriptor() {
      return this._instrumentDescriptor;
    }
    updateDescription(description) {
      this._instrumentDescriptor = (0, InstrumentDescriptor_1.createInstrumentDescriptor)(this._instrumentDescriptor.name, this._instrumentDescriptor.type, {
        description,
        valueType: this._instrumentDescriptor.valueType,
        unit: this._instrumentDescriptor.unit,
        advice: this._instrumentDescriptor.advice
      });
    }
  }
  exports.MetricStorage = MetricStorage;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/state/HashMap.js
var require_HashMap = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.AttributeHashMap = exports.HashMap = undefined;
  var utils_1 = require_utils4();

  class HashMap {
    _valueMap = new Map;
    _keyMap = new Map;
    _hash;
    constructor(hash) {
      this._hash = hash;
    }
    get(key, hashCode) {
      hashCode ??= this._hash(key);
      return this._valueMap.get(hashCode);
    }
    getOrDefault(key, defaultFactory) {
      const hash = this._hash(key);
      if (this._valueMap.has(hash)) {
        return this._valueMap.get(hash);
      }
      const val = defaultFactory();
      if (!this._keyMap.has(hash)) {
        this._keyMap.set(hash, key);
      }
      this._valueMap.set(hash, val);
      return val;
    }
    set(key, value, hashCode) {
      hashCode ??= this._hash(key);
      if (!this._keyMap.has(hashCode)) {
        this._keyMap.set(hashCode, key);
      }
      this._valueMap.set(hashCode, value);
    }
    has(key, hashCode) {
      hashCode ??= this._hash(key);
      return this._valueMap.has(hashCode);
    }
    *keys() {
      const keyIterator = this._keyMap.entries();
      let next = keyIterator.next();
      while (next.done !== true) {
        yield [next.value[1], next.value[0]];
        next = keyIterator.next();
      }
    }
    *entries() {
      const valueIterator = this._valueMap.entries();
      let next = valueIterator.next();
      while (next.done !== true) {
        yield [this._keyMap.get(next.value[0]), next.value[1], next.value[0]];
        next = valueIterator.next();
      }
    }
    get size() {
      return this._valueMap.size;
    }
  }
  exports.HashMap = HashMap;

  class AttributeHashMap extends HashMap {
    constructor() {
      super(utils_1.hashAttributes);
    }
  }
  exports.AttributeHashMap = AttributeHashMap;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/state/DeltaMetricProcessor.js
var require_DeltaMetricProcessor = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.DeltaMetricProcessor = undefined;
  var core_1 = require_src5();
  var utils_1 = require_utils4();
  var HashMap_1 = require_HashMap();

  class DeltaMetricProcessor {
    _activeCollectionStorage = new HashMap_1.AttributeHashMap;
    _cumulativeMemoStorage = new HashMap_1.AttributeHashMap;
    _cardinalityLimit;
    _overflowAttributes = { "otel.metric.overflow": true };
    _overflowHashCode;
    _aggregator;
    constructor(aggregator, aggregationCardinalityLimit) {
      this._aggregator = aggregator;
      this._cardinalityLimit = (aggregationCardinalityLimit ?? 2000) - 1;
      this._overflowHashCode = (0, utils_1.hashAttributes)(this._overflowAttributes);
    }
    record(value, attributes, collectionTime) {
      let accumulation = this._activeCollectionStorage.get(attributes);
      if (!accumulation) {
        const hrTime = (0, core_1.millisToHrTime)(collectionTime);
        if (this._activeCollectionStorage.size >= this._cardinalityLimit) {
          const overflowAccumulation = this._activeCollectionStorage.getOrDefault(this._overflowAttributes, () => this._aggregator.createAccumulation(hrTime));
          overflowAccumulation?.record(value);
          return;
        }
        accumulation = this._aggregator.createAccumulation(hrTime);
        this._activeCollectionStorage.set(attributes, accumulation);
      }
      accumulation?.record(value);
    }
    batchCumulate(measurements, collectionTime) {
      for (const [originalAttributes, value, originalHashCode] of measurements.entries()) {
        let attributes = originalAttributes;
        let hashCode = originalHashCode;
        const accumulation = this._aggregator.createAccumulation(collectionTime);
        accumulation?.record(value);
        let delta = accumulation;
        if (this._cumulativeMemoStorage.has(attributes, hashCode)) {
          const previous = this._cumulativeMemoStorage.get(attributes, hashCode);
          delta = this._aggregator.diff(previous, accumulation);
        } else {
          if (this._cumulativeMemoStorage.size >= this._cardinalityLimit) {
            attributes = this._overflowAttributes;
            hashCode = this._overflowHashCode;
            if (this._cumulativeMemoStorage.has(attributes, hashCode)) {
              const previous = this._cumulativeMemoStorage.get(attributes, hashCode);
              delta = this._aggregator.diff(previous, accumulation);
            }
          }
        }
        if (this._activeCollectionStorage.has(attributes, hashCode)) {
          const active = this._activeCollectionStorage.get(attributes, hashCode);
          delta = this._aggregator.merge(active, delta);
        }
        this._cumulativeMemoStorage.set(attributes, accumulation, hashCode);
        this._activeCollectionStorage.set(attributes, delta, hashCode);
      }
    }
    collect() {
      const unreportedDelta = this._activeCollectionStorage;
      this._activeCollectionStorage = new HashMap_1.AttributeHashMap;
      return unreportedDelta;
    }
  }
  exports.DeltaMetricProcessor = DeltaMetricProcessor;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/state/TemporalMetricProcessor.js
var require_TemporalMetricProcessor = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.TemporalMetricProcessor = undefined;
  var AggregationTemporality_1 = require_AggregationTemporality();
  var HashMap_1 = require_HashMap();

  class TemporalMetricProcessor {
    _aggregator;
    _unreportedAccumulations = new Map;
    _reportHistory = new Map;
    constructor(aggregator, collectorHandles) {
      this._aggregator = aggregator;
      collectorHandles.forEach((handle) => {
        this._unreportedAccumulations.set(handle, []);
      });
    }
    buildMetrics(collector, instrumentDescriptor, currentAccumulations, collectionTime) {
      this._stashAccumulations(currentAccumulations);
      const unreportedAccumulations = this._getMergedUnreportedAccumulations(collector);
      let result = unreportedAccumulations;
      let aggregationTemporality;
      if (this._reportHistory.has(collector)) {
        const last = this._reportHistory.get(collector);
        const lastCollectionTime = last.collectionTime;
        aggregationTemporality = last.aggregationTemporality;
        if (aggregationTemporality === AggregationTemporality_1.AggregationTemporality.CUMULATIVE) {
          result = TemporalMetricProcessor.merge(last.accumulations, unreportedAccumulations, this._aggregator);
        } else {
          result = TemporalMetricProcessor.calibrateStartTime(last.accumulations, unreportedAccumulations, lastCollectionTime);
        }
      } else {
        aggregationTemporality = collector.selectAggregationTemporality(instrumentDescriptor.type);
      }
      this._reportHistory.set(collector, {
        accumulations: result,
        collectionTime,
        aggregationTemporality
      });
      const accumulationRecords = AttributesMapToAccumulationRecords(result);
      if (accumulationRecords.length === 0) {
        return;
      }
      return this._aggregator.toMetricData(instrumentDescriptor, aggregationTemporality, accumulationRecords, collectionTime);
    }
    _stashAccumulations(currentAccumulation) {
      const registeredCollectors = this._unreportedAccumulations.keys();
      for (const collector of registeredCollectors) {
        let stash = this._unreportedAccumulations.get(collector);
        if (stash === undefined) {
          stash = [];
          this._unreportedAccumulations.set(collector, stash);
        }
        stash.push(currentAccumulation);
      }
    }
    _getMergedUnreportedAccumulations(collector) {
      let result = new HashMap_1.AttributeHashMap;
      const unreportedList = this._unreportedAccumulations.get(collector);
      this._unreportedAccumulations.set(collector, []);
      if (unreportedList === undefined) {
        return result;
      }
      for (const it of unreportedList) {
        result = TemporalMetricProcessor.merge(result, it, this._aggregator);
      }
      return result;
    }
    static merge(last, current, aggregator) {
      const result = last;
      const iterator = current.entries();
      let next = iterator.next();
      while (next.done !== true) {
        const [key, record, hash] = next.value;
        if (last.has(key, hash)) {
          const lastAccumulation = last.get(key, hash);
          const accumulation = aggregator.merge(lastAccumulation, record);
          result.set(key, accumulation, hash);
        } else {
          result.set(key, record, hash);
        }
        next = iterator.next();
      }
      return result;
    }
    static calibrateStartTime(last, current, lastCollectionTime) {
      for (const [key, hash] of last.keys()) {
        const currentAccumulation = current.get(key, hash);
        currentAccumulation?.setStartTime(lastCollectionTime);
      }
      return current;
    }
  }
  exports.TemporalMetricProcessor = TemporalMetricProcessor;
  function AttributesMapToAccumulationRecords(map) {
    return Array.from(map.entries());
  }
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/state/AsyncMetricStorage.js
var require_AsyncMetricStorage = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.AsyncMetricStorage = undefined;
  var MetricStorage_1 = require_MetricStorage();
  var DeltaMetricProcessor_1 = require_DeltaMetricProcessor();
  var TemporalMetricProcessor_1 = require_TemporalMetricProcessor();
  var HashMap_1 = require_HashMap();

  class AsyncMetricStorage extends MetricStorage_1.MetricStorage {
    _aggregationCardinalityLimit;
    _deltaMetricStorage;
    _temporalMetricStorage;
    _attributesProcessor;
    constructor(_instrumentDescriptor, aggregator, attributesProcessor, collectorHandles, aggregationCardinalityLimit) {
      super(_instrumentDescriptor);
      this._aggregationCardinalityLimit = aggregationCardinalityLimit;
      this._deltaMetricStorage = new DeltaMetricProcessor_1.DeltaMetricProcessor(aggregator, this._aggregationCardinalityLimit);
      this._temporalMetricStorage = new TemporalMetricProcessor_1.TemporalMetricProcessor(aggregator, collectorHandles);
      this._attributesProcessor = attributesProcessor;
    }
    record(measurements, observationTime) {
      if (this._attributesProcessor === undefined) {
        this._deltaMetricStorage.batchCumulate(measurements, observationTime);
        return;
      }
      const processed = new HashMap_1.AttributeHashMap;
      for (const [attributes, value] of measurements.entries()) {
        processed.set(this._attributesProcessor.process(attributes), value);
      }
      this._deltaMetricStorage.batchCumulate(processed, observationTime);
    }
    collect(collector, collectionTime) {
      const accumulations = this._deltaMetricStorage.collect();
      return this._temporalMetricStorage.buildMetrics(collector, this._instrumentDescriptor, accumulations, collectionTime);
    }
  }
  exports.AsyncMetricStorage = AsyncMetricStorage;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/view/RegistrationConflicts.js
var require_RegistrationConflicts = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.getConflictResolutionRecipe = exports.getDescriptionResolutionRecipe = exports.getTypeConflictResolutionRecipe = exports.getUnitConflictResolutionRecipe = exports.getValueTypeConflictResolutionRecipe = exports.getIncompatibilityDetails = undefined;
  function getIncompatibilityDetails(existing, otherDescriptor) {
    let incompatibility = "";
    if (existing.unit !== otherDescriptor.unit) {
      incompatibility += `	- Unit '${existing.unit}' does not match '${otherDescriptor.unit}'
`;
    }
    if (existing.type !== otherDescriptor.type) {
      incompatibility += `	- Type '${existing.type}' does not match '${otherDescriptor.type}'
`;
    }
    if (existing.valueType !== otherDescriptor.valueType) {
      incompatibility += `	- Value Type '${existing.valueType}' does not match '${otherDescriptor.valueType}'
`;
    }
    if (existing.description !== otherDescriptor.description) {
      incompatibility += `	- Description '${existing.description}' does not match '${otherDescriptor.description}'
`;
    }
    return incompatibility;
  }
  exports.getIncompatibilityDetails = getIncompatibilityDetails;
  function getValueTypeConflictResolutionRecipe(existing, otherDescriptor) {
    return `	- use valueType '${existing.valueType}' on instrument creation or use an instrument name other than '${otherDescriptor.name}'`;
  }
  exports.getValueTypeConflictResolutionRecipe = getValueTypeConflictResolutionRecipe;
  function getUnitConflictResolutionRecipe(existing, otherDescriptor) {
    return `	- use unit '${existing.unit}' on instrument creation or use an instrument name other than '${otherDescriptor.name}'`;
  }
  exports.getUnitConflictResolutionRecipe = getUnitConflictResolutionRecipe;
  function getTypeConflictResolutionRecipe(existing, otherDescriptor) {
    const selector = {
      name: otherDescriptor.name,
      type: otherDescriptor.type,
      unit: otherDescriptor.unit
    };
    const selectorString = JSON.stringify(selector);
    return `	- create a new view with a name other than '${existing.name}' and InstrumentSelector '${selectorString}'`;
  }
  exports.getTypeConflictResolutionRecipe = getTypeConflictResolutionRecipe;
  function getDescriptionResolutionRecipe(existing, otherDescriptor) {
    const selector = {
      name: otherDescriptor.name,
      type: otherDescriptor.type,
      unit: otherDescriptor.unit
    };
    const selectorString = JSON.stringify(selector);
    return `	- create a new view with a name other than '${existing.name}' and InstrumentSelector '${selectorString}'
    	- OR - create a new view with the name ${existing.name} and description '${existing.description}' and InstrumentSelector ${selectorString}
    	- OR - create a new view with the name ${otherDescriptor.name} and description '${existing.description}' and InstrumentSelector ${selectorString}`;
  }
  exports.getDescriptionResolutionRecipe = getDescriptionResolutionRecipe;
  function getConflictResolutionRecipe(existing, otherDescriptor) {
    if (existing.valueType !== otherDescriptor.valueType) {
      return getValueTypeConflictResolutionRecipe(existing, otherDescriptor);
    }
    if (existing.unit !== otherDescriptor.unit) {
      return getUnitConflictResolutionRecipe(existing, otherDescriptor);
    }
    if (existing.type !== otherDescriptor.type) {
      return getTypeConflictResolutionRecipe(existing, otherDescriptor);
    }
    if (existing.description !== otherDescriptor.description) {
      return getDescriptionResolutionRecipe(existing, otherDescriptor);
    }
    return "";
  }
  exports.getConflictResolutionRecipe = getConflictResolutionRecipe;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/state/MetricStorageRegistry.js
var require_MetricStorageRegistry = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.MetricStorageRegistry = undefined;
  var InstrumentDescriptor_1 = require_InstrumentDescriptor();
  var api = require_src();
  var RegistrationConflicts_1 = require_RegistrationConflicts();

  class MetricStorageRegistry {
    _sharedRegistry = new Map;
    _perCollectorRegistry = new Map;
    static create() {
      return new MetricStorageRegistry;
    }
    getStorages(collector) {
      let storages = [];
      for (const metricStorages of this._sharedRegistry.values()) {
        storages = storages.concat(metricStorages);
      }
      const perCollectorStorages = this._perCollectorRegistry.get(collector);
      if (perCollectorStorages != null) {
        for (const metricStorages of perCollectorStorages.values()) {
          storages = storages.concat(metricStorages);
        }
      }
      return storages;
    }
    register(storage) {
      this._registerStorage(storage, this._sharedRegistry);
    }
    registerForCollector(collector, storage) {
      let storageMap = this._perCollectorRegistry.get(collector);
      if (storageMap == null) {
        storageMap = new Map;
        this._perCollectorRegistry.set(collector, storageMap);
      }
      this._registerStorage(storage, storageMap);
    }
    findOrUpdateCompatibleStorage(expectedDescriptor) {
      const storages = this._sharedRegistry.get(expectedDescriptor.name);
      if (storages === undefined) {
        return null;
      }
      return this._findOrUpdateCompatibleStorage(expectedDescriptor, storages);
    }
    findOrUpdateCompatibleCollectorStorage(collector, expectedDescriptor) {
      const storageMap = this._perCollectorRegistry.get(collector);
      if (storageMap === undefined) {
        return null;
      }
      const storages = storageMap.get(expectedDescriptor.name);
      if (storages === undefined) {
        return null;
      }
      return this._findOrUpdateCompatibleStorage(expectedDescriptor, storages);
    }
    _registerStorage(storage, storageMap) {
      const descriptor = storage.getInstrumentDescriptor();
      const storages = storageMap.get(descriptor.name);
      if (storages === undefined) {
        storageMap.set(descriptor.name, [storage]);
        return;
      }
      storages.push(storage);
    }
    _findOrUpdateCompatibleStorage(expectedDescriptor, existingStorages) {
      let compatibleStorage = null;
      for (const existingStorage of existingStorages) {
        const existingDescriptor = existingStorage.getInstrumentDescriptor();
        if ((0, InstrumentDescriptor_1.isDescriptorCompatibleWith)(existingDescriptor, expectedDescriptor)) {
          if (existingDescriptor.description !== expectedDescriptor.description) {
            if (expectedDescriptor.description.length > existingDescriptor.description.length) {
              existingStorage.updateDescription(expectedDescriptor.description);
            }
            api.diag.warn("A view or instrument with the name ", expectedDescriptor.name, ` has already been registered, but has a different description and is incompatible with another registered view.
`, `Details:
`, (0, RegistrationConflicts_1.getIncompatibilityDetails)(existingDescriptor, expectedDescriptor), `The longer description will be used.
To resolve the conflict:`, (0, RegistrationConflicts_1.getConflictResolutionRecipe)(existingDescriptor, expectedDescriptor));
          }
          compatibleStorage = existingStorage;
        } else {
          api.diag.warn("A view or instrument with the name ", expectedDescriptor.name, ` has already been registered and is incompatible with another registered view.
`, `Details:
`, (0, RegistrationConflicts_1.getIncompatibilityDetails)(existingDescriptor, expectedDescriptor), `To resolve the conflict:
`, (0, RegistrationConflicts_1.getConflictResolutionRecipe)(existingDescriptor, expectedDescriptor));
        }
      }
      return compatibleStorage;
    }
  }
  exports.MetricStorageRegistry = MetricStorageRegistry;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/state/MultiWritableMetricStorage.js
var require_MultiWritableMetricStorage = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.MultiMetricStorage = undefined;
  var api_1 = require_src();

  class MultiMetricStorage {
    _backingStorages;
    hasAttributeProcessor;
    constructor(backingStorages) {
      this._backingStorages = backingStorages;
      this.hasAttributeProcessor = backingStorages.some((s) => s.hasAttributeProcessor);
    }
    record(value, attributes, context2, recordTime) {
      if (this.hasAttributeProcessor && context2 === undefined) {
        context2 = api_1.context.active();
      }
      const storages = this._backingStorages;
      for (let i = 0;i < storages.length; i++) {
        storages[i].record(value, attributes, context2, recordTime);
      }
    }
  }
  exports.MultiMetricStorage = MultiMetricStorage;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/ObservableResult.js
var require_ObservableResult = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.BatchObservableResultImpl = exports.ObservableResultImpl = undefined;
  var api_1 = require_src();
  var HashMap_1 = require_HashMap();
  var Instruments_1 = require_Instruments();

  class ObservableResultImpl {
    _buffer = new HashMap_1.AttributeHashMap;
    _instrumentName;
    _valueType;
    constructor(instrumentName, valueType) {
      this._instrumentName = instrumentName;
      this._valueType = valueType;
    }
    observe(value, attributes = {}) {
      if (typeof value !== "number") {
        api_1.diag.warn(`non-number value provided to metric ${this._instrumentName}: ${value}`);
        return;
      }
      if (this._valueType === api_1.ValueType.INT && !Number.isInteger(value)) {
        api_1.diag.warn(`INT value type cannot accept a floating-point value for ${this._instrumentName}, ignoring the fractional digits.`);
        value = Math.trunc(value);
        if (!Number.isInteger(value)) {
          return;
        }
      }
      this._buffer.set(attributes, value);
    }
  }
  exports.ObservableResultImpl = ObservableResultImpl;

  class BatchObservableResultImpl {
    _buffer = new Map;
    observe(metric, value, attributes = {}) {
      if (!(0, Instruments_1.isObservableInstrument)(metric)) {
        return;
      }
      let map = this._buffer.get(metric);
      if (map == null) {
        map = new HashMap_1.AttributeHashMap;
        this._buffer.set(metric, map);
      }
      if (typeof value !== "number") {
        api_1.diag.warn(`non-number value provided to metric ${metric._descriptor.name}: ${value}`);
        return;
      }
      if (metric._descriptor.valueType === api_1.ValueType.INT && !Number.isInteger(value)) {
        api_1.diag.warn(`INT value type cannot accept a floating-point value for ${metric._descriptor.name}, ignoring the fractional digits.`);
        value = Math.trunc(value);
        if (!Number.isInteger(value)) {
          return;
        }
      }
      map.set(attributes, value);
    }
  }
  exports.BatchObservableResultImpl = BatchObservableResultImpl;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/state/ObservableRegistry.js
var require_ObservableRegistry = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ObservableRegistry = undefined;
  var api_1 = require_src();
  var Instruments_1 = require_Instruments();
  var ObservableResult_1 = require_ObservableResult();
  var utils_1 = require_utils4();

  class ObservableRegistry {
    _callbacks = [];
    _batchCallbacks = [];
    addCallback(callback, instrument) {
      const idx = this._findCallback(callback, instrument);
      if (idx >= 0) {
        return;
      }
      this._callbacks.push({ callback, instrument });
    }
    removeCallback(callback, instrument) {
      const idx = this._findCallback(callback, instrument);
      if (idx < 0) {
        return;
      }
      this._callbacks.splice(idx, 1);
    }
    addBatchCallback(callback, instruments) {
      const observableInstruments = new Set(instruments.filter(Instruments_1.isObservableInstrument));
      if (observableInstruments.size === 0) {
        api_1.diag.error("BatchObservableCallback is not associated with valid instruments", instruments);
        return;
      }
      const idx = this._findBatchCallback(callback, observableInstruments);
      if (idx >= 0) {
        return;
      }
      this._batchCallbacks.push({ callback, instruments: observableInstruments });
    }
    removeBatchCallback(callback, instruments) {
      const observableInstruments = new Set(instruments.filter(Instruments_1.isObservableInstrument));
      const idx = this._findBatchCallback(callback, observableInstruments);
      if (idx < 0) {
        return;
      }
      this._batchCallbacks.splice(idx, 1);
    }
    async observe(collectionTime, timeoutMillis) {
      const callbackFutures = this._observeCallbacks(collectionTime, timeoutMillis);
      const batchCallbackFutures = this._observeBatchCallbacks(collectionTime, timeoutMillis);
      const results = await Promise.allSettled([
        ...callbackFutures,
        ...batchCallbackFutures
      ]);
      const rejections = results.filter((result) => result.status === "rejected").map((result) => result.reason);
      return rejections;
    }
    _observeCallbacks(observationTime, timeoutMillis) {
      return this._callbacks.map(async ({ callback, instrument }) => {
        const observableResult = new ObservableResult_1.ObservableResultImpl(instrument._descriptor.name, instrument._descriptor.valueType);
        let callPromise = Promise.resolve(callback(observableResult));
        if (timeoutMillis != null) {
          callPromise = (0, utils_1.callWithTimeout)(callPromise, timeoutMillis);
        }
        await callPromise;
        instrument._metricStorages.forEach((metricStorage) => {
          metricStorage.record(observableResult._buffer, observationTime);
        });
      });
    }
    _observeBatchCallbacks(observationTime, timeoutMillis) {
      return this._batchCallbacks.map(async ({ callback, instruments }) => {
        const observableResult = new ObservableResult_1.BatchObservableResultImpl;
        let callPromise = Promise.resolve(callback(observableResult));
        if (timeoutMillis != null) {
          callPromise = (0, utils_1.callWithTimeout)(callPromise, timeoutMillis);
        }
        await callPromise;
        instruments.forEach((instrument) => {
          const buffer = observableResult._buffer.get(instrument);
          if (buffer == null) {
            return;
          }
          instrument._metricStorages.forEach((metricStorage) => {
            metricStorage.record(buffer, observationTime);
          });
        });
      });
    }
    _findCallback(callback, instrument) {
      return this._callbacks.findIndex((record) => {
        return record.callback === callback && record.instrument === instrument;
      });
    }
    _findBatchCallback(callback, instruments) {
      return this._batchCallbacks.findIndex((record) => {
        return record.callback === callback && (0, utils_1.setEquals)(record.instruments, instruments);
      });
    }
  }
  exports.ObservableRegistry = ObservableRegistry;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/state/SyncMetricStorage.js
var require_SyncMetricStorage = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.SyncMetricStorage = undefined;
  var api_1 = require_src();
  var MetricStorage_1 = require_MetricStorage();
  var DeltaMetricProcessor_1 = require_DeltaMetricProcessor();
  var TemporalMetricProcessor_1 = require_TemporalMetricProcessor();

  class SyncMetricStorage extends MetricStorage_1.MetricStorage {
    _aggregationCardinalityLimit;
    _deltaMetricStorage;
    _temporalMetricStorage;
    _attributesProcessor;
    constructor(instrumentDescriptor, aggregator, attributesProcessor, collectorHandles, aggregationCardinalityLimit) {
      super(instrumentDescriptor);
      this._aggregationCardinalityLimit = aggregationCardinalityLimit;
      this._deltaMetricStorage = new DeltaMetricProcessor_1.DeltaMetricProcessor(aggregator, this._aggregationCardinalityLimit);
      this._temporalMetricStorage = new TemporalMetricProcessor_1.TemporalMetricProcessor(aggregator, collectorHandles);
      this._attributesProcessor = attributesProcessor;
      this.hasAttributeProcessor = attributesProcessor !== undefined;
    }
    hasAttributeProcessor;
    record(value, attributes, context2, recordTime) {
      if (this._attributesProcessor !== undefined) {
        attributes = this._attributesProcessor.process(attributes, context2 ?? api_1.context.active());
      }
      this._deltaMetricStorage.record(value, attributes, recordTime);
    }
    collect(collector, collectionTime) {
      const accumulations = this._deltaMetricStorage.collect();
      return this._temporalMetricStorage.buildMetrics(collector, this._instrumentDescriptor, accumulations, collectionTime);
    }
  }
  exports.SyncMetricStorage = SyncMetricStorage;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/state/MeterSharedState.js
var require_MeterSharedState = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.MeterSharedState = undefined;
  var InstrumentDescriptor_1 = require_InstrumentDescriptor();
  var Meter_1 = require_Meter();
  var AsyncMetricStorage_1 = require_AsyncMetricStorage();
  var MetricStorageRegistry_1 = require_MetricStorageRegistry();
  var MultiWritableMetricStorage_1 = require_MultiWritableMetricStorage();
  var ObservableRegistry_1 = require_ObservableRegistry();
  var SyncMetricStorage_1 = require_SyncMetricStorage();

  class MeterSharedState {
    metricStorageRegistry = new MetricStorageRegistry_1.MetricStorageRegistry;
    observableRegistry = new ObservableRegistry_1.ObservableRegistry;
    meter;
    _meterProviderSharedState;
    _instrumentationScope;
    constructor(meterProviderSharedState, instrumentationScope) {
      this.meter = new Meter_1.Meter(this);
      this._meterProviderSharedState = meterProviderSharedState;
      this._instrumentationScope = instrumentationScope;
    }
    registerMetricStorage(descriptor) {
      const storages = this._registerMetricStorage(descriptor, SyncMetricStorage_1.SyncMetricStorage);
      if (storages.length === 1) {
        return storages[0];
      }
      return new MultiWritableMetricStorage_1.MultiMetricStorage(storages);
    }
    registerAsyncMetricStorage(descriptor) {
      const storages = this._registerMetricStorage(descriptor, AsyncMetricStorage_1.AsyncMetricStorage);
      return storages;
    }
    async collect(collector, collectionTime, options) {
      const errors = await this.observableRegistry.observe(collectionTime, options?.timeoutMillis);
      const storages = this.metricStorageRegistry.getStorages(collector);
      if (storages.length === 0) {
        return null;
      }
      const metricDataList = [];
      storages.forEach((metricStorage) => {
        const metricData = metricStorage.collect(collector, collectionTime);
        if (metricData != null) {
          metricDataList.push(metricData);
        }
      });
      if (metricDataList.length === 0) {
        return { errors };
      }
      return {
        scopeMetrics: {
          scope: this._instrumentationScope,
          metrics: metricDataList
        },
        errors
      };
    }
    _registerMetricStorage(descriptor, MetricStorageType) {
      const views = this._meterProviderSharedState.viewRegistry.findViews(descriptor, this._instrumentationScope);
      let storages = views.map((view) => {
        const viewDescriptor = (0, InstrumentDescriptor_1.createInstrumentDescriptorWithView)(view, descriptor);
        const compatibleStorage = this.metricStorageRegistry.findOrUpdateCompatibleStorage(viewDescriptor);
        if (compatibleStorage != null) {
          return compatibleStorage;
        }
        const aggregator = view.aggregation.createAggregator(viewDescriptor);
        const viewStorage = new MetricStorageType(viewDescriptor, aggregator, view.attributesProcessor, this._meterProviderSharedState.metricCollectors, view.aggregationCardinalityLimit);
        this.metricStorageRegistry.register(viewStorage);
        return viewStorage;
      });
      if (storages.length === 0) {
        const perCollectorAggregations = this._meterProviderSharedState.selectAggregations(descriptor.type);
        const collectorStorages = perCollectorAggregations.map(([collector, aggregation]) => {
          const compatibleStorage = this.metricStorageRegistry.findOrUpdateCompatibleCollectorStorage(collector, descriptor);
          if (compatibleStorage != null) {
            return compatibleStorage;
          }
          const aggregator = aggregation.createAggregator(descriptor);
          const cardinalityLimit = collector.selectCardinalityLimit(descriptor.type);
          const storage = new MetricStorageType(descriptor, aggregator, undefined, [collector], cardinalityLimit);
          this.metricStorageRegistry.registerForCollector(collector, storage);
          return storage;
        });
        storages = storages.concat(collectorStorages);
      }
      return storages;
    }
  }
  exports.MeterSharedState = MeterSharedState;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/state/MeterProviderSharedState.js
var require_MeterProviderSharedState = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.MeterProviderSharedState = undefined;
  var utils_1 = require_utils4();
  var ViewRegistry_1 = require_ViewRegistry();
  var MeterSharedState_1 = require_MeterSharedState();
  var AggregationOption_1 = require_AggregationOption();

  class MeterProviderSharedState {
    viewRegistry = new ViewRegistry_1.ViewRegistry;
    metricCollectors = [];
    meterSharedStates = new Map;
    resource;
    constructor(resource) {
      this.resource = resource;
    }
    getMeterSharedState(instrumentationScope) {
      const id = (0, utils_1.instrumentationScopeId)(instrumentationScope);
      let meterSharedState = this.meterSharedStates.get(id);
      if (meterSharedState == null) {
        meterSharedState = new MeterSharedState_1.MeterSharedState(this, instrumentationScope);
        this.meterSharedStates.set(id, meterSharedState);
      }
      return meterSharedState;
    }
    selectAggregations(instrumentType) {
      const result = [];
      for (const collector of this.metricCollectors) {
        result.push([
          collector,
          (0, AggregationOption_1.toAggregation)(collector.selectAggregation(instrumentType))
        ]);
      }
      return result;
    }
  }
  exports.MeterProviderSharedState = MeterProviderSharedState;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/state/MetricCollector.js
var require_MetricCollector = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.MetricCollector = undefined;
  var core_1 = require_src5();

  class MetricCollector {
    _sharedState;
    _metricReader;
    constructor(sharedState, metricReader) {
      this._sharedState = sharedState;
      this._metricReader = metricReader;
    }
    async collect(options) {
      const collectionTime = (0, core_1.millisToHrTime)(Date.now());
      const scopeMetrics = [];
      const errors = [];
      const meterCollectionPromises = Array.from(this._sharedState.meterSharedStates.values()).map(async (meterSharedState) => {
        const current = await meterSharedState.collect(this, collectionTime, options);
        if (current?.scopeMetrics != null) {
          scopeMetrics.push(current.scopeMetrics);
        }
        if (current?.errors != null) {
          errors.push(...current.errors);
        }
      });
      await Promise.all(meterCollectionPromises);
      return {
        resourceMetrics: {
          resource: this._sharedState.resource,
          scopeMetrics
        },
        errors
      };
    }
    async forceFlush(options) {
      await this._metricReader.forceFlush(options);
    }
    async shutdown(options) {
      await this._metricReader.shutdown(options);
    }
    selectAggregationTemporality(instrumentType) {
      return this._metricReader.selectAggregationTemporality(instrumentType);
    }
    selectAggregation(instrumentType) {
      return this._metricReader.selectAggregation(instrumentType);
    }
    selectCardinalityLimit(instrumentType) {
      return this._metricReader.selectCardinalityLimit?.(instrumentType) ?? 2000;
    }
  }
  exports.MetricCollector = MetricCollector;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/view/Predicate.js
var require_Predicate = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ExactPredicate = exports.PatternPredicate = undefined;
  var ESCAPE = /[\^$\\.+?()[\]{}|]/g;

  class PatternPredicate {
    _matchAll;
    _regexp;
    constructor(pattern) {
      if (pattern === "*") {
        this._matchAll = true;
        this._regexp = /.*/;
      } else {
        this._matchAll = false;
        this._regexp = new RegExp(PatternPredicate.escapePattern(pattern));
      }
    }
    match(str) {
      if (this._matchAll) {
        return true;
      }
      return this._regexp.test(str);
    }
    static escapePattern(pattern) {
      return `^${pattern.replace(ESCAPE, "\\$&").replace("*", ".*")}$`;
    }
    static hasWildcard(pattern) {
      return pattern.includes("*");
    }
  }
  exports.PatternPredicate = PatternPredicate;

  class ExactPredicate {
    _matchAll;
    _pattern;
    constructor(pattern) {
      this._matchAll = pattern === undefined;
      this._pattern = pattern;
    }
    match(str) {
      if (this._matchAll) {
        return true;
      }
      if (str === this._pattern) {
        return true;
      }
      return false;
    }
  }
  exports.ExactPredicate = ExactPredicate;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/view/AttributesProcessor.js
var require_AttributesProcessor = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.createDenyListAttributesProcessor = exports.createAllowListAttributesProcessor = exports.createMultiAttributesProcessor = exports.createNoopAttributesProcessor = undefined;

  class NoopAttributesProcessor {
    process(incoming, _context) {
      return incoming;
    }
  }

  class MultiAttributesProcessor {
    _processors;
    constructor(processors) {
      this._processors = processors;
    }
    process(incoming, context) {
      let filteredAttributes = incoming;
      for (const processor of this._processors) {
        filteredAttributes = processor.process(filteredAttributes, context);
      }
      return filteredAttributes;
    }
  }

  class AllowListProcessor {
    _allowedAttributeNames;
    constructor(allowedAttributeNames) {
      this._allowedAttributeNames = new Set(allowedAttributeNames);
    }
    process(incoming, _context) {
      const filteredAttributes = {};
      for (const attributeName in incoming) {
        if (Object.prototype.hasOwnProperty.call(incoming, attributeName) && this._allowedAttributeNames.has(attributeName)) {
          filteredAttributes[attributeName] = incoming[attributeName];
        }
      }
      return filteredAttributes;
    }
  }

  class DenyListProcessor {
    _deniedAttributeNames;
    constructor(deniedAttributeNames) {
      this._deniedAttributeNames = new Set(deniedAttributeNames);
    }
    process(incoming, _context) {
      const filteredAttributes = {};
      for (const attributeName in incoming) {
        if (Object.prototype.hasOwnProperty.call(incoming, attributeName) && !this._deniedAttributeNames.has(attributeName)) {
          filteredAttributes[attributeName] = incoming[attributeName];
        }
      }
      return filteredAttributes;
    }
  }
  function createNoopAttributesProcessor() {
    return NOOP;
  }
  exports.createNoopAttributesProcessor = createNoopAttributesProcessor;
  function createMultiAttributesProcessor(processors) {
    return new MultiAttributesProcessor(processors);
  }
  exports.createMultiAttributesProcessor = createMultiAttributesProcessor;
  function createAllowListAttributesProcessor(attributeAllowList) {
    return new AllowListProcessor(attributeAllowList);
  }
  exports.createAllowListAttributesProcessor = createAllowListAttributesProcessor;
  function createDenyListAttributesProcessor(attributeDenyList) {
    return new DenyListProcessor(attributeDenyList);
  }
  exports.createDenyListAttributesProcessor = createDenyListAttributesProcessor;
  var NOOP = new NoopAttributesProcessor;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/view/InstrumentSelector.js
var require_InstrumentSelector = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.InstrumentSelector = undefined;
  var Predicate_1 = require_Predicate();

  class InstrumentSelector {
    _nameFilter;
    _type;
    _unitFilter;
    constructor(criteria) {
      this._nameFilter = new Predicate_1.PatternPredicate(criteria?.name ?? "*");
      this._type = criteria?.type;
      this._unitFilter = new Predicate_1.ExactPredicate(criteria?.unit);
    }
    getType() {
      return this._type;
    }
    getNameFilter() {
      return this._nameFilter;
    }
    getUnitFilter() {
      return this._unitFilter;
    }
  }
  exports.InstrumentSelector = InstrumentSelector;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/view/MeterSelector.js
var require_MeterSelector = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.MeterSelector = undefined;
  var Predicate_1 = require_Predicate();

  class MeterSelector {
    _nameFilter;
    _versionFilter;
    _schemaUrlFilter;
    constructor(criteria) {
      this._nameFilter = new Predicate_1.ExactPredicate(criteria?.name);
      this._versionFilter = new Predicate_1.ExactPredicate(criteria?.version);
      this._schemaUrlFilter = new Predicate_1.ExactPredicate(criteria?.schemaUrl);
    }
    getNameFilter() {
      return this._nameFilter;
    }
    getVersionFilter() {
      return this._versionFilter;
    }
    getSchemaUrlFilter() {
      return this._schemaUrlFilter;
    }
  }
  exports.MeterSelector = MeterSelector;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/view/View.js
var require_View = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.View = undefined;
  var Predicate_1 = require_Predicate();
  var AttributesProcessor_1 = require_AttributesProcessor();
  var InstrumentSelector_1 = require_InstrumentSelector();
  var MeterSelector_1 = require_MeterSelector();
  var AggregationOption_1 = require_AggregationOption();
  function isSelectorNotProvided(options) {
    return options.instrumentName == null && options.instrumentType == null && options.instrumentUnit == null && options.meterName == null && options.meterVersion == null && options.meterSchemaUrl == null;
  }
  function validateViewOptions(viewOptions) {
    if (isSelectorNotProvided(viewOptions)) {
      throw new Error("Cannot create view with no selector arguments supplied");
    }
    if (viewOptions.name != null && (viewOptions?.instrumentName == null || Predicate_1.PatternPredicate.hasWildcard(viewOptions.instrumentName))) {
      throw new Error("Views with a specified name must be declared with an instrument selector that selects at most one instrument per meter.");
    }
  }

  class View {
    name;
    description;
    aggregation;
    attributesProcessor;
    instrumentSelector;
    meterSelector;
    aggregationCardinalityLimit;
    constructor(viewOptions) {
      validateViewOptions(viewOptions);
      if (viewOptions.attributesProcessors != null) {
        this.attributesProcessor = (0, AttributesProcessor_1.createMultiAttributesProcessor)(viewOptions.attributesProcessors);
      } else {
        this.attributesProcessor = (0, AttributesProcessor_1.createNoopAttributesProcessor)();
      }
      this.name = viewOptions.name;
      this.description = viewOptions.description;
      this.aggregation = (0, AggregationOption_1.toAggregation)(viewOptions.aggregation ?? { type: AggregationOption_1.AggregationType.DEFAULT });
      this.instrumentSelector = new InstrumentSelector_1.InstrumentSelector({
        name: viewOptions.instrumentName,
        type: viewOptions.instrumentType,
        unit: viewOptions.instrumentUnit
      });
      this.meterSelector = new MeterSelector_1.MeterSelector({
        name: viewOptions.meterName,
        version: viewOptions.meterVersion,
        schemaUrl: viewOptions.meterSchemaUrl
      });
      this.aggregationCardinalityLimit = viewOptions.aggregationCardinalityLimit;
    }
  }
  exports.View = View;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/MeterProvider.js
var require_MeterProvider = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.MeterProvider = undefined;
  var api_1 = require_src();
  var resources_1 = require_src7();
  var MetricReader_1 = require_MetricReader();
  var MeterProviderSharedState_1 = require_MeterProviderSharedState();
  var MetricCollector_1 = require_MetricCollector();
  var View_1 = require_View();

  class MeterProvider {
    _sharedState;
    _shutdown = false;
    constructor(options) {
      this._sharedState = new MeterProviderSharedState_1.MeterProviderSharedState(options?.resource ?? (0, resources_1.defaultResource)());
      if (options?.views != null && options.views.length > 0) {
        for (const viewOption of options.views) {
          this._sharedState.viewRegistry.addView(new View_1.View(viewOption));
        }
      }
      if (options?.readers != null && options.readers.length > 0) {
        for (const metricReader of options.readers) {
          const collector = new MetricCollector_1.MetricCollector(this._sharedState, metricReader);
          metricReader.setMetricProducer(collector);
          this._sharedState.metricCollectors.push(collector);
          if (options.sdkMetricsEnabled && metricReader instanceof MetricReader_1.MetricReader) {
            metricReader._setSelfObsMeterProvider(this);
          }
        }
      }
    }
    getMeter(name, version = "", options = {}) {
      if (this._shutdown) {
        api_1.diag.warn("A shutdown MeterProvider cannot provide a Meter");
        return (0, api_1.createNoopMeter)();
      }
      return this._sharedState.getMeterSharedState({
        name,
        version,
        schemaUrl: options.schemaUrl
      }).meter;
    }
    async shutdown(options) {
      if (this._shutdown) {
        api_1.diag.warn("shutdown may only be called once per MeterProvider");
        return;
      }
      this._shutdown = true;
      await Promise.all(this._sharedState.metricCollectors.map((collector) => {
        return collector.shutdown(options);
      }));
    }
    async forceFlush(options) {
      if (this._shutdown) {
        api_1.diag.warn("invalid attempt to force flush after MeterProvider shutdown");
        return;
      }
      await Promise.all(this._sharedState.metricCollectors.map((collector) => {
        return collector.forceFlush(options);
      }));
    }
  }
  exports.MeterProvider = MeterProvider;
});

// node_modules/.bun/@opentelemetry+sdk-metrics@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-metrics/build/src/index.js
var require_src8 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.TimeoutError = exports.createDenyListAttributesProcessor = exports.createAllowListAttributesProcessor = exports.AggregationType = exports.MeterProvider = exports.ConsoleMetricExporter = exports.InMemoryMetricExporter = exports.PeriodicExportingMetricReader = exports.MetricReader = exports.InstrumentType = exports.DataPointType = exports.AggregationTemporality = undefined;
  var AggregationTemporality_1 = require_AggregationTemporality();
  Object.defineProperty(exports, "AggregationTemporality", { enumerable: true, get: function() {
    return AggregationTemporality_1.AggregationTemporality;
  } });
  var MetricData_1 = require_MetricData();
  Object.defineProperty(exports, "DataPointType", { enumerable: true, get: function() {
    return MetricData_1.DataPointType;
  } });
  Object.defineProperty(exports, "InstrumentType", { enumerable: true, get: function() {
    return MetricData_1.InstrumentType;
  } });
  var MetricReader_1 = require_MetricReader();
  Object.defineProperty(exports, "MetricReader", { enumerable: true, get: function() {
    return MetricReader_1.MetricReader;
  } });
  var PeriodicExportingMetricReader_1 = require_PeriodicExportingMetricReader();
  Object.defineProperty(exports, "PeriodicExportingMetricReader", { enumerable: true, get: function() {
    return PeriodicExportingMetricReader_1.PeriodicExportingMetricReader;
  } });
  var InMemoryMetricExporter_1 = require_InMemoryMetricExporter();
  Object.defineProperty(exports, "InMemoryMetricExporter", { enumerable: true, get: function() {
    return InMemoryMetricExporter_1.InMemoryMetricExporter;
  } });
  var ConsoleMetricExporter_1 = require_ConsoleMetricExporter();
  Object.defineProperty(exports, "ConsoleMetricExporter", { enumerable: true, get: function() {
    return ConsoleMetricExporter_1.ConsoleMetricExporter;
  } });
  var MeterProvider_1 = require_MeterProvider();
  Object.defineProperty(exports, "MeterProvider", { enumerable: true, get: function() {
    return MeterProvider_1.MeterProvider;
  } });
  var AggregationOption_1 = require_AggregationOption();
  Object.defineProperty(exports, "AggregationType", { enumerable: true, get: function() {
    return AggregationOption_1.AggregationType;
  } });
  var AttributesProcessor_1 = require_AttributesProcessor();
  Object.defineProperty(exports, "createAllowListAttributesProcessor", { enumerable: true, get: function() {
    return AttributesProcessor_1.createAllowListAttributesProcessor;
  } });
  Object.defineProperty(exports, "createDenyListAttributesProcessor", { enumerable: true, get: function() {
    return AttributesProcessor_1.createDenyListAttributesProcessor;
  } });
  var utils_1 = require_utils4();
  Object.defineProperty(exports, "TimeoutError", { enumerable: true, get: function() {
    return utils_1.TimeoutError;
  } });
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/metrics/protobuf/metrics-serializer.js
var require_metrics_serializer = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.serializeMetricsExportRequest = undefined;
  var api_1 = require_src();
  var sdk_metrics_1 = require_src8();
  var common_serializer_1 = require_common_serializer();
  var protobuf_size_estimator_1 = require_protobuf_size_estimator();
  var protobuf_writer_1 = require_protobuf_writer();
  function serializeNumberDataPoint(writer, dataPoint, valueType) {
    const start = writer.startLengthDelimited();
    const startPos = writer.pos;
    writer.writeTag(2, 1);
    (0, common_serializer_1.writeHrTimeAsFixed64)(writer, dataPoint.startTime);
    writer.writeTag(3, 1);
    (0, common_serializer_1.writeHrTimeAsFixed64)(writer, dataPoint.endTime);
    if (valueType === api_1.ValueType.INT) {
      writer.writeTag(6, 1);
      writer.writeSfixed64(dataPoint.value);
    } else {
      writer.writeTag(4, 1);
      writer.writeDouble(dataPoint.value);
    }
    if (dataPoint.attributes) {
      (0, common_serializer_1.writeAttributes)(writer, dataPoint.attributes, 7);
    }
    writer.finishLengthDelimited(start, writer.pos - startPos);
  }
  function serializeHistogramDataPoint(writer, dataPoint) {
    const start = writer.startLengthDelimited();
    const startPos = writer.pos;
    const histogram = dataPoint.value;
    writer.writeTag(2, 1);
    (0, common_serializer_1.writeHrTimeAsFixed64)(writer, dataPoint.startTime);
    writer.writeTag(3, 1);
    (0, common_serializer_1.writeHrTimeAsFixed64)(writer, dataPoint.endTime);
    writer.writeTag(4, 1);
    writer.writeFixed64(histogram.count >>> 0, histogram.count / 4294967296 >>> 0);
    if (histogram.sum !== undefined) {
      writer.writeTag(5, 1);
      writer.writeDouble(histogram.sum);
    }
    if (histogram.buckets.counts.length > 0) {
      writer.writeTag(6, 2);
      const countsStart = writer.startLengthDelimited();
      const countsStartPos = writer.pos;
      for (const count of histogram.buckets.counts) {
        writer.writeFixed64(count >>> 0, count / 4294967296 >>> 0);
      }
      writer.finishLengthDelimited(countsStart, writer.pos - countsStartPos);
    }
    if (histogram.buckets.boundaries.length > 0) {
      writer.writeTag(7, 2);
      const boundsStart = writer.startLengthDelimited();
      const boundsStartPos = writer.pos;
      for (const bound of histogram.buckets.boundaries) {
        writer.writeDouble(bound);
      }
      writer.finishLengthDelimited(boundsStart, writer.pos - boundsStartPos);
    }
    if (dataPoint.attributes) {
      (0, common_serializer_1.writeAttributes)(writer, dataPoint.attributes, 9);
    }
    if (histogram.min !== undefined) {
      writer.writeTag(11, 1);
      writer.writeDouble(histogram.min);
    }
    if (histogram.max !== undefined) {
      writer.writeTag(12, 1);
      writer.writeDouble(histogram.max);
    }
    writer.finishLengthDelimited(start, writer.pos - startPos);
  }
  function serializeExponentialBuckets(writer, offset, bucketCounts) {
    const start = writer.startLengthDelimited();
    const startPos = writer.pos;
    if (offset !== 0) {
      writer.writeTag(1, 0);
      writer.writeSint32(offset);
    }
    if (bucketCounts.length > 0) {
      writer.writeTag(2, 2);
      const bcStart = writer.startLengthDelimited();
      const bcStartPos = writer.pos;
      for (const count of bucketCounts) {
        writer.writeVarint(count);
      }
      writer.finishLengthDelimited(bcStart, writer.pos - bcStartPos);
    }
    writer.finishLengthDelimited(start, writer.pos - startPos);
  }
  function serializeExponentialHistogramDataPoint(writer, dataPoint) {
    const start = writer.startLengthDelimited();
    const startPos = writer.pos;
    const histogram = dataPoint.value;
    if (dataPoint.attributes) {
      (0, common_serializer_1.writeAttributes)(writer, dataPoint.attributes, 1);
    }
    writer.writeTag(2, 1);
    (0, common_serializer_1.writeHrTimeAsFixed64)(writer, dataPoint.startTime);
    writer.writeTag(3, 1);
    (0, common_serializer_1.writeHrTimeAsFixed64)(writer, dataPoint.endTime);
    writer.writeTag(4, 1);
    writer.writeFixed64(histogram.count >>> 0, histogram.count / 4294967296 >>> 0);
    if (histogram.sum !== undefined) {
      writer.writeTag(5, 1);
      writer.writeDouble(histogram.sum);
    }
    if (histogram.scale !== 0) {
      writer.writeTag(6, 0);
      writer.writeSint32(histogram.scale);
    }
    writer.writeTag(7, 1);
    writer.writeFixed64(histogram.zeroCount >>> 0, histogram.zeroCount / 4294967296 >>> 0);
    writer.writeTag(8, 2);
    serializeExponentialBuckets(writer, histogram.positive.offset, histogram.positive.bucketCounts);
    writer.writeTag(9, 2);
    serializeExponentialBuckets(writer, histogram.negative.offset, histogram.negative.bucketCounts);
    if (histogram.min !== undefined) {
      writer.writeTag(12, 1);
      writer.writeDouble(histogram.min);
    }
    if (histogram.max !== undefined) {
      writer.writeTag(13, 1);
      writer.writeDouble(histogram.max);
    }
    writer.finishLengthDelimited(start, writer.pos - startPos);
  }
  function serializeMetric(writer, metricData) {
    const metricStart = writer.startLengthDelimited();
    const metricStartPos = writer.pos;
    writer.writeTag(1, 2);
    writer.writeString(metricData.descriptor.name);
    if (metricData.descriptor.description) {
      writer.writeTag(2, 2);
      writer.writeString(metricData.descriptor.description);
    }
    if (metricData.descriptor.unit) {
      writer.writeTag(3, 2);
      writer.writeString(metricData.descriptor.unit);
    }
    switch (metricData.dataPointType) {
      case sdk_metrics_1.DataPointType.GAUGE:
        writer.writeTag(5, 2);
        serializeGauge(writer, metricData);
        break;
      case sdk_metrics_1.DataPointType.SUM:
        writer.writeTag(7, 2);
        serializeSum(writer, metricData);
        break;
      case sdk_metrics_1.DataPointType.HISTOGRAM:
        writer.writeTag(9, 2);
        serializeHistogramMetric(writer, metricData);
        break;
      case sdk_metrics_1.DataPointType.EXPONENTIAL_HISTOGRAM:
        writer.writeTag(10, 2);
        serializeExponentialHistogramMetric(writer, metricData);
        break;
      default: {
        const _exhaustive = metricData;
      }
    }
    writer.finishLengthDelimited(metricStart, writer.pos - metricStartPos);
  }
  function serializeGauge(writer, metricData) {
    const start = writer.startLengthDelimited();
    const startPos = writer.pos;
    for (const dataPoint of metricData.dataPoints) {
      writer.writeTag(1, 2);
      serializeNumberDataPoint(writer, dataPoint, metricData.descriptor.valueType);
    }
    writer.finishLengthDelimited(start, writer.pos - startPos);
  }
  function serializeSum(writer, metricData) {
    const start = writer.startLengthDelimited();
    const startPos = writer.pos;
    for (const dataPoint of metricData.dataPoints) {
      writer.writeTag(1, 2);
      serializeNumberDataPoint(writer, dataPoint, metricData.descriptor.valueType);
    }
    const temporality = toProtoAggregationTemporality(metricData.aggregationTemporality);
    if (temporality !== 0) {
      writer.writeTag(2, 0);
      writer.writeVarint(temporality);
    }
    if (metricData.isMonotonic) {
      writer.writeTag(3, 0);
      writer.writeVarint(1);
    }
    writer.finishLengthDelimited(start, writer.pos - startPos);
  }
  function serializeHistogramMetric(writer, metricData) {
    const start = writer.startLengthDelimited();
    const startPos = writer.pos;
    for (const dataPoint of metricData.dataPoints) {
      writer.writeTag(1, 2);
      serializeHistogramDataPoint(writer, dataPoint);
    }
    const temporality = toProtoAggregationTemporality(metricData.aggregationTemporality);
    if (temporality !== 0) {
      writer.writeTag(2, 0);
      writer.writeVarint(temporality);
    }
    writer.finishLengthDelimited(start, writer.pos - startPos);
  }
  function serializeExponentialHistogramMetric(writer, metricData) {
    const start = writer.startLengthDelimited();
    const startPos = writer.pos;
    for (const dataPoint of metricData.dataPoints) {
      writer.writeTag(1, 2);
      serializeExponentialHistogramDataPoint(writer, dataPoint);
    }
    const temporality = toProtoAggregationTemporality(metricData.aggregationTemporality);
    if (temporality !== 0) {
      writer.writeTag(2, 0);
      writer.writeVarint(temporality);
    }
    writer.finishLengthDelimited(start, writer.pos - startPos);
  }
  function serializeScopeMetrics(writer, scopeMetrics) {
    const scopeStart = writer.startLengthDelimited();
    const scopeStartPos = writer.pos;
    (0, common_serializer_1.writeInstrumentationScope)(writer, scopeMetrics.scope, 1);
    for (const metric of scopeMetrics.metrics) {
      writer.writeTag(2, 2);
      serializeMetric(writer, metric);
    }
    if (scopeMetrics.scope.schemaUrl) {
      writer.writeTag(3, 2);
      writer.writeString(scopeMetrics.scope.schemaUrl);
    }
    writer.finishLengthDelimited(scopeStart, writer.pos - scopeStartPos);
  }
  function serializeResourceMetrics(writer, resourceMetrics) {
    const start = writer.startLengthDelimited();
    const startPos = writer.pos;
    (0, common_serializer_1.writeResource)(writer, resourceMetrics.resource, 1);
    for (const scopeMetrics of resourceMetrics.scopeMetrics) {
      writer.writeTag(2, 2);
      serializeScopeMetrics(writer, scopeMetrics);
    }
    if (resourceMetrics.resource.schemaUrl) {
      writer.writeTag(3, 2);
      writer.writeString(resourceMetrics.resource.schemaUrl);
    }
    writer.finishLengthDelimited(start, writer.pos - startPos);
  }
  function toProtoAggregationTemporality(temporality) {
    switch (temporality) {
      case sdk_metrics_1.AggregationTemporality.DELTA:
        return 1;
      case sdk_metrics_1.AggregationTemporality.CUMULATIVE:
        return 2;
      default:
        return 0;
    }
  }
  function serializeMetricsExportRequest(resourceMetrics) {
    const estimator = new protobuf_size_estimator_1.ProtobufSizeEstimator;
    estimator.writeTag(1, 2);
    serializeResourceMetrics(estimator, resourceMetrics);
    const writer = new protobuf_writer_1.ProtobufWriter(estimator.pos);
    writer.writeTag(1, 2);
    serializeResourceMetrics(writer, resourceMetrics);
    return writer.finish();
  }
  exports.serializeMetricsExportRequest = serializeMetricsExportRequest;
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/metrics/protobuf/response-deserializer.js
var require_response_deserializer2 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.deserializeExportMetricsServiceResponse = undefined;
  var protobuf_reader_1 = require_protobuf_reader();
  function deserializePartialSuccess(data) {
    const reader = new protobuf_reader_1.ProtobufReader(data);
    const result = {};
    while (!reader.isAtEnd()) {
      const { fieldNumber, wireType } = reader.readTag();
      switch (fieldNumber) {
        case 1:
          if (wireType === 0) {
            result.rejectedDataPoints = reader.readVarint();
          } else {
            reader.skip(wireType);
          }
          break;
        case 2:
          if (wireType === 2) {
            result.errorMessage = reader.readString();
          } else {
            reader.skip(wireType);
          }
          break;
        default:
          reader.skip(wireType);
          break;
      }
    }
    return result;
  }
  function deserializeExportMetricsServiceResponse(data) {
    const reader = new protobuf_reader_1.ProtobufReader(data);
    const result = {};
    while (!reader.isAtEnd()) {
      const { fieldNumber, wireType } = reader.readTag();
      switch (fieldNumber) {
        case 1:
          if (wireType === 2) {
            result.partialSuccess = deserializePartialSuccess(reader.readBytes());
          } else {
            reader.skip(wireType);
          }
          break;
        default:
          reader.skip(wireType);
          break;
      }
    }
    return result;
  }
  exports.deserializeExportMetricsServiceResponse = deserializeExportMetricsServiceResponse;
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/metrics/protobuf/metrics.js
var require_metrics2 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ProtobufMetricsSerializer = undefined;
  var metrics_serializer_1 = require_metrics_serializer();
  var response_deserializer_1 = require_response_deserializer2();
  exports.ProtobufMetricsSerializer = {
    serializeRequest: (arg) => {
      return (0, metrics_serializer_1.serializeMetricsExportRequest)(arg);
    },
    deserializeResponse: (arg) => {
      return (0, response_deserializer_1.deserializeExportMetricsServiceResponse)(arg);
    }
  };
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/metrics/protobuf/index.js
var require_protobuf2 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ProtobufMetricsSerializer = undefined;
  var metrics_1 = require_metrics2();
  Object.defineProperty(exports, "ProtobufMetricsSerializer", { enumerable: true, get: function() {
    return metrics_1.ProtobufMetricsSerializer;
  } });
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/trace/protobuf/trace-serializer.js
var require_trace_serializer = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.serializeTraceExportRequest = undefined;
  var protobuf_writer_1 = require_protobuf_writer();
  var hex_to_binary_1 = require_hex_to_binary();
  var common_serializer_1 = require_common_serializer();
  var protobuf_size_estimator_1 = require_protobuf_size_estimator();
  var SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK = 256;
  var SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK = 512;
  function buildSpanFlags(traceFlags, isRemote) {
    let flags = traceFlags & 255 | SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK;
    if (isRemote) {
      flags |= SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK;
    }
    return flags;
  }
  function serializeStatus(writer, status) {
    const statusStart = writer.startLengthDelimited();
    const statusStartPos = writer.pos;
    if (status.message) {
      writer.writeTag(2, 2);
      writer.writeString(status.message);
    }
    writer.writeTag(3, 0);
    writer.writeVarint(status.code);
    writer.finishLengthDelimited(statusStart, writer.pos - statusStartPos);
  }
  function serializeEvent(writer, event) {
    const eventStart = writer.startLengthDelimited();
    const eventStartPos = writer.pos;
    writer.writeTag(1, 1);
    (0, common_serializer_1.writeHrTimeAsFixed64)(writer, event.time);
    writer.writeTag(2, 2);
    writer.writeString(event.name);
    if (event.attributes) {
      (0, common_serializer_1.writeAttributes)(writer, event.attributes, 3);
    }
    writer.writeTag(4, 0);
    writer.writeVarint(event.droppedAttributesCount || 0);
    writer.finishLengthDelimited(eventStart, writer.pos - eventStartPos);
  }
  function serializeLink(writer, link) {
    const linkStart = writer.startLengthDelimited();
    const linkStartPos = writer.pos;
    const context = link.context;
    writer.writeTag(1, 2);
    writer.writeBytes((0, hex_to_binary_1.hexToBinary)(context.traceId));
    writer.writeTag(2, 2);
    writer.writeBytes((0, hex_to_binary_1.hexToBinary)(context.spanId));
    const linkTraceState = context.traceState?.serialize();
    if (linkTraceState) {
      writer.writeTag(3, 2);
      writer.writeString(linkTraceState);
    }
    if (link.attributes) {
      (0, common_serializer_1.writeAttributes)(writer, link.attributes, 4);
    }
    writer.writeTag(5, 0);
    writer.writeVarint(link.droppedAttributesCount || 0);
    const linkFlags = buildSpanFlags(context.traceFlags, context.isRemote);
    if (linkFlags) {
      writer.writeTag(6, 5);
      writer.writeFixed32(linkFlags);
    }
    writer.finishLengthDelimited(linkStart, writer.pos - linkStartPos);
  }
  function serializeSpan(writer, span) {
    const spanStart = writer.startLengthDelimited();
    const spanStartPos = writer.pos;
    const ctx = span.spanContext();
    writer.writeTag(1, 2);
    writer.writeBytes((0, hex_to_binary_1.hexToBinary)(ctx.traceId));
    writer.writeTag(2, 2);
    writer.writeBytes((0, hex_to_binary_1.hexToBinary)(ctx.spanId));
    const traceState = ctx.traceState?.serialize();
    if (traceState) {
      writer.writeTag(3, 2);
      writer.writeString(traceState);
    }
    if (span.parentSpanContext?.spanId) {
      writer.writeTag(4, 2);
      writer.writeBytes((0, hex_to_binary_1.hexToBinary)(span.parentSpanContext.spanId));
    }
    writer.writeTag(5, 2);
    writer.writeString(span.name);
    const kind = span.kind == null ? 0 : span.kind + 1;
    if (kind !== 0) {
      writer.writeTag(6, 0);
      writer.writeVarint(kind);
    }
    writer.writeTag(7, 1);
    (0, common_serializer_1.writeHrTimeAsFixed64)(writer, span.startTime);
    writer.writeTag(8, 1);
    (0, common_serializer_1.writeHrTimeAsFixed64)(writer, span.endTime);
    if (span.attributes) {
      (0, common_serializer_1.writeAttributes)(writer, span.attributes, 9);
    }
    writer.writeTag(10, 0);
    writer.writeVarint(span.droppedAttributesCount);
    for (const event of span.events) {
      writer.writeTag(11, 2);
      serializeEvent(writer, event);
    }
    writer.writeTag(12, 0);
    writer.writeVarint(span.droppedEventsCount);
    for (const link of span.links) {
      writer.writeTag(13, 2);
      serializeLink(writer, link);
    }
    writer.writeTag(14, 0);
    writer.writeVarint(span.droppedLinksCount);
    writer.writeTag(15, 2);
    serializeStatus(writer, span.status);
    const flags = buildSpanFlags(ctx.traceFlags, span.parentSpanContext?.isRemote);
    if (flags) {
      writer.writeTag(16, 5);
      writer.writeFixed32(flags);
    }
    writer.finishLengthDelimited(spanStart, writer.pos - spanStartPos);
  }
  function serializeScopeSpans(writer, scope, spans) {
    const scopeSpansStart = writer.startLengthDelimited();
    const scopeSpansStartPos = writer.pos;
    (0, common_serializer_1.writeInstrumentationScope)(writer, scope, 1);
    for (const span of spans) {
      writer.writeTag(2, 2);
      serializeSpan(writer, span);
    }
    if (scope.schemaUrl) {
      writer.writeTag(3, 2);
      writer.writeString(scope.schemaUrl);
    }
    writer.finishLengthDelimited(scopeSpansStart, writer.pos - scopeSpansStartPos);
  }
  function serializeResourceSpans(writer, resource, scopeMap) {
    const resourceSpansStart = writer.startLengthDelimited();
    const resourceSpansStartPos = writer.pos;
    (0, common_serializer_1.writeResource)(writer, resource, 1);
    for (const scopeSpans of scopeMap.values()) {
      writer.writeTag(2, 2);
      const scope = scopeSpans[0].instrumentationScope;
      serializeScopeSpans(writer, scope, scopeSpans);
    }
    if (resource.schemaUrl) {
      writer.writeTag(3, 2);
      writer.writeString(resource.schemaUrl);
    }
    writer.finishLengthDelimited(resourceSpansStart, writer.pos - resourceSpansStartPos);
  }
  function createResourceMap(spans) {
    const resourceMap = new Map;
    for (const span of spans) {
      const resource = span.resource;
      const scope = span.instrumentationScope;
      let scopeMap = resourceMap.get(resource);
      if (!scopeMap) {
        scopeMap = new Map;
        resourceMap.set(resource, scopeMap);
      }
      let records = scopeMap.get(scope);
      if (!records) {
        records = [];
        scopeMap.set(scope, records);
      }
      records.push(span);
    }
    return resourceMap;
  }
  function serializeTraceExportRequest(spans) {
    const resourceMap = createResourceMap(spans);
    const estimator = new protobuf_size_estimator_1.ProtobufSizeEstimator;
    for (const [resource, scopeMap] of resourceMap) {
      estimator.writeTag(1, 2);
      serializeResourceSpans(estimator, resource, scopeMap);
    }
    const writer = new protobuf_writer_1.ProtobufWriter(estimator.pos);
    for (const [resource, scopeMap] of resourceMap) {
      writer.writeTag(1, 2);
      serializeResourceSpans(writer, resource, scopeMap);
    }
    return writer.finish();
  }
  exports.serializeTraceExportRequest = serializeTraceExportRequest;
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/trace/protobuf/response-deserializer.js
var require_response_deserializer3 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.deserializeExportTraceServiceResponse = undefined;
  var protobuf_reader_1 = require_protobuf_reader();
  function deserializePartialSuccess(data) {
    const reader = new protobuf_reader_1.ProtobufReader(data);
    const result = {};
    while (!reader.isAtEnd()) {
      const { fieldNumber, wireType } = reader.readTag();
      switch (fieldNumber) {
        case 1:
          if (wireType === 0) {
            result.rejectedSpans = reader.readVarint();
          } else {
            reader.skip(wireType);
          }
          break;
        case 2:
          if (wireType === 2) {
            result.errorMessage = reader.readString();
          } else {
            reader.skip(wireType);
          }
          break;
        default:
          reader.skip(wireType);
          break;
      }
    }
    return result;
  }
  function deserializeExportTraceServiceResponse(data) {
    const reader = new protobuf_reader_1.ProtobufReader(data);
    const result = {};
    while (!reader.isAtEnd()) {
      const { fieldNumber, wireType } = reader.readTag();
      switch (fieldNumber) {
        case 1:
          if (wireType === 2) {
            result.partialSuccess = deserializePartialSuccess(reader.readBytes());
          } else {
            reader.skip(wireType);
          }
          break;
        default:
          reader.skip(wireType);
          break;
      }
    }
    return result;
  }
  exports.deserializeExportTraceServiceResponse = deserializeExportTraceServiceResponse;
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/trace/protobuf/trace.js
var require_trace3 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ProtobufTraceSerializer = undefined;
  var trace_serializer_1 = require_trace_serializer();
  var response_deserializer_1 = require_response_deserializer3();
  exports.ProtobufTraceSerializer = {
    serializeRequest: (arg) => {
      return (0, trace_serializer_1.serializeTraceExportRequest)(arg);
    },
    deserializeResponse: (arg) => {
      return (0, response_deserializer_1.deserializeExportTraceServiceResponse)(arg);
    }
  };
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/trace/protobuf/index.js
var require_protobuf3 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ProtobufTraceSerializer = undefined;
  var trace_1 = require_trace3();
  Object.defineProperty(exports, "ProtobufTraceSerializer", { enumerable: true, get: function() {
    return trace_1.ProtobufTraceSerializer;
  } });
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/common/internal.js
var require_internal = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.toAnyValue = exports.toKeyValue = exports.toAttributes = exports.createInstrumentationScope = exports.createResource = undefined;
  function createResource(resource, encoder) {
    const result = {
      attributes: toAttributes(resource.attributes, encoder),
      droppedAttributesCount: 0
    };
    const schemaUrl = resource.schemaUrl;
    if (schemaUrl && schemaUrl !== "")
      result.schemaUrl = schemaUrl;
    return result;
  }
  exports.createResource = createResource;
  function createInstrumentationScope(scope, encoder) {
    const result = {
      name: scope.name,
      version: scope.version
    };
    if (scope.attributes && Object.keys(scope.attributes).length > 0) {
      result.attributes = toAttributes(scope.attributes, encoder);
      result.droppedAttributesCount = scope.droppedAttributesCount ?? 0;
    }
    return result;
  }
  exports.createInstrumentationScope = createInstrumentationScope;
  function toAttributes(attributes, encoder) {
    return Object.keys(attributes).map((key) => toKeyValue(key, attributes[key], encoder));
  }
  exports.toAttributes = toAttributes;
  function toKeyValue(key, value, encoder) {
    return {
      key,
      value: toAnyValue(value, encoder)
    };
  }
  exports.toKeyValue = toKeyValue;
  function toAnyValue(value, encoder) {
    const t = typeof value;
    if (t === "string")
      return { stringValue: value };
    if (t === "number") {
      if (!Number.isInteger(value))
        return { doubleValue: value };
      return { intValue: value };
    }
    if (t === "boolean")
      return { boolValue: value };
    if (value instanceof Uint8Array)
      return { bytesValue: encoder.encodeUint8Array(value) };
    if (Array.isArray(value)) {
      const values = new Array(value.length);
      for (let i = 0;i < value.length; i++) {
        values[i] = toAnyValue(value[i], encoder);
      }
      return { arrayValue: { values } };
    }
    if (t === "object" && value != null) {
      const keys = Object.keys(value);
      const values = new Array(keys.length);
      for (let i = 0;i < keys.length; i++) {
        values[i] = {
          key: keys[i],
          value: toAnyValue(value[keys[i]], encoder)
        };
      }
      return { kvlistValue: { values } };
    }
    return {};
  }
  exports.toAnyValue = toAnyValue;
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/logs/internal.js
var require_internal2 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.createExportLogsServiceRequest = undefined;
  var internal_1 = require_internal();
  function createExportLogsServiceRequest(logRecords, encoder) {
    return {
      resourceLogs: logRecordsToResourceLogs(logRecords, encoder)
    };
  }
  exports.createExportLogsServiceRequest = createExportLogsServiceRequest;
  function createResourceMap(logRecords) {
    const resourceMap = new Map;
    for (const record of logRecords) {
      const { resource, instrumentationScope } = record;
      let ismMap = resourceMap.get(resource);
      if (!ismMap) {
        ismMap = new Map;
        resourceMap.set(resource, ismMap);
      }
      let records = ismMap.get(instrumentationScope);
      if (!records) {
        records = [];
        ismMap.set(instrumentationScope, records);
      }
      records.push(record);
    }
    return resourceMap;
  }
  function logRecordsToResourceLogs(logRecords, encoder) {
    const resourceMap = createResourceMap(logRecords);
    return Array.from(resourceMap, ([resource, ismMap]) => {
      const processedResource = (0, internal_1.createResource)(resource, encoder);
      return {
        resource: processedResource,
        scopeLogs: Array.from(ismMap, ([, scopeLogs]) => {
          return {
            scope: (0, internal_1.createInstrumentationScope)(scopeLogs[0].instrumentationScope, encoder),
            logRecords: scopeLogs.map((log) => toLogRecord(log, encoder)),
            schemaUrl: scopeLogs[0].instrumentationScope.schemaUrl
          };
        }),
        schemaUrl: processedResource.schemaUrl
      };
    });
  }
  function toLogRecord(log, encoder) {
    return {
      timeUnixNano: encoder.encodeHrTime(log.hrTime),
      observedTimeUnixNano: encoder.encodeHrTime(log.hrTimeObserved),
      severityNumber: toSeverityNumber(log.severityNumber),
      severityText: log.severityText,
      body: (0, internal_1.toAnyValue)(log.body, encoder),
      eventName: log.eventName,
      attributes: (0, internal_1.toAttributes)(log.attributes, encoder),
      droppedAttributesCount: log.droppedAttributesCount,
      flags: log.spanContext?.traceFlags,
      traceId: encoder.encodeOptionalSpanContext(log.spanContext?.traceId),
      spanId: encoder.encodeOptionalSpanContext(log.spanContext?.spanId)
    };
  }
  function toSeverityNumber(severityNumber) {
    return severityNumber;
  }
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/common/utils.js
var require_utils7 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.JSON_ENCODER = exports.PROTOBUF_ENCODER = exports.encodeAsString = exports.encodeAsLongBits = exports.toLongBits = exports.hrTimeToNanos = undefined;
  var core_1 = require_src5();
  var hex_to_binary_1 = require_hex_to_binary();
  function hrTimeToNanos(hrTime) {
    const NANOSECONDS = BigInt(1e9);
    return BigInt(Math.trunc(hrTime[0])) * NANOSECONDS + BigInt(Math.trunc(hrTime[1]));
  }
  exports.hrTimeToNanos = hrTimeToNanos;
  function toLongBits(value) {
    const low = Number(BigInt.asUintN(32, value));
    const high = Number(BigInt.asUintN(32, value >> BigInt(32)));
    return { low, high };
  }
  exports.toLongBits = toLongBits;
  function encodeAsLongBits(hrTime) {
    const nanos = hrTimeToNanos(hrTime);
    return toLongBits(nanos);
  }
  exports.encodeAsLongBits = encodeAsLongBits;
  function encodeAsString(hrTime) {
    const nanos = hrTimeToNanos(hrTime);
    return nanos.toString();
  }
  exports.encodeAsString = encodeAsString;
  var encodeTimestamp = typeof BigInt !== "undefined" ? encodeAsString : core_1.hrTimeToNanoseconds;
  function identity(value) {
    return value;
  }
  function optionalHexToBinary(str) {
    if (str === undefined)
      return;
    return (0, hex_to_binary_1.hexToBinary)(str);
  }
  exports.PROTOBUF_ENCODER = {
    encodeHrTime: encodeAsLongBits,
    encodeSpanContext: hex_to_binary_1.hexToBinary,
    encodeOptionalSpanContext: optionalHexToBinary,
    encodeUint8Array: identity
  };
  exports.JSON_ENCODER = {
    encodeHrTime: encodeTimestamp,
    encodeSpanContext: identity,
    encodeOptionalSpanContext: identity,
    encodeUint8Array: (bytes) => {
      if (typeof Buffer !== "undefined") {
        return Buffer.from(bytes).toString("base64");
      }
      const chars = new Array(bytes.length);
      for (let i = 0;i < bytes.length; i++) {
        chars[i] = String.fromCharCode(bytes[i]);
      }
      return btoa(chars.join(""));
    }
  };
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/logs/json/logs.js
var require_logs3 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.JsonLogsSerializer = undefined;
  var internal_1 = require_internal2();
  var utils_1 = require_utils7();
  var api_1 = require_src();
  exports.JsonLogsSerializer = {
    serializeRequest: (arg) => {
      const request = (0, internal_1.createExportLogsServiceRequest)(arg, utils_1.JSON_ENCODER);
      const encoder = new TextEncoder;
      return encoder.encode(JSON.stringify(request));
    },
    deserializeResponse: (arg) => {
      if (arg.length === 0) {
        return {};
      }
      const decoder = new TextDecoder;
      try {
        return JSON.parse(decoder.decode(arg));
      } catch (err) {
        api_1.diag.warn(`Failed to parse logs export response: ${err.message}. Returning empty response`);
        return {};
      }
    }
  };
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/logs/json/index.js
var require_json = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.JsonLogsSerializer = undefined;
  var logs_1 = require_logs3();
  Object.defineProperty(exports, "JsonLogsSerializer", { enumerable: true, get: function() {
    return logs_1.JsonLogsSerializer;
  } });
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/metrics/internal-types.js
var require_internal_types = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.EAggregationTemporality = undefined;
  var EAggregationTemporality;
  (function(EAggregationTemporality) {
    EAggregationTemporality[EAggregationTemporality["AGGREGATION_TEMPORALITY_UNSPECIFIED"] = 0] = "AGGREGATION_TEMPORALITY_UNSPECIFIED";
    EAggregationTemporality[EAggregationTemporality["AGGREGATION_TEMPORALITY_DELTA"] = 1] = "AGGREGATION_TEMPORALITY_DELTA";
    EAggregationTemporality[EAggregationTemporality["AGGREGATION_TEMPORALITY_CUMULATIVE"] = 2] = "AGGREGATION_TEMPORALITY_CUMULATIVE";
  })(EAggregationTemporality || (exports.EAggregationTemporality = EAggregationTemporality = {}));
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/metrics/internal.js
var require_internal3 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.createExportMetricsServiceRequest = exports.toMetric = exports.toScopeMetrics = exports.toResourceMetrics = undefined;
  var api_1 = require_src();
  var sdk_metrics_1 = require_src8();
  var internal_types_1 = require_internal_types();
  var internal_1 = require_internal();
  function toResourceMetrics(resourceMetrics, encoder) {
    const processedResource = (0, internal_1.createResource)(resourceMetrics.resource, encoder);
    return {
      resource: processedResource,
      schemaUrl: processedResource.schemaUrl,
      scopeMetrics: toScopeMetrics(resourceMetrics.scopeMetrics, encoder)
    };
  }
  exports.toResourceMetrics = toResourceMetrics;
  function toScopeMetrics(scopeMetrics, encoder) {
    return Array.from(scopeMetrics.map((metrics) => ({
      scope: (0, internal_1.createInstrumentationScope)(metrics.scope, encoder),
      metrics: metrics.metrics.map((metricData) => toMetric(metricData, encoder)),
      schemaUrl: metrics.scope.schemaUrl
    })));
  }
  exports.toScopeMetrics = toScopeMetrics;
  function toMetric(metricData, encoder) {
    const out = {
      name: metricData.descriptor.name,
      description: metricData.descriptor.description,
      unit: metricData.descriptor.unit
    };
    const aggregationTemporality = toAggregationTemporality(metricData.aggregationTemporality);
    switch (metricData.dataPointType) {
      case sdk_metrics_1.DataPointType.SUM:
        out.sum = {
          aggregationTemporality,
          isMonotonic: metricData.isMonotonic,
          dataPoints: toSingularDataPoints(metricData, encoder)
        };
        break;
      case sdk_metrics_1.DataPointType.GAUGE:
        out.gauge = {
          dataPoints: toSingularDataPoints(metricData, encoder)
        };
        break;
      case sdk_metrics_1.DataPointType.HISTOGRAM:
        out.histogram = {
          aggregationTemporality,
          dataPoints: toHistogramDataPoints(metricData, encoder)
        };
        break;
      case sdk_metrics_1.DataPointType.EXPONENTIAL_HISTOGRAM:
        out.exponentialHistogram = {
          aggregationTemporality,
          dataPoints: toExponentialHistogramDataPoints(metricData, encoder)
        };
        break;
    }
    return out;
  }
  exports.toMetric = toMetric;
  function toSingularDataPoint(dataPoint, valueType, encoder) {
    const out = {
      attributes: (0, internal_1.toAttributes)(dataPoint.attributes, encoder),
      startTimeUnixNano: encoder.encodeHrTime(dataPoint.startTime),
      timeUnixNano: encoder.encodeHrTime(dataPoint.endTime)
    };
    switch (valueType) {
      case api_1.ValueType.INT:
        out.asInt = dataPoint.value;
        break;
      case api_1.ValueType.DOUBLE:
        out.asDouble = dataPoint.value;
        break;
    }
    return out;
  }
  function toSingularDataPoints(metricData, encoder) {
    return metricData.dataPoints.map((dataPoint) => {
      return toSingularDataPoint(dataPoint, metricData.descriptor.valueType, encoder);
    });
  }
  function toHistogramDataPoints(metricData, encoder) {
    return metricData.dataPoints.map((dataPoint) => {
      const histogram = dataPoint.value;
      return {
        attributes: (0, internal_1.toAttributes)(dataPoint.attributes, encoder),
        bucketCounts: histogram.buckets.counts,
        explicitBounds: histogram.buckets.boundaries,
        count: histogram.count,
        sum: histogram.sum,
        min: histogram.min,
        max: histogram.max,
        startTimeUnixNano: encoder.encodeHrTime(dataPoint.startTime),
        timeUnixNano: encoder.encodeHrTime(dataPoint.endTime)
      };
    });
  }
  function toExponentialHistogramDataPoints(metricData, encoder) {
    return metricData.dataPoints.map((dataPoint) => {
      const histogram = dataPoint.value;
      return {
        attributes: (0, internal_1.toAttributes)(dataPoint.attributes, encoder),
        count: histogram.count,
        min: histogram.min,
        max: histogram.max,
        sum: histogram.sum,
        positive: {
          offset: histogram.positive.offset,
          bucketCounts: histogram.positive.bucketCounts
        },
        negative: {
          offset: histogram.negative.offset,
          bucketCounts: histogram.negative.bucketCounts
        },
        scale: histogram.scale,
        zeroCount: histogram.zeroCount,
        startTimeUnixNano: encoder.encodeHrTime(dataPoint.startTime),
        timeUnixNano: encoder.encodeHrTime(dataPoint.endTime)
      };
    });
  }
  function toAggregationTemporality(temporality) {
    switch (temporality) {
      case sdk_metrics_1.AggregationTemporality.DELTA:
        return internal_types_1.EAggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
      case sdk_metrics_1.AggregationTemporality.CUMULATIVE:
        return internal_types_1.EAggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE;
    }
  }
  function createExportMetricsServiceRequest(resourceMetrics, encoder) {
    return {
      resourceMetrics: resourceMetrics.map((metrics) => toResourceMetrics(metrics, encoder))
    };
  }
  exports.createExportMetricsServiceRequest = createExportMetricsServiceRequest;
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/metrics/json/metrics.js
var require_metrics3 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.JsonMetricsSerializer = undefined;
  var internal_1 = require_internal3();
  var utils_1 = require_utils7();
  var api_1 = require_src();
  exports.JsonMetricsSerializer = {
    serializeRequest: (arg) => {
      const request = (0, internal_1.createExportMetricsServiceRequest)([arg], utils_1.JSON_ENCODER);
      const encoder = new TextEncoder;
      return encoder.encode(JSON.stringify(request));
    },
    deserializeResponse: (arg) => {
      if (arg.length === 0) {
        return {};
      }
      const decoder = new TextDecoder;
      try {
        return JSON.parse(decoder.decode(arg));
      } catch (err) {
        api_1.diag.warn(`Failed to parse metrics export response: ${err.message}. Returning empty response`);
        return {};
      }
    }
  };
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/metrics/json/index.js
var require_json2 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.JsonMetricsSerializer = undefined;
  var metrics_1 = require_metrics3();
  Object.defineProperty(exports, "JsonMetricsSerializer", { enumerable: true, get: function() {
    return metrics_1.JsonMetricsSerializer;
  } });
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/trace/internal.js
var require_internal4 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.createExportTraceServiceRequest = exports.toOtlpSpanEvent = exports.toOtlpLink = exports.sdkSpanToOtlpSpan = undefined;
  var internal_1 = require_internal();
  var SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK = 256;
  var SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK = 512;
  function buildSpanFlagsFrom(traceFlags, isRemote) {
    let flags = traceFlags & 255 | SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK;
    if (isRemote) {
      flags |= SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK;
    }
    return flags;
  }
  function sdkSpanToOtlpSpan(span, encoder) {
    const ctx = span.spanContext();
    const status = span.status;
    const parentSpanId = span.parentSpanContext?.spanId ? encoder.encodeSpanContext(span.parentSpanContext?.spanId) : undefined;
    return {
      traceId: encoder.encodeSpanContext(ctx.traceId),
      spanId: encoder.encodeSpanContext(ctx.spanId),
      parentSpanId,
      traceState: ctx.traceState?.serialize(),
      name: span.name,
      kind: span.kind == null ? 0 : span.kind + 1,
      startTimeUnixNano: encoder.encodeHrTime(span.startTime),
      endTimeUnixNano: encoder.encodeHrTime(span.endTime),
      attributes: (0, internal_1.toAttributes)(span.attributes, encoder),
      droppedAttributesCount: span.droppedAttributesCount,
      events: span.events.map((event) => toOtlpSpanEvent(event, encoder)),
      droppedEventsCount: span.droppedEventsCount,
      status: {
        code: status.code,
        message: status.message
      },
      links: span.links.map((link) => toOtlpLink(link, encoder)),
      droppedLinksCount: span.droppedLinksCount,
      flags: buildSpanFlagsFrom(ctx.traceFlags, span.parentSpanContext?.isRemote)
    };
  }
  exports.sdkSpanToOtlpSpan = sdkSpanToOtlpSpan;
  function toOtlpLink(link, encoder) {
    return {
      attributes: link.attributes ? (0, internal_1.toAttributes)(link.attributes, encoder) : [],
      spanId: encoder.encodeSpanContext(link.context.spanId),
      traceId: encoder.encodeSpanContext(link.context.traceId),
      traceState: link.context.traceState?.serialize(),
      droppedAttributesCount: link.droppedAttributesCount || 0,
      flags: buildSpanFlagsFrom(link.context.traceFlags, link.context.isRemote)
    };
  }
  exports.toOtlpLink = toOtlpLink;
  function toOtlpSpanEvent(timedEvent, encoder) {
    return {
      attributes: timedEvent.attributes ? (0, internal_1.toAttributes)(timedEvent.attributes, encoder) : [],
      name: timedEvent.name,
      timeUnixNano: encoder.encodeHrTime(timedEvent.time),
      droppedAttributesCount: timedEvent.droppedAttributesCount || 0
    };
  }
  exports.toOtlpSpanEvent = toOtlpSpanEvent;
  function createExportTraceServiceRequest(spans, encoder) {
    return {
      resourceSpans: spanRecordsToResourceSpans(spans, encoder)
    };
  }
  exports.createExportTraceServiceRequest = createExportTraceServiceRequest;
  function createResourceMap(readableSpans) {
    const resourceMap = new Map;
    for (const record of readableSpans) {
      let ilsMap = resourceMap.get(record.resource);
      if (!ilsMap) {
        ilsMap = new Map;
        resourceMap.set(record.resource, ilsMap);
      }
      const instrumentationScopeKey = `${record.instrumentationScope.name}@${record.instrumentationScope.version || ""}:${record.instrumentationScope.schemaUrl || ""}`;
      let records = ilsMap.get(instrumentationScopeKey);
      if (!records) {
        records = [];
        ilsMap.set(instrumentationScopeKey, records);
      }
      records.push(record);
    }
    return resourceMap;
  }
  function spanRecordsToResourceSpans(readableSpans, encoder) {
    const resourceMap = createResourceMap(readableSpans);
    const out = [];
    const entryIterator = resourceMap.entries();
    let entry = entryIterator.next();
    while (!entry.done) {
      const [resource, ilmMap] = entry.value;
      const scopeResourceSpans = [];
      const ilmIterator = ilmMap.values();
      let ilmEntry = ilmIterator.next();
      while (!ilmEntry.done) {
        const scopeSpans = ilmEntry.value;
        if (scopeSpans.length > 0) {
          const spans = scopeSpans.map((readableSpan) => sdkSpanToOtlpSpan(readableSpan, encoder));
          scopeResourceSpans.push({
            scope: (0, internal_1.createInstrumentationScope)(scopeSpans[0].instrumentationScope, encoder),
            spans,
            schemaUrl: scopeSpans[0].instrumentationScope.schemaUrl
          });
        }
        ilmEntry = ilmIterator.next();
      }
      const processedResource = (0, internal_1.createResource)(resource, encoder);
      const transformedSpans = {
        resource: processedResource,
        scopeSpans: scopeResourceSpans,
        schemaUrl: processedResource.schemaUrl
      };
      out.push(transformedSpans);
      entry = entryIterator.next();
    }
    return out;
  }
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/trace/json/trace.js
var require_trace4 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.JsonTraceSerializer = undefined;
  var internal_1 = require_internal4();
  var utils_1 = require_utils7();
  var api_1 = require_src();
  exports.JsonTraceSerializer = {
    serializeRequest: (arg) => {
      const request = (0, internal_1.createExportTraceServiceRequest)(arg, utils_1.JSON_ENCODER);
      const encoder = new TextEncoder;
      return encoder.encode(JSON.stringify(request));
    },
    deserializeResponse: (arg) => {
      if (arg.length === 0) {
        return {};
      }
      const decoder = new TextDecoder;
      try {
        return JSON.parse(decoder.decode(arg));
      } catch (err) {
        api_1.diag.warn(`Failed to parse trace export response: ${err.message}. Returning empty response`);
        return {};
      }
    }
  };
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/trace/json/index.js
var require_json3 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.JsonTraceSerializer = undefined;
  var trace_1 = require_trace4();
  Object.defineProperty(exports, "JsonTraceSerializer", { enumerable: true, get: function() {
    return trace_1.JsonTraceSerializer;
  } });
});

// node_modules/.bun/@opentelemetry+otlp-transformer@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-transformer/build/src/index.js
var require_src9 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.JsonTraceSerializer = exports.JsonMetricsSerializer = exports.JsonLogsSerializer = exports.ProtobufTraceSerializer = exports.ProtobufMetricsSerializer = exports.ProtobufLogsSerializer = exports.LogsExporterMetricsHelper = exports.TraceExporterMetricsHelper = exports.MetricsExporterMetricsHelper = undefined;
  var metrics_1 = require_metrics();
  Object.defineProperty(exports, "MetricsExporterMetricsHelper", { enumerable: true, get: function() {
    return metrics_1.MetricsExporterMetricsHelper;
  } });
  var trace_1 = require_trace2();
  Object.defineProperty(exports, "TraceExporterMetricsHelper", { enumerable: true, get: function() {
    return trace_1.TraceExporterMetricsHelper;
  } });
  var logs_1 = require_logs();
  Object.defineProperty(exports, "LogsExporterMetricsHelper", { enumerable: true, get: function() {
    return logs_1.LogsExporterMetricsHelper;
  } });
  var protobuf_1 = require_protobuf();
  Object.defineProperty(exports, "ProtobufLogsSerializer", { enumerable: true, get: function() {
    return protobuf_1.ProtobufLogsSerializer;
  } });
  var protobuf_2 = require_protobuf2();
  Object.defineProperty(exports, "ProtobufMetricsSerializer", { enumerable: true, get: function() {
    return protobuf_2.ProtobufMetricsSerializer;
  } });
  var protobuf_3 = require_protobuf3();
  Object.defineProperty(exports, "ProtobufTraceSerializer", { enumerable: true, get: function() {
    return protobuf_3.ProtobufTraceSerializer;
  } });
  var json_1 = require_json();
  Object.defineProperty(exports, "JsonLogsSerializer", { enumerable: true, get: function() {
    return json_1.JsonLogsSerializer;
  } });
  var json_2 = require_json2();
  Object.defineProperty(exports, "JsonMetricsSerializer", { enumerable: true, get: function() {
    return json_2.JsonMetricsSerializer;
  } });
  var json_3 = require_json3();
  Object.defineProperty(exports, "JsonTraceSerializer", { enumerable: true, get: function() {
    return json_3.JsonTraceSerializer;
  } });
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/util.js
var require_util2 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.validateAndNormalizeHeaders = undefined;
  var api_1 = require_src();
  function validateAndNormalizeHeaders(partialHeaders) {
    const headers = {};
    Object.entries(partialHeaders ?? {}).forEach(([key, value]) => {
      if (typeof value !== "undefined") {
        headers[key] = String(value);
      } else {
        api_1.diag.warn(`Header "${key}" has invalid value (${value}) and will be ignored`);
      }
    });
    return headers;
  }
  exports.validateAndNormalizeHeaders = validateAndNormalizeHeaders;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/configuration/otlp-http-configuration.js
var require_otlp_http_configuration = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.getHttpConfigurationDefaults = exports.mergeOtlpHttpConfigurationWithDefaults = undefined;
  var shared_configuration_1 = require_shared_configuration();
  var util_1 = require_util2();
  function mergeHeaders(userProvidedHeaders, fallbackHeaders, defaultHeaders) {
    return async () => {
      const requiredHeaders = {
        ...await defaultHeaders()
      };
      const headers = {};
      if (fallbackHeaders != null) {
        Object.assign(headers, await fallbackHeaders());
      }
      if (userProvidedHeaders != null) {
        Object.assign(headers, (0, util_1.validateAndNormalizeHeaders)(await userProvidedHeaders()));
      }
      return Object.assign(headers, requiredHeaders);
    };
  }
  function validateUserProvidedUrl(url) {
    if (url == null) {
      return;
    }
    try {
      const base = globalThis.location?.href;
      return new URL(url, base).href;
    } catch {
      throw new Error(`Configuration: Could not parse user-provided export URL: '${url}'`);
    }
  }
  function mergeOtlpHttpConfigurationWithDefaults(userProvidedConfiguration, fallbackConfiguration, defaultConfiguration) {
    return {
      ...(0, shared_configuration_1.mergeOtlpSharedConfigurationWithDefaults)(userProvidedConfiguration, fallbackConfiguration, defaultConfiguration),
      headers: mergeHeaders(userProvidedConfiguration.headers, fallbackConfiguration.headers, defaultConfiguration.headers),
      url: validateUserProvidedUrl(userProvidedConfiguration.url) ?? fallbackConfiguration.url ?? defaultConfiguration.url
    };
  }
  exports.mergeOtlpHttpConfigurationWithDefaults = mergeOtlpHttpConfigurationWithDefaults;
  function getHttpConfigurationDefaults(requiredHeaders, signalResourcePath) {
    return {
      ...(0, shared_configuration_1.getSharedConfigurationDefaults)(),
      headers: async () => requiredHeaders,
      url: "http://localhost:4318/" + signalResourcePath
    };
  }
  exports.getHttpConfigurationDefaults = getHttpConfigurationDefaults;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/configuration/otlp-node-http-configuration.js
var require_otlp_node_http_configuration = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.getNodeHttpConfigurationDefaults = exports.mergeOtlpNodeHttpConfigurationWithDefaults = exports.httpAgentFactoryFromOptions = undefined;
  var otlp_http_configuration_1 = require_otlp_http_configuration();
  function httpAgentFactoryFromOptions(options) {
    return async (protocol) => {
      const isInsecure = protocol === "http:";
      const module2 = isInsecure ? import("http") : import("https");
      const { Agent } = await module2;
      if (isInsecure) {
        const { ca, cert, key, ...insecureOptions } = options;
        return new Agent(insecureOptions);
      }
      return new Agent(options);
    };
  }
  exports.httpAgentFactoryFromOptions = httpAgentFactoryFromOptions;
  function mergeOtlpNodeHttpConfigurationWithDefaults(userProvidedConfiguration, fallbackConfiguration, defaultConfiguration) {
    return {
      ...(0, otlp_http_configuration_1.mergeOtlpHttpConfigurationWithDefaults)(userProvidedConfiguration, fallbackConfiguration, defaultConfiguration),
      agentFactory: userProvidedConfiguration.agentFactory ?? fallbackConfiguration.agentFactory ?? defaultConfiguration.agentFactory,
      userAgent: userProvidedConfiguration.userAgent
    };
  }
  exports.mergeOtlpNodeHttpConfigurationWithDefaults = mergeOtlpNodeHttpConfigurationWithDefaults;
  function getNodeHttpConfigurationDefaults(requiredHeaders, signalResourcePath) {
    return {
      ...(0, otlp_http_configuration_1.getHttpConfigurationDefaults)(requiredHeaders, signalResourcePath),
      agentFactory: httpAgentFactoryFromOptions({ keepAlive: true })
    };
  }
  exports.getNodeHttpConfigurationDefaults = getNodeHttpConfigurationDefaults;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/is-export-retryable.js
var require_is_export_retryable = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.parseRetryAfterToMills = exports.isExportHTTPErrorRetryable = undefined;
  function isExportHTTPErrorRetryable(statusCode) {
    return statusCode === 429 || statusCode === 502 || statusCode === 503 || statusCode === 504;
  }
  exports.isExportHTTPErrorRetryable = isExportHTTPErrorRetryable;
  function parseRetryAfterToMills(retryAfter) {
    if (retryAfter == null) {
      return;
    }
    const seconds = Number.parseInt(retryAfter, 10);
    if (Number.isInteger(seconds)) {
      return seconds > 0 ? seconds * 1000 : -1;
    }
    const delay = new Date(retryAfter).getTime() - Date.now();
    if (delay >= 0) {
      return delay;
    }
    return 0;
  }
  exports.parseRetryAfterToMills = parseRetryAfterToMills;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/transport/http-transport-utils.js
var require_http_transport_utils = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.compressAndSend = exports.sendWithHttp = exports.MAX_RESPONSE_BODY_SIZE = undefined;
  var zlib = __require("zlib");
  var stream_1 = __require("stream");
  var is_export_retryable_1 = require_is_export_retryable();
  var types_1 = require_types();
  var version_1 = require_version2();
  var DEFAULT_USER_AGENT = `OTel-OTLP-Exporter-JavaScript/${version_1.VERSION}`;
  exports.MAX_RESPONSE_BODY_SIZE = 4 * 1024 * 1024;
  function sendWithHttp(request, url, headers, compression, userAgent, agent, data, timeoutMillis) {
    return new Promise((resolve) => {
      const parsedUrl = new URL(url);
      if (userAgent) {
        headers["User-Agent"] = `${userAgent} ${DEFAULT_USER_AGENT}`;
      } else {
        headers["User-Agent"] = DEFAULT_USER_AGENT;
      }
      const options = {
        method: "POST",
        headers,
        agent
      };
      const req = request(parsedUrl, options, (res) => {
        const responseData = [];
        let responseSize = 0;
        res.on("data", (chunk) => {
          responseSize += chunk.length;
          if (responseSize > exports.MAX_RESPONSE_BODY_SIZE) {
            const sizeError = new Error(`OTLP export response body exceeded size limit of ${exports.MAX_RESPONSE_BODY_SIZE} bytes`);
            resolve({ status: "failure", error: sizeError });
            res.destroy();
            return;
          }
          responseData.push(chunk);
        });
        res.on("end", () => {
          if (res.statusCode && res.statusCode <= 299) {
            resolve({
              status: "success",
              data: Buffer.concat(responseData)
            });
          } else if (res.statusCode && (0, is_export_retryable_1.isExportHTTPErrorRetryable)(res.statusCode)) {
            resolve({
              status: "retryable",
              retryInMillis: (0, is_export_retryable_1.parseRetryAfterToMills)(res.headers["retry-after"])
            });
          } else {
            const error = new types_1.OTLPExporterError(res.statusMessage, res.statusCode, Buffer.concat(responseData).toString());
            resolve({
              status: "failure",
              error
            });
          }
        });
        res.on("error", (error) => {
          if (res.statusCode && res.statusCode <= 299) {
            resolve({
              status: "success"
            });
          } else if (res.statusCode && (0, is_export_retryable_1.isExportHTTPErrorRetryable)(res.statusCode)) {
            resolve({
              status: "retryable",
              error,
              retryInMillis: (0, is_export_retryable_1.parseRetryAfterToMills)(res.headers["retry-after"])
            });
          } else {
            resolve({
              status: "failure",
              error
            });
          }
        });
      });
      req.setTimeout(timeoutMillis, () => {
        req.destroy();
        resolve({
          status: "retryable",
          error: new Error("Request timed out")
        });
      });
      req.on("error", (error) => {
        if (isHttpTransportNetworkErrorRetryable(error)) {
          resolve({
            status: "retryable",
            error
          });
        } else {
          resolve({
            status: "failure",
            error
          });
        }
      });
      compressAndSend(req, compression, data, (error) => {
        resolve({
          status: "failure",
          error
        });
      });
    });
  }
  exports.sendWithHttp = sendWithHttp;
  function compressAndSend(req, compression, data, onError) {
    let dataStream = readableFromUint8Array(data);
    if (compression === "gzip") {
      req.setHeader("Content-Encoding", "gzip");
      dataStream = dataStream.on("error", onError).pipe(zlib.createGzip()).on("error", onError);
    }
    dataStream.pipe(req).on("error", onError);
  }
  exports.compressAndSend = compressAndSend;
  function readableFromUint8Array(buff) {
    const readable = new stream_1.Readable;
    readable.push(buff);
    readable.push(null);
    return readable;
  }
  function isHttpTransportNetworkErrorRetryable(error) {
    const RETRYABLE_NETWORK_ERROR_CODES = new Set([
      "ECONNRESET",
      "ECONNREFUSED",
      "EPIPE",
      "ETIMEDOUT",
      "EAI_AGAIN",
      "ENOTFOUND",
      "ENETUNREACH",
      "EHOSTUNREACH"
    ]);
    if ("code" in error && typeof error.code === "string") {
      return RETRYABLE_NETWORK_ERROR_CODES.has(error.code);
    }
    return false;
  }
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/transport/http-exporter-transport.js
var require_http_exporter_transport = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.createHttpExporterTransport = undefined;
  var http_transport_utils_1 = require_http_transport_utils();

  class HttpExporterTransport {
    _utils = null;
    _parameters;
    constructor(parameters) {
      this._parameters = parameters;
    }
    async send(data, timeoutMillis) {
      const { agent, request } = await this._loadUtils();
      const headers = await this._parameters.headers();
      return (0, http_transport_utils_1.sendWithHttp)(request, this._parameters.url, headers, this._parameters.compression, this._parameters.userAgent, agent, data, timeoutMillis);
    }
    shutdown() {}
    async _loadUtils() {
      let utils = this._utils;
      if (utils === null) {
        const protocol = new URL(this._parameters.url).protocol;
        const [agent, request] = await Promise.all([
          this._parameters.agentFactory(protocol),
          requestFunctionFactory(protocol)
        ]);
        utils = this._utils = { agent, request };
      }
      return utils;
    }
  }
  async function requestFunctionFactory(protocol) {
    const module2 = protocol === "http:" ? import("http") : import("https");
    const { request } = await module2;
    return request;
  }
  function createHttpExporterTransport(parameters) {
    return new HttpExporterTransport(parameters);
  }
  exports.createHttpExporterTransport = createHttpExporterTransport;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/retrying-transport.js
var require_retrying_transport = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.createRetryingTransport = undefined;
  var api_1 = require_src();
  var MAX_ATTEMPTS = 5;
  var INITIAL_BACKOFF = 1000;
  var MAX_BACKOFF = 5000;
  var BACKOFF_MULTIPLIER = 1.5;
  var JITTER = 0.2;
  function getJitter() {
    return Math.random() * (2 * JITTER) - JITTER;
  }

  class RetryingTransport {
    _transport;
    constructor(transport) {
      this._transport = transport;
    }
    retry(data, timeoutMillis, inMillis) {
      return new Promise((resolve, reject) => {
        setTimeout(() => {
          this._transport.send(data, timeoutMillis).then(resolve, reject);
        }, inMillis);
      });
    }
    async send(data, timeoutMillis) {
      let attempts = MAX_ATTEMPTS;
      let nextBackoff = INITIAL_BACKOFF;
      const deadline = Date.now() + timeoutMillis;
      let result = await this._transport.send(data, timeoutMillis);
      while (result.status === "retryable" && attempts > 0) {
        attempts--;
        const backoff = Math.max(Math.min(nextBackoff * (1 + getJitter()), MAX_BACKOFF), 0);
        nextBackoff = nextBackoff * BACKOFF_MULTIPLIER;
        const retryInMillis = result.retryInMillis ?? backoff;
        const remainingTimeoutMillis = deadline - Date.now();
        if (retryInMillis > remainingTimeoutMillis) {
          api_1.diag.info(`Export retry time ${Math.round(retryInMillis)}ms exceeds remaining timeout ${Math.round(remainingTimeoutMillis)}ms, not retrying further.`);
          return result;
        }
        api_1.diag.verbose(`Scheduling export retry in ${Math.round(retryInMillis)}ms`);
        result = await this.retry(data, remainingTimeoutMillis, retryInMillis);
      }
      if (result.status === "success") {
        api_1.diag.verbose(`Export succeeded after ${MAX_ATTEMPTS - attempts} retry attempts.`);
      } else if (result.status === "retryable") {
        api_1.diag.info(`Export failed after maximum retry attempts (${MAX_ATTEMPTS}).`);
      } else {
        api_1.diag.info(`Export failed with non-retryable error: ${result.error}`);
      }
      return result;
    }
    shutdown() {
      return this._transport.shutdown();
    }
  }
  function createRetryingTransport(options) {
    return new RetryingTransport(options.transport);
  }
  exports.createRetryingTransport = createRetryingTransport;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/otlp-http-export-delegate.js
var require_otlp_http_export_delegate = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.createOtlpHttpExportDelegate = exports.createOtlpHttpExporterMetrics = undefined;
  var otlp_export_delegate_1 = require_otlp_export_delegate();
  var http_exporter_transport_1 = require_http_exporter_transport();
  var bounded_queue_export_promise_handler_1 = require_bounded_queue_export_promise_handler();
  var retrying_transport_1 = require_retrying_transport();
  var types_1 = require_types();
  var semconv_1 = require_semconv2();
  var ExporterMetrics_1 = require_ExporterMetrics();
  function createOtlpHttpExporterMetrics(metricsComponentType, exporterMetricsHelper, url, meterProvider) {
    return new ExporterMetrics_1.ExporterMetrics({
      componentType: metricsComponentType,
      metricsHelper: exporterMetricsHelper,
      url,
      meterProvider,
      responseAttributesFromError: (error) => {
        if (!error) {
          return {
            [semconv_1.ATTR_HTTP_RESPONSE_STATUS_CODE]: 200
          };
        }
        if (!(error instanceof types_1.OTLPExporterError)) {
          return {};
        }
        return {
          [semconv_1.ATTR_HTTP_RESPONSE_STATUS_CODE]: error.code
        };
      }
    });
  }
  exports.createOtlpHttpExporterMetrics = createOtlpHttpExporterMetrics;
  function createOtlpHttpExportDelegate(options, serializer, metricsComponentType, exporterMetricsHelper, meterProvider) {
    return (0, otlp_export_delegate_1.createOtlpExportDelegate)({
      transport: (0, retrying_transport_1.createRetryingTransport)({
        transport: (0, http_exporter_transport_1.createHttpExporterTransport)(options)
      }),
      serializer,
      promiseHandler: (0, bounded_queue_export_promise_handler_1.createBoundedQueueExportPromiseHandler)(options),
      metrics: createOtlpHttpExporterMetrics(metricsComponentType, exporterMetricsHelper, options.url, meterProvider)
    }, { timeout: options.timeoutMillis });
  }
  exports.createOtlpHttpExportDelegate = createOtlpHttpExportDelegate;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/configuration/shared-env-configuration.js
var require_shared_env_configuration = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.getSharedConfigurationFromEnvironment = undefined;
  var core_1 = require_src5();
  var api_1 = require_src();
  function parseAndValidateTimeoutFromEnv(timeoutEnvVar) {
    const envTimeout = (0, core_1.getNumberFromEnv)(timeoutEnvVar);
    if (envTimeout != null) {
      if (Number.isFinite(envTimeout) && envTimeout > 0) {
        return envTimeout;
      }
      api_1.diag.warn(`Configuration: ${timeoutEnvVar} is invalid, expected number greater than 0 (actual: ${envTimeout})`);
    }
    return;
  }
  function getTimeoutFromEnv(signalIdentifier) {
    const specificTimeout = parseAndValidateTimeoutFromEnv(`OTEL_EXPORTER_OTLP_${signalIdentifier}_TIMEOUT`);
    const nonSpecificTimeout = parseAndValidateTimeoutFromEnv("OTEL_EXPORTER_OTLP_TIMEOUT");
    return specificTimeout ?? nonSpecificTimeout;
  }
  function parseAndValidateCompressionFromEnv(compressionEnvVar) {
    const compression = (0, core_1.getStringFromEnv)(compressionEnvVar)?.trim();
    if (compression == null || compression === "none" || compression === "gzip") {
      return compression;
    }
    api_1.diag.warn(`Configuration: ${compressionEnvVar} is invalid, expected 'none' or 'gzip' (actual: '${compression}')`);
    return;
  }
  function getCompressionFromEnv(signalIdentifier) {
    const specificCompression = parseAndValidateCompressionFromEnv(`OTEL_EXPORTER_OTLP_${signalIdentifier}_COMPRESSION`);
    const nonSpecificCompression = parseAndValidateCompressionFromEnv("OTEL_EXPORTER_OTLP_COMPRESSION");
    return specificCompression ?? nonSpecificCompression;
  }
  function getSharedConfigurationFromEnvironment(signalIdentifier) {
    return {
      timeoutMillis: getTimeoutFromEnv(signalIdentifier),
      compression: getCompressionFromEnv(signalIdentifier)
    };
  }
  exports.getSharedConfigurationFromEnvironment = getSharedConfigurationFromEnvironment;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/configuration/otlp-node-http-env-configuration.js
var require_otlp_node_http_env_configuration = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.getNodeHttpConfigurationFromEnvironment = undefined;
  var fs = __require("fs");
  var path = __require("path");
  var core_1 = require_src5();
  var api_1 = require_src();
  var shared_env_configuration_1 = require_shared_env_configuration();
  var shared_configuration_1 = require_shared_configuration();
  var otlp_node_http_configuration_1 = require_otlp_node_http_configuration();
  function getStaticHeadersFromEnv(signalIdentifier) {
    const signalSpecificRawHeaders = (0, core_1.getStringFromEnv)(`OTEL_EXPORTER_OTLP_${signalIdentifier}_HEADERS`);
    const nonSignalSpecificRawHeaders = (0, core_1.getStringFromEnv)("OTEL_EXPORTER_OTLP_HEADERS");
    const signalSpecificHeaders = (0, core_1.parseKeyPairsIntoRecord)(signalSpecificRawHeaders);
    const nonSignalSpecificHeaders = (0, core_1.parseKeyPairsIntoRecord)(nonSignalSpecificRawHeaders);
    if (Object.keys(signalSpecificHeaders).length === 0 && Object.keys(nonSignalSpecificHeaders).length === 0) {
      return;
    }
    return Object.assign({}, (0, core_1.parseKeyPairsIntoRecord)(nonSignalSpecificRawHeaders), (0, core_1.parseKeyPairsIntoRecord)(signalSpecificRawHeaders));
  }
  function appendRootPathToUrlIfNeeded(url) {
    try {
      const parsedUrl = new URL(url);
      return parsedUrl.toString();
    } catch {
      api_1.diag.warn(`Configuration: Could not parse environment-provided export URL: '${url}', falling back to undefined`);
      return;
    }
  }
  function appendResourcePathToUrl(url, path) {
    try {
      new URL(url);
    } catch {
      api_1.diag.warn(`Configuration: Could not parse environment-provided export URL: '${url}', falling back to undefined`);
      return;
    }
    if (!url.endsWith("/")) {
      url = url + "/";
    }
    url += path;
    try {
      new URL(url);
    } catch {
      api_1.diag.warn(`Configuration: Provided URL appended with '${path}' is not a valid URL, using 'undefined' instead of '${url}'`);
      return;
    }
    return url;
  }
  function getNonSpecificUrlFromEnv(signalResourcePath) {
    const envUrl = (0, core_1.getStringFromEnv)("OTEL_EXPORTER_OTLP_ENDPOINT");
    if (envUrl === undefined) {
      return;
    }
    return appendResourcePathToUrl(envUrl, signalResourcePath);
  }
  function getSpecificUrlFromEnv(signalIdentifier) {
    const envUrl = (0, core_1.getStringFromEnv)(`OTEL_EXPORTER_OTLP_${signalIdentifier}_ENDPOINT`);
    if (envUrl === undefined) {
      return;
    }
    return appendRootPathToUrlIfNeeded(envUrl);
  }
  function readFileFromEnv(signalSpecificEnvVar, nonSignalSpecificEnvVar, warningMessage) {
    const signalSpecificPath = (0, core_1.getStringFromEnv)(signalSpecificEnvVar);
    const nonSignalSpecificPath = (0, core_1.getStringFromEnv)(nonSignalSpecificEnvVar);
    const filePath = signalSpecificPath ?? nonSignalSpecificPath;
    if (filePath != null) {
      try {
        return fs.readFileSync(path.resolve(process.cwd(), filePath));
      } catch {
        api_1.diag.warn(warningMessage);
        return;
      }
    } else {
      return;
    }
  }
  function getClientCertificateFromEnv(signalIdentifier) {
    return readFileFromEnv(`OTEL_EXPORTER_OTLP_${signalIdentifier}_CLIENT_CERTIFICATE`, "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", "Failed to read client certificate chain file");
  }
  function getClientKeyFromEnv(signalIdentifier) {
    return readFileFromEnv(`OTEL_EXPORTER_OTLP_${signalIdentifier}_CLIENT_KEY`, "OTEL_EXPORTER_OTLP_CLIENT_KEY", "Failed to read client certificate private key file");
  }
  function getRootCertificateFromEnv(signalIdentifier) {
    return readFileFromEnv(`OTEL_EXPORTER_OTLP_${signalIdentifier}_CERTIFICATE`, "OTEL_EXPORTER_OTLP_CERTIFICATE", "Failed to read root certificate file");
  }
  function getNodeHttpConfigurationFromEnvironment(signalIdentifier, signalResourcePath) {
    return {
      ...(0, shared_env_configuration_1.getSharedConfigurationFromEnvironment)(signalIdentifier),
      url: getSpecificUrlFromEnv(signalIdentifier) ?? getNonSpecificUrlFromEnv(signalResourcePath),
      headers: (0, shared_configuration_1.wrapStaticHeadersInFunction)(getStaticHeadersFromEnv(signalIdentifier)),
      agentFactory: (0, otlp_node_http_configuration_1.httpAgentFactoryFromOptions)({
        keepAlive: true,
        ca: getRootCertificateFromEnv(signalIdentifier),
        cert: getClientCertificateFromEnv(signalIdentifier),
        key: getClientKeyFromEnv(signalIdentifier)
      })
    };
  }
  exports.getNodeHttpConfigurationFromEnvironment = getNodeHttpConfigurationFromEnvironment;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/configuration/convert-legacy-http-options.js
var require_convert_legacy_http_options = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.convertLegacyHeaders = undefined;
  var shared_configuration_1 = require_shared_configuration();
  function convertLegacyHeaders(config) {
    if (typeof config.headers === "function") {
      return config.headers;
    }
    return (0, shared_configuration_1.wrapStaticHeadersInFunction)(config.headers);
  }
  exports.convertLegacyHeaders = convertLegacyHeaders;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/configuration/convert-legacy-node-http-options.js
var require_convert_legacy_node_http_options = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.convertLegacyHttpOptions = undefined;
  var api_1 = require_src();
  var otlp_node_http_configuration_1 = require_otlp_node_http_configuration();
  var index_node_http_1 = require_index_node_http();
  var otlp_node_http_env_configuration_1 = require_otlp_node_http_env_configuration();
  var convert_legacy_http_options_1 = require_convert_legacy_http_options();
  function convertLegacyAgentOptions(config) {
    if (typeof config.httpAgentOptions === "function") {
      return config.httpAgentOptions;
    }
    let legacy = config.httpAgentOptions;
    if (config.keepAlive != null) {
      legacy = { keepAlive: config.keepAlive, ...legacy };
    }
    if (legacy != null) {
      return (0, index_node_http_1.httpAgentFactoryFromOptions)(legacy);
    } else {
      return;
    }
  }
  function convertLegacyHttpOptions(config, signalIdentifier, signalResourcePath, requiredHeaders) {
    if (config.metadata) {
      api_1.diag.warn("Metadata cannot be set when using http");
    }
    return (0, otlp_node_http_configuration_1.mergeOtlpNodeHttpConfigurationWithDefaults)({
      url: config.url,
      headers: (0, convert_legacy_http_options_1.convertLegacyHeaders)(config),
      concurrencyLimit: config.concurrencyLimit,
      timeoutMillis: config.timeoutMillis,
      compression: config.compression,
      agentFactory: convertLegacyAgentOptions(config),
      userAgent: config.userAgent
    }, (0, otlp_node_http_env_configuration_1.getNodeHttpConfigurationFromEnvironment)(signalIdentifier, signalResourcePath), (0, otlp_node_http_configuration_1.getNodeHttpConfigurationDefaults)(requiredHeaders, signalResourcePath));
  }
  exports.convertLegacyHttpOptions = convertLegacyHttpOptions;
});

// node_modules/.bun/@opentelemetry+otlp-exporter-base@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/otlp-exporter-base/build/src/index-node-http.js
var require_index_node_http = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.convertLegacyHttpOptions = exports.getSharedConfigurationFromEnvironment = exports.createOtlpHttpExporterMetrics = exports.createOtlpHttpExportDelegate = exports.httpAgentFactoryFromOptions = undefined;
  var otlp_node_http_configuration_1 = require_otlp_node_http_configuration();
  Object.defineProperty(exports, "httpAgentFactoryFromOptions", { enumerable: true, get: function() {
    return otlp_node_http_configuration_1.httpAgentFactoryFromOptions;
  } });
  var otlp_http_export_delegate_1 = require_otlp_http_export_delegate();
  Object.defineProperty(exports, "createOtlpHttpExportDelegate", { enumerable: true, get: function() {
    return otlp_http_export_delegate_1.createOtlpHttpExportDelegate;
  } });
  Object.defineProperty(exports, "createOtlpHttpExporterMetrics", { enumerable: true, get: function() {
    return otlp_http_export_delegate_1.createOtlpHttpExporterMetrics;
  } });
  var shared_env_configuration_1 = require_shared_env_configuration();
  Object.defineProperty(exports, "getSharedConfigurationFromEnvironment", { enumerable: true, get: function() {
    return shared_env_configuration_1.getSharedConfigurationFromEnvironment;
  } });
  var convert_legacy_node_http_options_1 = require_convert_legacy_node_http_options();
  Object.defineProperty(exports, "convertLegacyHttpOptions", { enumerable: true, get: function() {
    return convert_legacy_node_http_options_1.convertLegacyHttpOptions;
  } });
});

// node_modules/.bun/@opentelemetry+exporter-logs-otlp-http@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-logs-otlp-http/build/src/semconv.js
var require_semconv5 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_LOG_EXPORTER = undefined;
  exports.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_LOG_EXPORTER = "otlp_http_log_exporter";
});

// node_modules/.bun/@opentelemetry+exporter-logs-otlp-http@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-logs-otlp-http/build/src/platform/node/OTLPLogExporter.js
var require_OTLPLogExporter = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPLogExporter = undefined;
  var otlp_exporter_base_1 = require_src6();
  var otlp_transformer_1 = require_src9();
  var node_http_1 = require_index_node_http();
  var semconv_1 = require_semconv5();

  class OTLPLogExporter extends otlp_exporter_base_1.OTLPExporterBase {
    constructor(config = {}) {
      super((0, node_http_1.createOtlpHttpExportDelegate)((0, node_http_1.convertLegacyHttpOptions)(config, "LOGS", "v1/logs", {
        "Content-Type": "application/json"
      }), otlp_transformer_1.JsonLogsSerializer, semconv_1.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_LOG_EXPORTER, otlp_transformer_1.LogsExporterMetricsHelper, config.selfObsMeterProvider));
    }
  }
  exports.OTLPLogExporter = OTLPLogExporter;
});

// node_modules/.bun/@opentelemetry+exporter-logs-otlp-http@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-logs-otlp-http/build/src/platform/node/index.js
var require_node3 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPLogExporter = undefined;
  var OTLPLogExporter_1 = require_OTLPLogExporter();
  Object.defineProperty(exports, "OTLPLogExporter", { enumerable: true, get: function() {
    return OTLPLogExporter_1.OTLPLogExporter;
  } });
});

// node_modules/.bun/@opentelemetry+exporter-logs-otlp-http@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-logs-otlp-http/build/src/platform/index.js
var require_platform3 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPLogExporter = undefined;
  var node_1 = require_node3();
  Object.defineProperty(exports, "OTLPLogExporter", { enumerable: true, get: function() {
    return node_1.OTLPLogExporter;
  } });
});

// node_modules/.bun/@opentelemetry+exporter-logs-otlp-http@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-logs-otlp-http/build/src/index.js
var require_src10 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPLogExporter = undefined;
  var platform_1 = require_platform3();
  Object.defineProperty(exports, "OTLPLogExporter", { enumerable: true, get: function() {
    return platform_1.OTLPLogExporter;
  } });
});

// node_modules/.bun/@opentelemetry+exporter-logs-otlp-proto@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-logs-otlp-proto/build/src/semconv.js
var require_semconv6 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_LOG_EXPORTER = undefined;
  exports.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_LOG_EXPORTER = "otlp_http_log_exporter";
});

// node_modules/.bun/@opentelemetry+exporter-logs-otlp-proto@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-logs-otlp-proto/build/src/platform/node/OTLPLogExporter.js
var require_OTLPLogExporter2 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPLogExporter = undefined;
  var otlp_exporter_base_1 = require_src6();
  var otlp_transformer_1 = require_src9();
  var node_http_1 = require_index_node_http();
  var semconv_1 = require_semconv6();

  class OTLPLogExporter extends otlp_exporter_base_1.OTLPExporterBase {
    constructor(config = {}) {
      super((0, node_http_1.createOtlpHttpExportDelegate)((0, node_http_1.convertLegacyHttpOptions)(config, "LOGS", "v1/logs", {
        "Content-Type": "application/x-protobuf"
      }), otlp_transformer_1.ProtobufLogsSerializer, semconv_1.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_LOG_EXPORTER, otlp_transformer_1.LogsExporterMetricsHelper, config.selfObsMeterProvider));
    }
  }
  exports.OTLPLogExporter = OTLPLogExporter;
});

// node_modules/.bun/@opentelemetry+exporter-logs-otlp-proto@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-logs-otlp-proto/build/src/platform/node/index.js
var require_node4 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPLogExporter = undefined;
  var OTLPLogExporter_1 = require_OTLPLogExporter2();
  Object.defineProperty(exports, "OTLPLogExporter", { enumerable: true, get: function() {
    return OTLPLogExporter_1.OTLPLogExporter;
  } });
});

// node_modules/.bun/@opentelemetry+exporter-logs-otlp-proto@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-logs-otlp-proto/build/src/platform/index.js
var require_platform4 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPLogExporter = undefined;
  var node_1 = require_node4();
  Object.defineProperty(exports, "OTLPLogExporter", { enumerable: true, get: function() {
    return node_1.OTLPLogExporter;
  } });
});

// node_modules/.bun/@opentelemetry+exporter-logs-otlp-proto@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-logs-otlp-proto/build/src/index.js
var require_src11 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPLogExporter = undefined;
  var platform_1 = require_platform4();
  Object.defineProperty(exports, "OTLPLogExporter", { enumerable: true, get: function() {
    return platform_1.OTLPLogExporter;
  } });
});

// node_modules/.bun/@opentelemetry+exporter-metrics-otlp-http@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-metrics-otlp-http/build/src/OTLPMetricExporterOptions.js
var require_OTLPMetricExporterOptions = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.AggregationTemporalityPreference = undefined;
  var AggregationTemporalityPreference;
  (function(AggregationTemporalityPreference) {
    AggregationTemporalityPreference[AggregationTemporalityPreference["DELTA"] = 0] = "DELTA";
    AggregationTemporalityPreference[AggregationTemporalityPreference["CUMULATIVE"] = 1] = "CUMULATIVE";
    AggregationTemporalityPreference[AggregationTemporalityPreference["LOWMEMORY"] = 2] = "LOWMEMORY";
  })(AggregationTemporalityPreference || (exports.AggregationTemporalityPreference = AggregationTemporalityPreference = {}));
});

// node_modules/.bun/@opentelemetry+exporter-metrics-otlp-http@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-metrics-otlp-http/build/src/OTLPMetricExporterBase.js
var require_OTLPMetricExporterBase = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPMetricExporterBase = exports.LowMemoryTemporalitySelector = exports.DeltaTemporalitySelector = exports.CumulativeTemporalitySelector = undefined;
  var core_1 = require_src5();
  var sdk_metrics_1 = require_src8();
  var OTLPMetricExporterOptions_1 = require_OTLPMetricExporterOptions();
  var otlp_exporter_base_1 = require_src6();
  var api_1 = require_src();
  var CumulativeTemporalitySelector = () => sdk_metrics_1.AggregationTemporality.CUMULATIVE;
  exports.CumulativeTemporalitySelector = CumulativeTemporalitySelector;
  var DeltaTemporalitySelector = (instrumentType) => {
    switch (instrumentType) {
      case sdk_metrics_1.InstrumentType.COUNTER:
      case sdk_metrics_1.InstrumentType.OBSERVABLE_COUNTER:
      case sdk_metrics_1.InstrumentType.GAUGE:
      case sdk_metrics_1.InstrumentType.HISTOGRAM:
      case sdk_metrics_1.InstrumentType.OBSERVABLE_GAUGE:
        return sdk_metrics_1.AggregationTemporality.DELTA;
      case sdk_metrics_1.InstrumentType.UP_DOWN_COUNTER:
      case sdk_metrics_1.InstrumentType.OBSERVABLE_UP_DOWN_COUNTER:
        return sdk_metrics_1.AggregationTemporality.CUMULATIVE;
    }
  };
  exports.DeltaTemporalitySelector = DeltaTemporalitySelector;
  var LowMemoryTemporalitySelector = (instrumentType) => {
    switch (instrumentType) {
      case sdk_metrics_1.InstrumentType.COUNTER:
      case sdk_metrics_1.InstrumentType.HISTOGRAM:
        return sdk_metrics_1.AggregationTemporality.DELTA;
      case sdk_metrics_1.InstrumentType.GAUGE:
      case sdk_metrics_1.InstrumentType.UP_DOWN_COUNTER:
      case sdk_metrics_1.InstrumentType.OBSERVABLE_UP_DOWN_COUNTER:
      case sdk_metrics_1.InstrumentType.OBSERVABLE_COUNTER:
      case sdk_metrics_1.InstrumentType.OBSERVABLE_GAUGE:
        return sdk_metrics_1.AggregationTemporality.CUMULATIVE;
    }
  };
  exports.LowMemoryTemporalitySelector = LowMemoryTemporalitySelector;
  function chooseTemporalitySelectorFromEnvironment() {
    const configuredTemporality = ((0, core_1.getStringFromEnv)("OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE") ?? "cumulative").toLowerCase();
    if (configuredTemporality === "cumulative") {
      return exports.CumulativeTemporalitySelector;
    }
    if (configuredTemporality === "delta") {
      return exports.DeltaTemporalitySelector;
    }
    if (configuredTemporality === "lowmemory") {
      return exports.LowMemoryTemporalitySelector;
    }
    api_1.diag.warn(`OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE is set to '${configuredTemporality}', but only 'cumulative' and 'delta' are allowed. Using default ('cumulative') instead.`);
    return exports.CumulativeTemporalitySelector;
  }
  function chooseTemporalitySelector(temporalityPreference) {
    if (temporalityPreference != null) {
      if (temporalityPreference === OTLPMetricExporterOptions_1.AggregationTemporalityPreference.DELTA) {
        return exports.DeltaTemporalitySelector;
      } else if (temporalityPreference === OTLPMetricExporterOptions_1.AggregationTemporalityPreference.LOWMEMORY) {
        return exports.LowMemoryTemporalitySelector;
      }
      return exports.CumulativeTemporalitySelector;
    }
    return chooseTemporalitySelectorFromEnvironment();
  }
  var DEFAULT_AGGREGATION = Object.freeze({
    type: sdk_metrics_1.AggregationType.DEFAULT
  });
  function chooseAggregationSelector(config) {
    return config?.aggregationPreference ?? (() => DEFAULT_AGGREGATION);
  }

  class OTLPMetricExporterBase extends otlp_exporter_base_1.OTLPExporterBase {
    _aggregationTemporalitySelector;
    _aggregationSelector;
    constructor(delegate, config) {
      super(delegate);
      this._aggregationSelector = chooseAggregationSelector(config);
      this._aggregationTemporalitySelector = chooseTemporalitySelector(config?.temporalityPreference);
    }
    selectAggregation(instrumentType) {
      return this._aggregationSelector(instrumentType);
    }
    selectAggregationTemporality(instrumentType) {
      return this._aggregationTemporalitySelector(instrumentType);
    }
  }
  exports.OTLPMetricExporterBase = OTLPMetricExporterBase;
});

// node_modules/.bun/@opentelemetry+exporter-metrics-otlp-http@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-metrics-otlp-http/build/src/semconv.js
var require_semconv7 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_METRIC_EXPORTER = undefined;
  exports.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_METRIC_EXPORTER = "otlp_http_metric_exporter";
});

// node_modules/.bun/@opentelemetry+exporter-metrics-otlp-http@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-metrics-otlp-http/build/src/platform/node/OTLPMetricExporter.js
var require_OTLPMetricExporter = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPMetricExporter = undefined;
  var OTLPMetricExporterBase_1 = require_OTLPMetricExporterBase();
  var otlp_transformer_1 = require_src9();
  var node_http_1 = require_index_node_http();
  var semconv_1 = require_semconv7();

  class OTLPMetricExporter extends OTLPMetricExporterBase_1.OTLPMetricExporterBase {
    _url;
    constructor(config) {
      super((0, node_http_1.createOtlpHttpExportDelegate)((0, node_http_1.convertLegacyHttpOptions)(config ?? {}, "METRICS", "v1/metrics", {
        "Content-Type": "application/json"
      }), otlp_transformer_1.JsonMetricsSerializer, semconv_1.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_METRIC_EXPORTER, otlp_transformer_1.MetricsExporterMetricsHelper, config?.selfObsMeterProvider), config);
      this._url = config?.url;
    }
    setSelfObsMeterProvider(meterProvider) {
      this.setMetrics((0, node_http_1.createOtlpHttpExporterMetrics)(semconv_1.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_METRIC_EXPORTER, otlp_transformer_1.MetricsExporterMetricsHelper, this._url, meterProvider));
    }
  }
  exports.OTLPMetricExporter = OTLPMetricExporter;
});

// node_modules/.bun/@opentelemetry+exporter-metrics-otlp-http@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-metrics-otlp-http/build/src/platform/node/index.js
var require_node5 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPMetricExporter = undefined;
  var OTLPMetricExporter_1 = require_OTLPMetricExporter();
  Object.defineProperty(exports, "OTLPMetricExporter", { enumerable: true, get: function() {
    return OTLPMetricExporter_1.OTLPMetricExporter;
  } });
});

// node_modules/.bun/@opentelemetry+exporter-metrics-otlp-http@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-metrics-otlp-http/build/src/platform/index.js
var require_platform5 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPMetricExporter = undefined;
  var node_1 = require_node5();
  Object.defineProperty(exports, "OTLPMetricExporter", { enumerable: true, get: function() {
    return node_1.OTLPMetricExporter;
  } });
});

// node_modules/.bun/@opentelemetry+exporter-metrics-otlp-http@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-metrics-otlp-http/build/src/index.js
var require_src12 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPMetricExporterBase = exports.LowMemoryTemporalitySelector = exports.DeltaTemporalitySelector = exports.CumulativeTemporalitySelector = exports.AggregationTemporalityPreference = exports.OTLPMetricExporter = undefined;
  var platform_1 = require_platform5();
  Object.defineProperty(exports, "OTLPMetricExporter", { enumerable: true, get: function() {
    return platform_1.OTLPMetricExporter;
  } });
  var OTLPMetricExporterOptions_1 = require_OTLPMetricExporterOptions();
  Object.defineProperty(exports, "AggregationTemporalityPreference", { enumerable: true, get: function() {
    return OTLPMetricExporterOptions_1.AggregationTemporalityPreference;
  } });
  var OTLPMetricExporterBase_1 = require_OTLPMetricExporterBase();
  Object.defineProperty(exports, "CumulativeTemporalitySelector", { enumerable: true, get: function() {
    return OTLPMetricExporterBase_1.CumulativeTemporalitySelector;
  } });
  Object.defineProperty(exports, "DeltaTemporalitySelector", { enumerable: true, get: function() {
    return OTLPMetricExporterBase_1.DeltaTemporalitySelector;
  } });
  Object.defineProperty(exports, "LowMemoryTemporalitySelector", { enumerable: true, get: function() {
    return OTLPMetricExporterBase_1.LowMemoryTemporalitySelector;
  } });
  Object.defineProperty(exports, "OTLPMetricExporterBase", { enumerable: true, get: function() {
    return OTLPMetricExporterBase_1.OTLPMetricExporterBase;
  } });
});

// node_modules/.bun/@opentelemetry+exporter-metrics-otlp-proto@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-metrics-otlp-proto/build/src/semconv.js
var require_semconv8 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_METRIC_EXPORTER = undefined;
  exports.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_METRIC_EXPORTER = "otlp_http_metric_exporter";
});

// node_modules/.bun/@opentelemetry+exporter-metrics-otlp-proto@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-metrics-otlp-proto/build/src/platform/node/OTLPMetricExporter.js
var require_OTLPMetricExporter2 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPMetricExporter = undefined;
  var exporter_metrics_otlp_http_1 = require_src12();
  var otlp_transformer_1 = require_src9();
  var node_http_1 = require_index_node_http();
  var semconv_1 = require_semconv8();

  class OTLPMetricExporter extends exporter_metrics_otlp_http_1.OTLPMetricExporterBase {
    _url;
    constructor(config) {
      super((0, node_http_1.createOtlpHttpExportDelegate)((0, node_http_1.convertLegacyHttpOptions)(config ?? {}, "METRICS", "v1/metrics", {
        "Content-Type": "application/x-protobuf"
      }), otlp_transformer_1.ProtobufMetricsSerializer, semconv_1.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_METRIC_EXPORTER, otlp_transformer_1.MetricsExporterMetricsHelper, config?.selfObsMeterProvider), config);
      this._url = config?.url;
    }
    setSelfObsMeterProvider(meterProvider) {
      this.setMetrics((0, node_http_1.createOtlpHttpExporterMetrics)(semconv_1.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_METRIC_EXPORTER, otlp_transformer_1.MetricsExporterMetricsHelper, this._url, meterProvider));
    }
  }
  exports.OTLPMetricExporter = OTLPMetricExporter;
});

// node_modules/.bun/@opentelemetry+exporter-metrics-otlp-proto@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-metrics-otlp-proto/build/src/platform/node/index.js
var require_node6 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPMetricExporter = undefined;
  var OTLPMetricExporter_1 = require_OTLPMetricExporter2();
  Object.defineProperty(exports, "OTLPMetricExporter", { enumerable: true, get: function() {
    return OTLPMetricExporter_1.OTLPMetricExporter;
  } });
});

// node_modules/.bun/@opentelemetry+exporter-metrics-otlp-proto@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-metrics-otlp-proto/build/src/platform/index.js
var require_platform6 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPMetricExporter = undefined;
  var node_1 = require_node6();
  Object.defineProperty(exports, "OTLPMetricExporter", { enumerable: true, get: function() {
    return node_1.OTLPMetricExporter;
  } });
});

// node_modules/.bun/@opentelemetry+exporter-metrics-otlp-proto@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-metrics-otlp-proto/build/src/index.js
var require_src13 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPMetricExporter = undefined;
  var platform_1 = require_platform6();
  Object.defineProperty(exports, "OTLPMetricExporter", { enumerable: true, get: function() {
    return platform_1.OTLPMetricExporter;
  } });
});

// node_modules/.bun/@opentelemetry+exporter-trace-otlp-http@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-trace-otlp-http/build/src/semconv.js
var require_semconv9 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_SPAN_EXPORTER = undefined;
  exports.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_SPAN_EXPORTER = "otlp_http_span_exporter";
});

// node_modules/.bun/@opentelemetry+exporter-trace-otlp-http@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-trace-otlp-http/build/src/platform/node/OTLPTraceExporter.js
var require_OTLPTraceExporter = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPTraceExporter = undefined;
  var otlp_exporter_base_1 = require_src6();
  var otlp_transformer_1 = require_src9();
  var node_http_1 = require_index_node_http();
  var semconv_1 = require_semconv9();

  class OTLPTraceExporter extends otlp_exporter_base_1.OTLPExporterBase {
    constructor(config = {}) {
      super((0, node_http_1.createOtlpHttpExportDelegate)((0, node_http_1.convertLegacyHttpOptions)(config, "TRACES", "v1/traces", {
        "Content-Type": "application/json"
      }), otlp_transformer_1.JsonTraceSerializer, semconv_1.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_SPAN_EXPORTER, otlp_transformer_1.TraceExporterMetricsHelper, config.selfObsMeterProvider));
    }
  }
  exports.OTLPTraceExporter = OTLPTraceExporter;
});

// node_modules/.bun/@opentelemetry+exporter-trace-otlp-http@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-trace-otlp-http/build/src/platform/node/index.js
var require_node7 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPTraceExporter = undefined;
  var OTLPTraceExporter_1 = require_OTLPTraceExporter();
  Object.defineProperty(exports, "OTLPTraceExporter", { enumerable: true, get: function() {
    return OTLPTraceExporter_1.OTLPTraceExporter;
  } });
});

// node_modules/.bun/@opentelemetry+exporter-trace-otlp-http@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-trace-otlp-http/build/src/platform/index.js
var require_platform7 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPTraceExporter = undefined;
  var node_1 = require_node7();
  Object.defineProperty(exports, "OTLPTraceExporter", { enumerable: true, get: function() {
    return node_1.OTLPTraceExporter;
  } });
});

// node_modules/.bun/@opentelemetry+exporter-trace-otlp-http@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-trace-otlp-http/build/src/index.js
var require_src14 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPTraceExporter = undefined;
  var platform_1 = require_platform7();
  Object.defineProperty(exports, "OTLPTraceExporter", { enumerable: true, get: function() {
    return platform_1.OTLPTraceExporter;
  } });
});

// node_modules/.bun/@opentelemetry+exporter-trace-otlp-proto@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-trace-otlp-proto/build/src/semconv.js
var require_semconv10 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_SPAN_EXPORTER = undefined;
  exports.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_SPAN_EXPORTER = "otlp_http_span_exporter";
});

// node_modules/.bun/@opentelemetry+exporter-trace-otlp-proto@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-trace-otlp-proto/build/src/platform/node/OTLPTraceExporter.js
var require_OTLPTraceExporter2 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPTraceExporter = undefined;
  var otlp_exporter_base_1 = require_src6();
  var otlp_transformer_1 = require_src9();
  var node_http_1 = require_index_node_http();
  var semconv_1 = require_semconv10();

  class OTLPTraceExporter extends otlp_exporter_base_1.OTLPExporterBase {
    constructor(config = {}) {
      super((0, node_http_1.createOtlpHttpExportDelegate)((0, node_http_1.convertLegacyHttpOptions)(config, "TRACES", "v1/traces", {
        "Content-Type": "application/x-protobuf"
      }), otlp_transformer_1.ProtobufTraceSerializer, semconv_1.OTEL_COMPONENT_TYPE_VALUE_OTLP_HTTP_SPAN_EXPORTER, otlp_transformer_1.TraceExporterMetricsHelper, config.selfObsMeterProvider));
    }
  }
  exports.OTLPTraceExporter = OTLPTraceExporter;
});

// node_modules/.bun/@opentelemetry+exporter-trace-otlp-proto@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-trace-otlp-proto/build/src/platform/node/index.js
var require_node8 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPTraceExporter = undefined;
  var OTLPTraceExporter_1 = require_OTLPTraceExporter2();
  Object.defineProperty(exports, "OTLPTraceExporter", { enumerable: true, get: function() {
    return OTLPTraceExporter_1.OTLPTraceExporter;
  } });
});

// node_modules/.bun/@opentelemetry+exporter-trace-otlp-proto@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-trace-otlp-proto/build/src/platform/index.js
var require_platform8 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPTraceExporter = undefined;
  var node_1 = require_node8();
  Object.defineProperty(exports, "OTLPTraceExporter", { enumerable: true, get: function() {
    return node_1.OTLPTraceExporter;
  } });
});

// node_modules/.bun/@opentelemetry+exporter-trace-otlp-proto@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/exporter-trace-otlp-proto/build/src/index.js
var require_src15 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTLPTraceExporter = undefined;
  var platform_1 = require_platform8();
  Object.defineProperty(exports, "OTLPTraceExporter", { enumerable: true, get: function() {
    return platform_1.OTLPTraceExporter;
  } });
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/utils/validation.js
var require_validation = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.normalizeScopeAttributes = exports.addAttribute = exports.AddAttributeDecision = exports.isLogAttributeValue = undefined;
  var api_1 = require_src();
  function isLogAttributeValue(val) {
    return isLogAttributeValueInternal(val, new WeakSet);
  }
  exports.isLogAttributeValue = isLogAttributeValue;
  function isLogAttributeValueInternal(val, visited) {
    if (val == null) {
      return true;
    }
    if (typeof val === "string" || typeof val === "number" || typeof val === "boolean") {
      return true;
    }
    if (val instanceof Uint8Array) {
      return true;
    }
    if (typeof val === "object") {
      if (visited.has(val)) {
        return false;
      }
      visited.add(val);
      if (Array.isArray(val)) {
        for (const item of val) {
          if (!isLogAttributeValueInternal(item, visited)) {
            return false;
          }
        }
        return true;
      }
      const obj = val;
      if (obj.constructor !== Object && obj.constructor !== undefined) {
        return false;
      }
      for (const key in obj) {
        if (Object.prototype.hasOwnProperty.call(obj, key) && !isLogAttributeValueInternal(obj[key], visited)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }
  var AddAttributeDecision;
  (function(AddAttributeDecision) {
    AddAttributeDecision[AddAttributeDecision["DROP_INVALID"] = 0] = "DROP_INVALID";
    AddAttributeDecision[AddAttributeDecision["DROP_LIMIT_REACHED"] = 1] = "DROP_LIMIT_REACHED";
    AddAttributeDecision[AddAttributeDecision["ADD_NEW"] = 2] = "ADD_NEW";
    AddAttributeDecision[AddAttributeDecision["ADD_OVERWRITE_EXISTING"] = 3] = "ADD_OVERWRITE_EXISTING";
  })(AddAttributeDecision || (exports.AddAttributeDecision = AddAttributeDecision = {}));
  function addAttribute(attributes, limits, currentAttributesCount, key, value) {
    if (key.length === 0) {
      api_1.diag.warn(`Invalid attribute key: ${key}`);
      return AddAttributeDecision.DROP_INVALID;
    }
    if (!isLogAttributeValue(value)) {
      api_1.diag.warn(`Invalid attribute value set for key: ${key}`);
      return AddAttributeDecision.DROP_INVALID;
    }
    const isNewKey = !Object.prototype.hasOwnProperty.call(attributes, key);
    if (isNewKey && currentAttributesCount >= limits.attributeCountLimit) {
      return AddAttributeDecision.DROP_LIMIT_REACHED;
    }
    attributes[key] = truncateToSize(value, limits.attributeValueLengthLimit);
    if (isNewKey) {
      return AddAttributeDecision.ADD_NEW;
    }
    return AddAttributeDecision.ADD_OVERWRITE_EXISTING;
  }
  exports.addAttribute = addAttribute;
  function truncateToSize(value, limit) {
    if (limit <= 0) {
      api_1.diag.warn(`Attribute value limit must be positive, got ${limit}`);
      return value;
    }
    if (value == null) {
      return value;
    }
    if (typeof value === "string") {
      if (value.length <= limit) {
        return value;
      }
      return value.substring(0, limit);
    }
    if (value instanceof Uint8Array) {
      return value;
    }
    if (Array.isArray(value)) {
      return value.map((val) => truncateToSize(val, limit));
    }
    if (typeof value === "object") {
      const truncatedObj = {};
      for (const [k, v] of Object.entries(value)) {
        truncatedObj[k] = truncateToSize(v, limit);
      }
      return truncatedObj;
    }
    return value;
  }
  function normalizeScopeAttributes(limits, attributes) {
    if (attributes == null) {
      return {};
    }
    const normalizedAttributes = {};
    let currentAttributesCount = 0;
    let droppedAttributesCount = 0;
    for (const [key, value] of Object.entries(attributes)) {
      const decision = addAttribute(normalizedAttributes, limits, currentAttributesCount, key, value);
      if (decision === AddAttributeDecision.ADD_NEW) {
        currentAttributesCount += 1;
      } else if (decision === AddAttributeDecision.DROP_INVALID) {
        droppedAttributesCount += 1;
      } else if (decision === AddAttributeDecision.DROP_LIMIT_REACHED) {
        droppedAttributesCount += 1;
      }
    }
    return {
      attributes: currentAttributesCount > 0 ? normalizedAttributes : undefined,
      droppedAttributesCount
    };
  }
  exports.normalizeScopeAttributes = normalizeScopeAttributes;
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/LogRecordImpl.js
var require_LogRecordImpl = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.LogRecordImpl = undefined;
  var api = require_src();
  var core_1 = require_src5();
  var semantic_conventions_1 = require_src4();
  var validation_1 = require_validation();

  class LogRecordImpl {
    resource;
    instrumentationScope;
    attributes = {};
    _hrTime;
    _hrTimeObserved;
    _spanContext;
    _severityText;
    _severityNumber;
    _body;
    _eventName;
    _attributesCount = 0;
    _droppedAttributesCount = 0;
    _isReadonly = false;
    _logRecordLimits;
    get hrTime() {
      return this._hrTime;
    }
    set hrTime(hrTime) {
      if (this._isLogRecordReadonly()) {
        return;
      }
      this._hrTime = hrTime;
    }
    get hrTimeObserved() {
      return this._hrTimeObserved;
    }
    set hrTimeObserved(hrTimeObserved) {
      if (this._isLogRecordReadonly()) {
        return;
      }
      this._hrTimeObserved = hrTimeObserved;
    }
    get spanContext() {
      return this._spanContext;
    }
    set spanContext(spanContext) {
      if (this._isLogRecordReadonly()) {
        return;
      }
      this._spanContext = spanContext;
    }
    set severityText(severityText) {
      if (this._isLogRecordReadonly()) {
        return;
      }
      this._severityText = severityText;
    }
    get severityText() {
      return this._severityText;
    }
    set severityNumber(severityNumber) {
      if (this._isLogRecordReadonly()) {
        return;
      }
      this._severityNumber = severityNumber;
    }
    get severityNumber() {
      return this._severityNumber;
    }
    set body(body) {
      if (this._isLogRecordReadonly()) {
        return;
      }
      this._body = body;
    }
    get body() {
      return this._body;
    }
    get eventName() {
      return this._eventName;
    }
    set eventName(eventName) {
      if (this._isLogRecordReadonly()) {
        return;
      }
      this._eventName = eventName;
    }
    get droppedAttributesCount() {
      return this._droppedAttributesCount;
    }
    constructor(_sharedState, instrumentationScope, logRecord) {
      const { timestamp, observedTimestamp, eventName, severityNumber, severityText, body, attributes = {}, exception, context } = logRecord;
      const now = Date.now();
      this._hrTime = (0, core_1.timeInputToHrTime)(timestamp ?? now);
      this._hrTimeObserved = (0, core_1.timeInputToHrTime)(observedTimestamp ?? now);
      if (context) {
        const spanContext = api.trace.getSpanContext(context);
        if (spanContext && api.isSpanContextValid(spanContext)) {
          this._spanContext = spanContext;
        }
      }
      this.severityNumber = severityNumber;
      this.severityText = severityText;
      this.body = body;
      this.resource = _sharedState.resource;
      this.instrumentationScope = instrumentationScope;
      this._logRecordLimits = _sharedState.logRecordLimits;
      this._eventName = eventName;
      this.setAttributes(attributes);
      if (exception != null) {
        this._setException(exception);
      }
    }
    setAttribute(key, value) {
      if (this._isLogRecordReadonly()) {
        return this;
      }
      const decision = (0, validation_1.addAttribute)(this.attributes, this._logRecordLimits, this._attributesCount, key, value);
      if (decision === validation_1.AddAttributeDecision.DROP_LIMIT_REACHED) {
        this._droppedAttributesCount++;
        if (this._droppedAttributesCount === 1) {
          api.diag.warn("Dropping extra attributes.");
        }
      } else if (decision === validation_1.AddAttributeDecision.ADD_NEW) {
        this._attributesCount++;
      }
      return this;
    }
    setAttributes(attributes) {
      for (const [k, v] of Object.entries(attributes)) {
        this.setAttribute(k, v);
      }
      return this;
    }
    setBody(body) {
      this.body = body;
      return this;
    }
    setEventName(eventName) {
      this.eventName = eventName;
      return this;
    }
    setSeverityNumber(severityNumber) {
      this.severityNumber = severityNumber;
      return this;
    }
    setSeverityText(severityText) {
      this.severityText = severityText;
      return this;
    }
    _makeReadonly() {
      this._isReadonly = true;
    }
    _setException(exception) {
      let hasMinimumAttributes = false;
      if (typeof exception === "string" || typeof exception === "number") {
        if (!Object.hasOwn(this.attributes, semantic_conventions_1.ATTR_EXCEPTION_MESSAGE)) {
          this.setAttribute(semantic_conventions_1.ATTR_EXCEPTION_MESSAGE, String(exception));
        }
        hasMinimumAttributes = true;
      } else if (exception && typeof exception === "object") {
        const exceptionObj = exception;
        if (exceptionObj.code) {
          if (!Object.hasOwn(this.attributes, semantic_conventions_1.ATTR_EXCEPTION_TYPE)) {
            this.setAttribute(semantic_conventions_1.ATTR_EXCEPTION_TYPE, exceptionObj.code.toString());
          }
          hasMinimumAttributes = true;
        } else if (exceptionObj.name) {
          if (!Object.hasOwn(this.attributes, semantic_conventions_1.ATTR_EXCEPTION_TYPE)) {
            this.setAttribute(semantic_conventions_1.ATTR_EXCEPTION_TYPE, exceptionObj.name);
          }
          hasMinimumAttributes = true;
        }
        if (exceptionObj.message) {
          if (!Object.hasOwn(this.attributes, semantic_conventions_1.ATTR_EXCEPTION_MESSAGE)) {
            this.setAttribute(semantic_conventions_1.ATTR_EXCEPTION_MESSAGE, exceptionObj.message);
          }
          hasMinimumAttributes = true;
        }
        if (exceptionObj.stack) {
          if (!Object.hasOwn(this.attributes, semantic_conventions_1.ATTR_EXCEPTION_STACKTRACE)) {
            this.setAttribute(semantic_conventions_1.ATTR_EXCEPTION_STACKTRACE, exceptionObj.stack);
          }
          hasMinimumAttributes = true;
        }
      }
      if (!hasMinimumAttributes) {
        api.diag.warn(`Failed to record an exception ${exception}`);
      }
    }
    _isLogRecordReadonly() {
      if (this._isReadonly) {
        api.diag.warn("Can not execute the operation on emitted log record");
      }
      return this._isReadonly;
    }
  }
  exports.LogRecordImpl = LogRecordImpl;
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/Logger.js
var require_Logger = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.Logger = undefined;
  var api_logs_1 = require_src2();
  var api_1 = require_src();
  var LogRecordImpl_1 = require_LogRecordImpl();

  class Logger {
    _instrumentationScope;
    _sharedState;
    _loggerConfig;
    constructor(instrumentationScope, sharedState) {
      this._instrumentationScope = instrumentationScope;
      this._sharedState = sharedState;
      this._loggerConfig = this._sharedState.getLoggerConfig(this._instrumentationScope);
    }
    emit(logRecord) {
      const currentContext = logRecord.context || api_1.context.active();
      if (!this.enabled(logRecord)) {
        return;
      }
      const logRecordInstance = new LogRecordImpl_1.LogRecordImpl(this._sharedState, this._instrumentationScope, {
        context: currentContext,
        ...logRecord
      });
      this._sharedState.loggerMetrics.emitLog();
      this._sharedState.activeProcessor.onEmit(logRecordInstance, currentContext);
      logRecordInstance._makeReadonly();
    }
    enabled(options) {
      if (this._sharedState.hasShutdown) {
        return false;
      }
      const loggerConfig = this._loggerConfig;
      if (loggerConfig.disabled) {
        return false;
      }
      const severityNumber = options?.severityNumber;
      if (typeof severityNumber === "number" && severityNumber !== api_logs_1.SeverityNumber.UNSPECIFIED && severityNumber < loggerConfig.minimumSeverity) {
        return false;
      }
      const currentContext = options?.context || api_1.context.active();
      if (loggerConfig.traceBased) {
        const spanContext = api_1.trace.getSpanContext(currentContext);
        if (spanContext && (0, api_1.isSpanContextValid)(spanContext)) {
          const isSampled = (spanContext.traceFlags & api_1.TraceFlags.SAMPLED) === api_1.TraceFlags.SAMPLED;
          if (!isSampled) {
            return false;
          }
        }
      }
      const enabledOpts = {
        context: currentContext,
        instrumentationScope: this._instrumentationScope,
        severityNumber: options?.severityNumber,
        eventName: options?.eventName
      };
      for (const processor of this._sharedState.processors) {
        if (!processor.enabled || processor.enabled(enabledOpts)) {
          return true;
        }
      }
      return false;
    }
  }
  exports.Logger = Logger;
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/export/NoopLogRecordProcessor.js
var require_NoopLogRecordProcessor = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.NoopLogRecordProcessor = undefined;

  class NoopLogRecordProcessor {
    forceFlush() {
      return Promise.resolve();
    }
    onEmit(_logRecord, _context) {}
    shutdown() {
      return Promise.resolve();
    }
    enabled(_options) {
      return false;
    }
  }
  exports.NoopLogRecordProcessor = NoopLogRecordProcessor;
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/MultiLogRecordProcessor.js
var require_MultiLogRecordProcessor = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.MultiLogRecordProcessor = undefined;
  var core_1 = require_src5();

  class MultiLogRecordProcessor {
    processors;
    constructor(processors) {
      this.processors = processors;
    }
    async forceFlush(options) {
      const timeout = options?.timeoutMillis ?? 30000;
      await Promise.all(this.processors.map((processor) => (0, core_1.callWithTimeout)(processor.forceFlush(), timeout)));
    }
    onEmit(logRecord, context) {
      this.processors.forEach((processors) => processors.onEmit(logRecord, context));
    }
    async shutdown() {
      await Promise.all(this.processors.map((processor) => processor.shutdown()));
    }
    enabled(options) {
      for (const processor of this.processors) {
        if (!processor.enabled || processor.enabled(options)) {
          return true;
        }
      }
      return false;
    }
  }
  exports.MultiLogRecordProcessor = MultiLogRecordProcessor;
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/internal/utils.js
var require_utils8 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.getInstrumentationScopeKey = undefined;
  function normalizeAnyValue(value) {
    if (value === undefined) {
      return ["u", null];
    }
    if (value === null) {
      return ["n", null];
    }
    const valueType = typeof value;
    if (valueType === "string") {
      return ["s", value];
    }
    if (valueType === "boolean") {
      return ["b", value];
    }
    if (valueType === "number") {
      if (Number.isNaN(value))
        return ["nan", null];
      if (value === Infinity)
        return ["inf", null];
      if (value === -Infinity)
        return ["-inf", null];
      if (Object.is(value, -0))
        return ["n0", null];
      return ["d", value];
    }
    if (value instanceof Uint8Array) {
      return ["bytes", Array.from(value)];
    }
    if (Array.isArray(value)) {
      return ["arr", value.map(normalizeAnyValue)];
    }
    return [
      "map",
      Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => [k, normalizeAnyValue(v)])
    ];
  }
  function getInstrumentationScopeKey(scope) {
    return JSON.stringify([
      scope.name,
      scope.version || "",
      scope.schemaUrl || "",
      normalizeAnyValue(scope.attributes),
      scope.droppedAttributesCount ?? 0
    ]);
  }
  exports.getInstrumentationScopeKey = getInstrumentationScopeKey;
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/semconv.js
var require_semconv11 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ATTR_ERROR_TYPE = exports.OTEL_COMPONENT_TYPE_VALUE_SIMPLE_LOG_PROCESSOR = exports.OTEL_COMPONENT_TYPE_VALUE_BATCHING_LOG_PROCESSOR = exports.ATTR_OTEL_COMPONENT_TYPE = exports.ATTR_OTEL_COMPONENT_NAME = exports.METRIC_OTEL_SDK_PROCESSOR_LOG_QUEUE_SIZE = exports.METRIC_OTEL_SDK_PROCESSOR_LOG_QUEUE_CAPACITY = exports.METRIC_OTEL_SDK_PROCESSOR_LOG_PROCESSED = exports.METRIC_OTEL_SDK_LOG_CREATED = undefined;
  exports.METRIC_OTEL_SDK_LOG_CREATED = "otel.sdk.log.created";
  exports.METRIC_OTEL_SDK_PROCESSOR_LOG_PROCESSED = "otel.sdk.processor.log.processed";
  exports.METRIC_OTEL_SDK_PROCESSOR_LOG_QUEUE_CAPACITY = "otel.sdk.processor.log.queue.capacity";
  exports.METRIC_OTEL_SDK_PROCESSOR_LOG_QUEUE_SIZE = "otel.sdk.processor.log.queue.size";
  exports.ATTR_OTEL_COMPONENT_NAME = "otel.component.name";
  exports.ATTR_OTEL_COMPONENT_TYPE = "otel.component.type";
  exports.OTEL_COMPONENT_TYPE_VALUE_BATCHING_LOG_PROCESSOR = "batching_log_processor";
  exports.OTEL_COMPONENT_TYPE_VALUE_SIMPLE_LOG_PROCESSOR = "simple_log_processor";
  exports.ATTR_ERROR_TYPE = "error.type";
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/LoggerMetrics.js
var require_LoggerMetrics = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.LoggerMetrics = undefined;
  var semconv_1 = require_semconv11();

  class LoggerMetrics {
    createdLogs;
    constructor(meter) {
      this.createdLogs = meter.createCounter(semconv_1.METRIC_OTEL_SDK_LOG_CREATED, {
        unit: "{log_record}",
        description: "The number of logs submitted to enabled SDK Loggers."
      });
    }
    emitLog() {
      this.createdLogs.add(1);
    }
  }
  exports.LoggerMetrics = LoggerMetrics;
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/version.js
var require_version4 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.VERSION = undefined;
  exports.VERSION = "0.222.0";
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/internal/LoggerProviderSharedState.js
var require_LoggerProviderSharedState = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.LoggerProviderSharedState = exports.DEFAULT_LOGGER_CONFIGURATOR = undefined;
  var api_1 = require_src();
  var api_logs_1 = require_src2();
  var NoopLogRecordProcessor_1 = require_NoopLogRecordProcessor();
  var MultiLogRecordProcessor_1 = require_MultiLogRecordProcessor();
  var utils_1 = require_utils8();
  var LoggerMetrics_1 = require_LoggerMetrics();
  var version_1 = require_version4();
  var DEFAULT_LOGGER_CONFIG = {
    disabled: false,
    minimumSeverity: api_logs_1.SeverityNumber.UNSPECIFIED,
    traceBased: false
  };
  var DEFAULT_LOGGER_CONFIGURATOR = () => ({
    ...DEFAULT_LOGGER_CONFIG
  });
  exports.DEFAULT_LOGGER_CONFIGURATOR = DEFAULT_LOGGER_CONFIGURATOR;

  class LoggerProviderSharedState {
    loggers = new Map;
    activeProcessor;
    registeredLogRecordProcessors = [];
    resource;
    logRecordLimits;
    processors;
    loggerMetrics;
    hasShutdown = false;
    _loggerConfigurator;
    _loggerConfigs = new Map;
    constructor(resource, logRecordLimits, processors, loggerConfigurator, meterProvider) {
      this.resource = resource;
      this.logRecordLimits = logRecordLimits;
      this.processors = processors;
      if (processors.length > 0) {
        this.registeredLogRecordProcessors = processors;
        this.activeProcessor = new MultiLogRecordProcessor_1.MultiLogRecordProcessor(this.registeredLogRecordProcessors);
      } else {
        this.activeProcessor = new NoopLogRecordProcessor_1.NoopLogRecordProcessor;
      }
      this._loggerConfigurator = loggerConfigurator ?? exports.DEFAULT_LOGGER_CONFIGURATOR;
      const meter = meterProvider ? meterProvider.getMeter("@opentelemetry/sdk-logs", version_1.VERSION) : (0, api_1.createNoopMeter)();
      this.loggerMetrics = new LoggerMetrics_1.LoggerMetrics(meter);
    }
    getLoggerConfig(instrumentationScope) {
      const key = (0, utils_1.getInstrumentationScopeKey)(instrumentationScope);
      let config = this._loggerConfigs.get(key);
      if (config) {
        return config;
      }
      config = this._loggerConfigurator(instrumentationScope);
      this._loggerConfigs.set(key, config);
      return config;
    }
  }
  exports.LoggerProviderSharedState = LoggerProviderSharedState;
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/LoggerProvider.js
var require_LoggerProvider = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.LoggerProvider = exports.DEFAULT_LOGGER_NAME = undefined;
  var api_1 = require_src();
  var api_logs_1 = require_src2();
  var resources_1 = require_src7();
  var core_1 = require_src5();
  var Logger_1 = require_Logger();
  var LoggerProviderSharedState_1 = require_LoggerProviderSharedState();
  var utils_1 = require_utils8();
  var validation_1 = require_validation();
  exports.DEFAULT_LOGGER_NAME = "unknown";

  class LoggerProvider {
    _shutdownOnce;
    _sharedState;
    constructor(config = {}) {
      const mergedConfig = {
        resource: config.resource ?? (0, resources_1.defaultResource)(),
        logRecordLimits: {
          attributeCountLimit: config.logRecordLimits?.attributeCountLimit ?? 128,
          attributeValueLengthLimit: config.logRecordLimits?.attributeValueLengthLimit ?? Infinity
        },
        loggerConfigurator: config.loggerConfigurator ?? LoggerProviderSharedState_1.DEFAULT_LOGGER_CONFIGURATOR,
        processors: config.processors ?? [],
        meterProvider: config.meterProvider
      };
      this._sharedState = new LoggerProviderSharedState_1.LoggerProviderSharedState(mergedConfig.resource, mergedConfig.logRecordLimits, mergedConfig.processors, mergedConfig.loggerConfigurator, mergedConfig.meterProvider);
      this._shutdownOnce = new core_1.BindOnceFuture(this._shutdown, this);
    }
    getLogger(name, version, options) {
      if (this._shutdownOnce.isCalled) {
        api_1.diag.warn("A shutdown LoggerProvider cannot provide a Logger");
        return (0, api_logs_1.createNoopLogger)();
      }
      if (!name) {
        api_1.diag.warn("Logger requested without instrumentation scope name.");
      }
      const loggerName = name || exports.DEFAULT_LOGGER_NAME;
      const instrumentationScope = {
        name: loggerName,
        version,
        schemaUrl: options?.schemaUrl,
        ...(0, validation_1.normalizeScopeAttributes)(this._sharedState.logRecordLimits, options?.attributes)
      };
      const key = (0, utils_1.getInstrumentationScopeKey)(instrumentationScope);
      if (!this._sharedState.loggers.has(key)) {
        this._sharedState.loggers.set(key, new Logger_1.Logger(instrumentationScope, this._sharedState));
      }
      return this._sharedState.loggers.get(key);
    }
    forceFlush(options) {
      if (this._shutdownOnce.isCalled) {
        api_1.diag.warn("invalid attempt to force flush after LoggerProvider shutdown");
        return this._shutdownOnce.promise;
      }
      return this._sharedState.activeProcessor.forceFlush(options);
    }
    shutdown() {
      if (this._shutdownOnce.isCalled) {
        api_1.diag.warn("shutdown may only be called once per LoggerProvider");
        return this._shutdownOnce.promise;
      }
      return this._shutdownOnce.call();
    }
    _shutdown() {
      this._sharedState.hasShutdown = true;
      return this._sharedState.activeProcessor.shutdown();
    }
  }
  exports.LoggerProvider = LoggerProvider;
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/export/ConsoleLogRecordExporter.js
var require_ConsoleLogRecordExporter = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ConsoleLogRecordExporter = undefined;
  var core_1 = require_src5();

  class ConsoleLogRecordExporter {
    export(logs, resultCallback) {
      this._sendLogRecords(logs, resultCallback);
    }
    async forceFlush() {}
    async shutdown() {}
    _exportInfo(logRecord) {
      return {
        resource: {
          attributes: logRecord.resource.attributes
        },
        instrumentationScope: logRecord.instrumentationScope,
        timestamp: (0, core_1.hrTimeToMicroseconds)(logRecord.hrTime),
        traceId: logRecord.spanContext?.traceId,
        spanId: logRecord.spanContext?.spanId,
        traceFlags: logRecord.spanContext?.traceFlags,
        severityText: logRecord.severityText,
        severityNumber: logRecord.severityNumber,
        eventName: logRecord.eventName,
        body: logRecord.body,
        attributes: logRecord.attributes
      };
    }
    _sendLogRecords(logRecords, done) {
      for (const logRecord of logRecords) {
        console.dir(this._exportInfo(logRecord), { depth: 3 });
      }
      done?.({ code: core_1.ExportResultCode.SUCCESS });
    }
  }
  exports.ConsoleLogRecordExporter = ConsoleLogRecordExporter;
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/export/LogRecordProcessorMetrics.js
var require_LogRecordProcessorMetrics = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.LogRecordProcessorMetrics = undefined;
  var semconv_1 = require_semconv11();
  var componentCounter = new Map;

  class LogRecordProcessorMetrics {
    processedLogs;
    queueSize;
    queueSizeCallback;
    standardAttrs;
    droppedAttrs;
    constructor(componentType, meter, queueConfig) {
      const counter = componentCounter.get(componentType) ?? 0;
      componentCounter.set(componentType, counter + 1);
      this.standardAttrs = {
        [semconv_1.ATTR_OTEL_COMPONENT_TYPE]: componentType,
        [semconv_1.ATTR_OTEL_COMPONENT_NAME]: `${componentType}/${counter}`
      };
      this.droppedAttrs = {
        ...this.standardAttrs,
        [semconv_1.ATTR_ERROR_TYPE]: "queue_full"
      };
      this.processedLogs = meter.createCounter(semconv_1.METRIC_OTEL_SDK_PROCESSOR_LOG_PROCESSED, {
        unit: "{log_record}",
        description: "The number of log records for which the processing has finished, either successful or failed."
      });
      if (queueConfig) {
        const { capacity, getQueueSize } = queueConfig;
        const queueCapacity = meter.createUpDownCounter(semconv_1.METRIC_OTEL_SDK_PROCESSOR_LOG_QUEUE_CAPACITY, {
          unit: "{log_record}",
          description: "The maximum number of log records the queue of a given instance of an SDK log processor can hold."
        });
        queueCapacity.add(capacity, this.standardAttrs);
        this.queueSize = meter.createObservableUpDownCounter(semconv_1.METRIC_OTEL_SDK_PROCESSOR_LOG_QUEUE_SIZE, {
          unit: "{log_record}",
          description: "The number of log records in the queue of a given instance of an SDK log processor."
        });
        this.queueSizeCallback = (result) => result.observe(getQueueSize(), this.standardAttrs);
        this.queueSize.addCallback(this.queueSizeCallback);
      }
    }
    dropLogs(count) {
      this.processedLogs.add(count, this.droppedAttrs);
    }
    finishLogs(count, error) {
      if (!error) {
        this.processedLogs.add(count, this.standardAttrs);
        return;
      }
      const attrs = {
        ...this.standardAttrs,
        [semconv_1.ATTR_ERROR_TYPE]: error.name
      };
      this.processedLogs.add(count, attrs);
    }
    shutdown() {
      if (this.queueSize && this.queueSizeCallback) {
        this.queueSize.removeCallback(this.queueSizeCallback);
      }
    }
  }
  exports.LogRecordProcessorMetrics = LogRecordProcessorMetrics;
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/export/SimpleLogRecordProcessor.js
var require_SimpleLogRecordProcessor = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.SimpleLogRecordProcessor = undefined;
  var api_1 = require_src();
  var core_1 = require_src5();
  var semconv_1 = require_semconv11();
  var LogRecordProcessorMetrics_1 = require_LogRecordProcessorMetrics();

  class SimpleLogRecordProcessor {
    _exporter;
    _metrics;
    _shutdownOnce;
    _unresolvedExports;
    constructor(options) {
      this._exporter = options.exporter;
      this._shutdownOnce = new core_1.BindOnceFuture(this._shutdown, this);
      this._unresolvedExports = new Set;
      const meter = options?.selfObsMeterProvider ? options.selfObsMeterProvider.getMeter("@opentelemetry/sdk-logs") : (0, api_1.createNoopMeter)();
      this._metrics = new LogRecordProcessorMetrics_1.LogRecordProcessorMetrics(semconv_1.OTEL_COMPONENT_TYPE_VALUE_SIMPLE_LOG_PROCESSOR, meter);
    }
    onEmit(logRecord, _context) {
      if (this._shutdownOnce.isCalled) {
        return;
      }
      const doExport = () => core_1.internal._export(this._exporter, [logRecord]).then((result) => {
        this._metrics.finishLogs(1, result.error);
        if (result.code !== core_1.ExportResultCode.SUCCESS) {
          (0, core_1.globalErrorHandler)(result.error ?? new Error(`SimpleLogRecordProcessor: log record export failed (status ${result})`));
        }
      }).catch(core_1.globalErrorHandler);
      if (logRecord.resource.asyncAttributesPending) {
        const exportPromise = logRecord.resource.waitForAsyncAttributes?.().then(() => {
          this._unresolvedExports.delete(exportPromise);
          return doExport();
        }, core_1.globalErrorHandler);
        if (exportPromise != null) {
          this._unresolvedExports.add(exportPromise);
        }
      } else {
        doExport();
      }
    }
    async forceFlush() {
      await Promise.all(Array.from(this._unresolvedExports));
    }
    shutdown() {
      return this._shutdownOnce.call();
    }
    _shutdown() {
      this._metrics.shutdown();
      return this._exporter.shutdown();
    }
  }
  exports.SimpleLogRecordProcessor = SimpleLogRecordProcessor;
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/export/InMemoryLogRecordExporter.js
var require_InMemoryLogRecordExporter = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.InMemoryLogRecordExporter = undefined;
  var core_1 = require_src5();

  class InMemoryLogRecordExporter {
    _finishedLogRecords = [];
    _stopped = false;
    export(logs, resultCallback) {
      if (this._stopped) {
        return resultCallback({
          code: core_1.ExportResultCode.FAILED,
          error: new Error("Exporter has been stopped")
        });
      }
      this._finishedLogRecords.push(...logs);
      resultCallback({ code: core_1.ExportResultCode.SUCCESS });
    }
    async shutdown() {
      this._stopped = true;
      this.reset();
    }
    async forceFlush() {}
    getFinishedLogRecords() {
      return this._finishedLogRecords;
    }
    reset() {
      this._finishedLogRecords = [];
    }
  }
  exports.InMemoryLogRecordExporter = InMemoryLogRecordExporter;
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/export/BatchLogRecordProcessorBase.js
var require_BatchLogRecordProcessorBase = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.BatchLogRecordProcessorBase = undefined;
  var api_1 = require_src();
  var core_1 = require_src5();
  var LogRecordProcessorMetrics_1 = require_LogRecordProcessorMetrics();
  var semconv_1 = require_semconv11();
  async function waitForResources(logRecords) {
    const pendingResources = [];
    for (let i = 0, len = logRecords.length;i < len; i++) {
      const logRecord = logRecords[i];
      if (logRecord.resource.asyncAttributesPending && logRecord.resource.waitForAsyncAttributes) {
        pendingResources.push(logRecord.resource.waitForAsyncAttributes());
      }
    }
    if (pendingResources != null && pendingResources.length > 0) {
      await Promise.all(pendingResources);
    }
  }

  class ExportOperation {
    _exportCompleted;
    _exportScheduledPromise;
    _metrics;
    _exportScheduledResolve;
    constructor(exporter, logRecords, exportTimeoutMillis, metrics) {
      this._exportScheduledPromise = new Promise((resolve) => {
        this._exportScheduledResolve = resolve;
      });
      this._exportCompleted = this._executeExport(exporter, logRecords, exportTimeoutMillis);
      this._metrics = metrics;
    }
    get exportCompleted() {
      return this._exportCompleted;
    }
    get exportScheduled() {
      return this._exportScheduledPromise;
    }
    async _executeExport(exporter, logRecords, exportTimeoutMillis) {
      try {
        await waitForResources(logRecords);
        await api_1.context.with((0, core_1.suppressTracing)(api_1.context.active()), async () => {
          return this._exportWithTimeout(exporter, logRecords, exportTimeoutMillis);
        });
      } catch (e) {
        (0, core_1.globalErrorHandler)(e);
        this._exportScheduledResolve();
      }
    }
    async _exportWithTimeout(exporter, logRecords, exportTimeoutMillis) {
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          reject(new Error("Timeout"));
        }, exportTimeoutMillis);
        exporter.export(logRecords, (result) => {
          this._metrics.finishLogs(logRecords.length, result.error);
          clearTimeout(timer);
          if (result.code === core_1.ExportResultCode.SUCCESS) {
            resolve();
          } else {
            reject(result.error ?? new Error("BatchLogRecordProcessor: log record export failed"));
          }
        });
        this._exportScheduledResolve();
      });
    }
  }

  class BatchLogRecordProcessorBase {
    _maxExportBatchSize;
    _maxQueueSize;
    _scheduledDelayMillis;
    _exportTimeoutMillis;
    _exporter;
    _metrics;
    _currentExport = null;
    _finishedLogRecords = [];
    _timer;
    _shutdownOnce;
    _flushing = false;
    constructor(options) {
      this._exporter = options.exporter;
      this._maxExportBatchSize = options.maxExportBatchSize ?? 512;
      this._maxQueueSize = options.maxQueueSize ?? 2048;
      this._scheduledDelayMillis = options.scheduledDelayMillis ?? 1000;
      this._exportTimeoutMillis = options.exportTimeoutMillis ?? 30000;
      this._shutdownOnce = new core_1.BindOnceFuture(this._shutdown, this);
      if (this._maxExportBatchSize > this._maxQueueSize) {
        api_1.diag.warn("BatchLogRecordProcessor: maxExportBatchSize must be smaller or equal to maxQueueSize, setting maxExportBatchSize to match maxQueueSize");
        this._maxExportBatchSize = this._maxQueueSize;
      }
      const meter = options?.selfObsMeterProvider ? options.selfObsMeterProvider.getMeter("@opentelemetry/sdk-logs") : (0, api_1.createNoopMeter)();
      this._metrics = new LogRecordProcessorMetrics_1.LogRecordProcessorMetrics(semconv_1.OTEL_COMPONENT_TYPE_VALUE_BATCHING_LOG_PROCESSOR, meter, {
        capacity: this._maxQueueSize,
        getQueueSize: () => this._finishedLogRecords.length
      });
    }
    onEmit(logRecord) {
      if (this._shutdownOnce.isCalled) {
        return;
      }
      this._addToBuffer(logRecord);
    }
    forceFlush() {
      if (this._shutdownOnce.isCalled) {
        return this._shutdownOnce.promise;
      }
      return this._flushAll();
    }
    _addToBuffer(logRecord) {
      if (this._finishedLogRecords.length >= this._maxQueueSize) {
        this._metrics.dropLogs(1);
        return;
      }
      this._finishedLogRecords.push(logRecord);
      this._maybeStartTimer();
    }
    shutdown() {
      return this._shutdownOnce.call();
    }
    async _shutdown() {
      this.onShutdown();
      await this._flushAll();
      this._metrics.shutdown();
      await this._exporter.shutdown();
    }
    async _flushAll() {
      if (this._flushing) {
        return;
      }
      this._flushing = true;
      let toFlush = this._finishedLogRecords;
      this._finishedLogRecords = [];
      this._clearTimer();
      const inFlight = this._currentExport;
      if (inFlight !== null) {
        await this._exporter.forceFlush();
        await inFlight.exportCompleted;
        this._currentExport = null;
      }
      while (toFlush.length > 0) {
        let batch;
        if (toFlush.length <= this._maxExportBatchSize) {
          batch = toFlush;
          toFlush = [];
        } else {
          batch = toFlush.splice(0, this._maxExportBatchSize);
        }
        const exportOp = new ExportOperation(this._exporter, batch, this._exportTimeoutMillis, this._metrics);
        this._currentExport = exportOp;
        try {
          await exportOp.exportScheduled;
          await this._exporter.forceFlush();
          await exportOp.exportCompleted;
        } catch (e) {
          (0, core_1.globalErrorHandler)(e);
        } finally {
          this._currentExport = null;
        }
      }
      this._flushing = false;
      this._maybeStartTimer();
    }
    _extractBatch() {
      if (this._finishedLogRecords.length === 0) {
        return null;
      }
      if (this._finishedLogRecords.length <= this._maxExportBatchSize) {
        const batch = this._finishedLogRecords;
        this._finishedLogRecords = [];
        return batch;
      } else {
        return this._finishedLogRecords.splice(0, this._maxExportBatchSize);
      }
    }
    _exportOneBatch() {
      this._clearTimer();
      const logRecords = this._extractBatch();
      if (logRecords === null) {
        return;
      }
      const exportOp = new ExportOperation(this._exporter, logRecords, this._exportTimeoutMillis, this._metrics);
      this._currentExport = exportOp;
      exportOp.exportCompleted.then(() => {
        this._currentExport = null;
        this._maybeStartTimer();
      }).catch((error) => {
        this._currentExport = null;
        (0, core_1.globalErrorHandler)(error);
        this._maybeStartTimer();
      });
    }
    _maybeStartTimer() {
      if (this._shutdownOnce.isCalled) {
        return;
      }
      if (this._flushing) {
        return;
      }
      if (this._finishedLogRecords.length === 0) {
        return;
      }
      if (this._currentExport !== null) {
        return;
      }
      if (this._finishedLogRecords.length >= this._maxExportBatchSize) {
        this._exportOneBatch();
        return;
      }
      if (this._timer !== undefined) {
        return;
      }
      this._timer = setTimeout(() => {
        this._timer = undefined;
        this._exportOneBatch();
      }, this._scheduledDelayMillis);
      if (typeof this._timer !== "number") {
        this._timer.unref();
      }
    }
    _clearTimer() {
      if (this._timer !== undefined) {
        clearTimeout(this._timer);
        this._timer = undefined;
      }
    }
  }
  exports.BatchLogRecordProcessorBase = BatchLogRecordProcessorBase;
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/platform/node/export/BatchLogRecordProcessor.js
var require_BatchLogRecordProcessor = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.BatchLogRecordProcessor = undefined;
  var BatchLogRecordProcessorBase_1 = require_BatchLogRecordProcessorBase();

  class BatchLogRecordProcessor extends BatchLogRecordProcessorBase_1.BatchLogRecordProcessorBase {
    onShutdown() {}
  }
  exports.BatchLogRecordProcessor = BatchLogRecordProcessor;
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/platform/node/index.js
var require_node9 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.BatchLogRecordProcessor = undefined;
  var BatchLogRecordProcessor_1 = require_BatchLogRecordProcessor();
  Object.defineProperty(exports, "BatchLogRecordProcessor", { enumerable: true, get: function() {
    return BatchLogRecordProcessor_1.BatchLogRecordProcessor;
  } });
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/platform/index.js
var require_platform9 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.BatchLogRecordProcessor = undefined;
  var node_1 = require_node9();
  Object.defineProperty(exports, "BatchLogRecordProcessor", { enumerable: true, get: function() {
    return node_1.BatchLogRecordProcessor;
  } });
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/config/LoggerConfigurators.js
var require_LoggerConfigurators = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.createLoggerConfigurator = undefined;
  var api_logs_1 = require_src2();
  var DEFAULT_LOGGER_CONFIG = {
    disabled: false,
    minimumSeverity: api_logs_1.SeverityNumber.UNSPECIFIED,
    traceBased: false
  };
  function createLoggerConfigurator(patterns) {
    return (loggerScope) => {
      const loggerName = loggerScope.name;
      for (const { pattern, config } of patterns) {
        if (matchesPattern(loggerName, pattern)) {
          return {
            disabled: config.disabled ?? DEFAULT_LOGGER_CONFIG.disabled,
            minimumSeverity: config.minimumSeverity ?? DEFAULT_LOGGER_CONFIG.minimumSeverity,
            traceBased: config.traceBased ?? DEFAULT_LOGGER_CONFIG.traceBased
          };
        }
      }
      return { ...DEFAULT_LOGGER_CONFIG };
    };
  }
  exports.createLoggerConfigurator = createLoggerConfigurator;
  function matchesPattern(name, pattern) {
    if (pattern === name) {
      return true;
    }
    if (pattern.includes("*")) {
      const regexPattern = pattern.split("*").map((part) => part.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join(".*");
      const regex = new RegExp(`^${regexPattern}$`);
      return regex.test(name);
    }
    return false;
  }
});

// node_modules/.bun/@opentelemetry+sdk-logs@0.222.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-logs/build/src/index.js
var require_src16 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.createLoggerConfigurator = exports.BatchLogRecordProcessor = exports.InMemoryLogRecordExporter = exports.SimpleLogRecordProcessor = exports.ConsoleLogRecordExporter = exports.LoggerProvider = undefined;
  var LoggerProvider_1 = require_LoggerProvider();
  Object.defineProperty(exports, "LoggerProvider", { enumerable: true, get: function() {
    return LoggerProvider_1.LoggerProvider;
  } });
  var ConsoleLogRecordExporter_1 = require_ConsoleLogRecordExporter();
  Object.defineProperty(exports, "ConsoleLogRecordExporter", { enumerable: true, get: function() {
    return ConsoleLogRecordExporter_1.ConsoleLogRecordExporter;
  } });
  var SimpleLogRecordProcessor_1 = require_SimpleLogRecordProcessor();
  Object.defineProperty(exports, "SimpleLogRecordProcessor", { enumerable: true, get: function() {
    return SimpleLogRecordProcessor_1.SimpleLogRecordProcessor;
  } });
  var InMemoryLogRecordExporter_1 = require_InMemoryLogRecordExporter();
  Object.defineProperty(exports, "InMemoryLogRecordExporter", { enumerable: true, get: function() {
    return InMemoryLogRecordExporter_1.InMemoryLogRecordExporter;
  } });
  var platform_1 = require_platform9();
  Object.defineProperty(exports, "BatchLogRecordProcessor", { enumerable: true, get: function() {
    return platform_1.BatchLogRecordProcessor;
  } });
  var LoggerConfigurators_1 = require_LoggerConfigurators();
  Object.defineProperty(exports, "createLoggerConfigurator", { enumerable: true, get: function() {
    return LoggerConfigurators_1.createLoggerConfigurator;
  } });
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/enums.js
var require_enums = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ExceptionEventName = undefined;
  exports.ExceptionEventName = "exception";
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/inspect.js
var require_inspect = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.formatInspect = exports.settledResourceAttributes = exports.inspectCustom = undefined;
  exports.inspectCustom = Symbol.for("nodejs.util.inspect.custom");
  function settledResourceAttributes(resource) {
    const attrs = {};
    for (const [k, v] of resource.getRawAttributes()) {
      if (typeof v?.then === "function") {
        continue;
      }
      if (v != null) {
        attrs[k] ??= v;
      }
    }
    return attrs;
  }
  exports.settledResourceAttributes = settledResourceAttributes;
  function formatInspect(className, payload, depth, options, inspect) {
    if (typeof depth === "number" && depth < 0) {
      const tag = `[${className}]`;
      return options?.stylize ? options.stylize(tag, "special") : tag;
    }
    if (typeof inspect !== "function" || !options) {
      return payload;
    }
    const childOptions = {
      ...options,
      depth: options.depth == null ? options.depth : options.depth - 1
    };
    return `${className} ${inspect(payload, childOptions)}`;
  }
  exports.formatInspect = formatInspect;
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/Span.js
var require_Span = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.SpanImpl = undefined;
  var api_1 = require_src();
  var core_1 = require_src5();
  var semantic_conventions_1 = require_src4();
  var enums_1 = require_enums();
  var inspect_1 = require_inspect();

  class SpanImpl {
    _spanContext;
    kind;
    parentSpanContext;
    attributes = {};
    links = [];
    events = [];
    startTime;
    resource;
    instrumentationScope;
    _droppedAttributesCount = 0;
    _droppedEventsCount = 0;
    _droppedLinksCount = 0;
    _attributesCount = 0;
    name;
    status = {
      code: api_1.SpanStatusCode.UNSET
    };
    endTime = [0, 0];
    _ended = false;
    _duration = [-1, -1];
    _spanProcessor;
    _spanLimits;
    _attributeValueLengthLimit;
    _recordEndMetrics;
    _performanceStartTime;
    _performanceOffset;
    _startTimeProvided;
    constructor(opts) {
      const now = Date.now();
      this._spanContext = opts.spanContext;
      this._performanceStartTime = core_1.otperformance.now();
      this._performanceOffset = now - (this._performanceStartTime + core_1.otperformance.timeOrigin);
      this._startTimeProvided = opts.startTime != null;
      this._spanLimits = opts.spanLimits;
      this._attributeValueLengthLimit = this._spanLimits.attributeValueLengthLimit ?? 0;
      this._spanProcessor = opts.spanProcessor;
      this.name = opts.name;
      this.parentSpanContext = opts.parentSpanContext;
      this.kind = opts.kind;
      if (opts.links) {
        for (const link of opts.links) {
          this.addLink(link);
        }
      }
      this.startTime = this._getTime(opts.startTime ?? now);
      this.resource = opts.resource;
      this.instrumentationScope = opts.scope;
      this._recordEndMetrics = opts.recordEndMetrics;
      if (opts.attributes != null) {
        this.setAttributes(opts.attributes);
      }
      this._spanProcessor.onStart(this, opts.context);
    }
    spanContext() {
      return this._spanContext;
    }
    setAttribute(key, value) {
      if (value == null || this._isSpanEnded())
        return this;
      if (key.length === 0) {
        api_1.diag.warn(`Invalid attribute key: ${key}`);
        return this;
      }
      if (!(0, core_1.isAttributeValue)(value)) {
        api_1.diag.warn(`Invalid attribute value set for key: ${key}`);
        return this;
      }
      const { attributeCountLimit } = this._spanLimits;
      const isNewKey = !Object.prototype.hasOwnProperty.call(this.attributes, key);
      if (attributeCountLimit !== undefined && this._attributesCount >= attributeCountLimit && isNewKey) {
        this._droppedAttributesCount++;
        return this;
      }
      this.attributes[key] = this._truncateToSize(value);
      if (isNewKey) {
        this._attributesCount++;
      }
      return this;
    }
    setAttributes(attributes) {
      for (const key in attributes) {
        if (Object.prototype.hasOwnProperty.call(attributes, key)) {
          this.setAttribute(key, attributes[key]);
        }
      }
      return this;
    }
    addEvent(name, attributesOrStartTime, timeStamp) {
      if (this._isSpanEnded())
        return this;
      const { eventCountLimit } = this._spanLimits;
      if (eventCountLimit === 0) {
        api_1.diag.warn("No events allowed.");
        this._droppedEventsCount++;
        return this;
      }
      if (eventCountLimit !== undefined && this.events.length >= eventCountLimit) {
        if (this._droppedEventsCount === 0) {
          api_1.diag.debug("Dropping extra events.");
        }
        this.events.shift();
        this._droppedEventsCount++;
      }
      if ((0, core_1.isTimeInput)(attributesOrStartTime)) {
        if (!(0, core_1.isTimeInput)(timeStamp)) {
          timeStamp = attributesOrStartTime;
        }
        attributesOrStartTime = undefined;
      }
      const sanitized = (0, core_1.sanitizeAttributes)(attributesOrStartTime);
      const { attributePerEventCountLimit } = this._spanLimits;
      const attributes = {};
      let droppedAttributesCount = 0;
      let eventAttributesCount = 0;
      for (const attr in sanitized) {
        if (!Object.prototype.hasOwnProperty.call(sanitized, attr)) {
          continue;
        }
        const attrVal = sanitized[attr];
        if (attributePerEventCountLimit !== undefined && eventAttributesCount >= attributePerEventCountLimit) {
          droppedAttributesCount++;
          continue;
        }
        attributes[attr] = this._truncateToSize(attrVal);
        eventAttributesCount++;
      }
      this.events.push({
        name,
        attributes,
        time: this._getTime(timeStamp),
        droppedAttributesCount
      });
      return this;
    }
    addLink(link) {
      if (this._isSpanEnded())
        return this;
      const { linkCountLimit } = this._spanLimits;
      if (linkCountLimit === 0) {
        this._droppedLinksCount++;
        return this;
      }
      if (linkCountLimit !== undefined && this.links.length >= linkCountLimit) {
        if (this._droppedLinksCount === 0) {
          api_1.diag.debug("Dropping extra links.");
        }
        this.links.shift();
        this._droppedLinksCount++;
      }
      const { attributePerLinkCountLimit } = this._spanLimits;
      const sanitized = (0, core_1.sanitizeAttributes)(link.attributes);
      const attributes = {};
      let droppedAttributesCount = 0;
      let linkAttributesCount = 0;
      for (const attr in sanitized) {
        if (!Object.prototype.hasOwnProperty.call(sanitized, attr)) {
          continue;
        }
        const attrVal = sanitized[attr];
        if (attributePerLinkCountLimit !== undefined && linkAttributesCount >= attributePerLinkCountLimit) {
          droppedAttributesCount++;
          continue;
        }
        attributes[attr] = this._truncateToSize(attrVal);
        linkAttributesCount++;
      }
      const processedLink = { context: link.context };
      if (linkAttributesCount > 0) {
        processedLink.attributes = attributes;
      }
      if (droppedAttributesCount > 0) {
        processedLink.droppedAttributesCount = droppedAttributesCount;
      }
      this.links.push(processedLink);
      return this;
    }
    addLinks(links) {
      for (const link of links) {
        this.addLink(link);
      }
      return this;
    }
    setStatus(status) {
      if (this._isSpanEnded())
        return this;
      if (status.code === api_1.SpanStatusCode.UNSET)
        return this;
      if (this.status.code === api_1.SpanStatusCode.OK)
        return this;
      const newStatus = { code: status.code };
      if (status.code === api_1.SpanStatusCode.ERROR) {
        if (typeof status.message === "string") {
          newStatus.message = status.message;
        } else if (status.message != null) {
          api_1.diag.warn(`Dropping invalid status.message of type '${typeof status.message}', expected 'string'`);
        }
      }
      this.status = newStatus;
      return this;
    }
    updateName(name) {
      if (this._isSpanEnded())
        return this;
      this.name = name;
      return this;
    }
    end(endTime) {
      if (this._isSpanEnded()) {
        api_1.diag.error(`${this.name} ${this._spanContext.traceId}-${this._spanContext.spanId} - You can only call end() on a span once.`);
        return;
      }
      this.endTime = this._getTime(endTime);
      this._duration = (0, core_1.hrTimeDuration)(this.startTime, this.endTime);
      if (this._duration[0] < 0) {
        api_1.diag.warn("Inconsistent start and end time, startTime > endTime. Setting span duration to 0ms.", this.startTime, this.endTime);
        this.endTime = this.startTime.slice();
        this._duration = [0, 0];
      }
      if (this._droppedEventsCount > 0) {
        api_1.diag.warn(`Dropped ${this._droppedEventsCount} events because eventCountLimit reached`);
      }
      if (this._droppedLinksCount > 0) {
        api_1.diag.warn(`Dropped ${this._droppedLinksCount} links because linkCountLimit reached`);
      }
      if (this._spanProcessor.onEnding) {
        this._spanProcessor.onEnding(this);
      }
      this._recordEndMetrics?.();
      this._ended = true;
      this._spanProcessor.onEnd(this);
    }
    _getTime(inp) {
      if (typeof inp === "number" && inp <= core_1.otperformance.now()) {
        return (0, core_1.hrTime)(inp + this._performanceOffset);
      }
      if (typeof inp === "number") {
        return (0, core_1.millisToHrTime)(inp);
      }
      if (inp instanceof Date) {
        return (0, core_1.millisToHrTime)(inp.getTime());
      }
      if ((0, core_1.isTimeInputHrTime)(inp)) {
        return inp;
      }
      if (this._startTimeProvided) {
        return (0, core_1.millisToHrTime)(Date.now());
      }
      const msDuration = core_1.otperformance.now() - this._performanceStartTime;
      return (0, core_1.addHrTimes)(this.startTime, (0, core_1.millisToHrTime)(msDuration));
    }
    isRecording() {
      return this._ended === false;
    }
    recordException(exception, time) {
      const attributes = {};
      if (typeof exception === "string") {
        attributes[semantic_conventions_1.ATTR_EXCEPTION_MESSAGE] = exception;
      } else if (exception) {
        if (exception.code) {
          attributes[semantic_conventions_1.ATTR_EXCEPTION_TYPE] = exception.code.toString();
        } else if (exception.name) {
          attributes[semantic_conventions_1.ATTR_EXCEPTION_TYPE] = exception.name;
        }
        if (exception.message) {
          attributes[semantic_conventions_1.ATTR_EXCEPTION_MESSAGE] = exception.message;
        }
        if (exception.stack) {
          attributes[semantic_conventions_1.ATTR_EXCEPTION_STACKTRACE] = exception.stack;
        }
      }
      if (attributes[semantic_conventions_1.ATTR_EXCEPTION_TYPE] || attributes[semantic_conventions_1.ATTR_EXCEPTION_MESSAGE]) {
        this.addEvent(enums_1.ExceptionEventName, attributes, time);
      } else {
        api_1.diag.warn(`Failed to record an exception ${exception}`);
      }
    }
    get duration() {
      return this._duration;
    }
    get ended() {
      return this._ended;
    }
    get droppedAttributesCount() {
      return this._droppedAttributesCount;
    }
    get droppedEventsCount() {
      return this._droppedEventsCount;
    }
    get droppedLinksCount() {
      return this._droppedLinksCount;
    }
    _isSpanEnded() {
      if (this._ended) {
        const error = new Error(`Operation attempted on ended Span {traceId: ${this._spanContext.traceId}, spanId: ${this._spanContext.spanId}}`);
        api_1.diag.warn(`Cannot execute the operation on ended Span {traceId: ${this._spanContext.traceId}, spanId: ${this._spanContext.spanId}}`, error);
      }
      return this._ended;
    }
    _truncateToLimitUtil(value, limit) {
      if (value.length <= limit) {
        return value;
      }
      return value.substring(0, limit);
    }
    _truncateToSize(value) {
      const limit = this._attributeValueLengthLimit;
      if (limit <= 0) {
        api_1.diag.warn(`Attribute value limit must be positive, got ${limit}`);
        return value;
      }
      if (typeof value === "string") {
        return this._truncateToLimitUtil(value, limit);
      }
      if (Array.isArray(value)) {
        return value.map((val) => typeof val === "string" ? this._truncateToLimitUtil(val, limit) : val);
      }
      return value;
    }
    [inspect_1.inspectCustom](depth, options, inspect) {
      const payload = {
        name: this.name,
        kind: this.kind,
        spanContext: this._spanContext,
        parentSpanContext: this.parentSpanContext,
        status: this.status,
        startTime: this.startTime,
        endTime: this.endTime,
        duration: this._duration,
        ended: this._ended,
        attributes: this.attributes,
        events: this.events,
        links: this.links,
        droppedAttributesCount: this._droppedAttributesCount,
        droppedEventsCount: this._droppedEventsCount,
        droppedLinksCount: this._droppedLinksCount,
        instrumentationScope: this.instrumentationScope,
        resource: { attributes: (0, inspect_1.settledResourceAttributes)(this.resource) }
      };
      return (0, inspect_1.formatInspect)("SpanImpl", payload, depth, options, inspect);
    }
  }
  exports.SpanImpl = SpanImpl;
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/Sampler.js
var require_Sampler = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.SamplingDecision = undefined;
  var SamplingDecision;
  (function(SamplingDecision) {
    SamplingDecision[SamplingDecision["NOT_RECORD"] = 0] = "NOT_RECORD";
    SamplingDecision[SamplingDecision["RECORD"] = 1] = "RECORD";
    SamplingDecision[SamplingDecision["RECORD_AND_SAMPLED"] = 2] = "RECORD_AND_SAMPLED";
  })(SamplingDecision || (exports.SamplingDecision = SamplingDecision = {}));
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/semconv.js
var require_semconv12 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.OTEL_COMPONENT_TYPE_VALUE_SIMPLE_SPAN_PROCESSOR = exports.OTEL_COMPONENT_TYPE_VALUE_BATCHING_SPAN_PROCESSOR = exports.METRIC_OTEL_SDK_SPAN_STARTED = exports.METRIC_OTEL_SDK_SPAN_LIVE = exports.METRIC_OTEL_SDK_PROCESSOR_SPAN_QUEUE_SIZE = exports.METRIC_OTEL_SDK_PROCESSOR_SPAN_QUEUE_CAPACITY = exports.METRIC_OTEL_SDK_PROCESSOR_SPAN_PROCESSED = exports.ATTR_OTEL_SPAN_SAMPLING_RESULT = exports.ATTR_OTEL_SPAN_PARENT_ORIGIN = exports.ATTR_OTEL_COMPONENT_TYPE = exports.ATTR_OTEL_COMPONENT_NAME = undefined;
  exports.ATTR_OTEL_COMPONENT_NAME = "otel.component.name";
  exports.ATTR_OTEL_COMPONENT_TYPE = "otel.component.type";
  exports.ATTR_OTEL_SPAN_PARENT_ORIGIN = "otel.span.parent.origin";
  exports.ATTR_OTEL_SPAN_SAMPLING_RESULT = "otel.span.sampling_result";
  exports.METRIC_OTEL_SDK_PROCESSOR_SPAN_PROCESSED = "otel.sdk.processor.span.processed";
  exports.METRIC_OTEL_SDK_PROCESSOR_SPAN_QUEUE_CAPACITY = "otel.sdk.processor.span.queue.capacity";
  exports.METRIC_OTEL_SDK_PROCESSOR_SPAN_QUEUE_SIZE = "otel.sdk.processor.span.queue.size";
  exports.METRIC_OTEL_SDK_SPAN_LIVE = "otel.sdk.span.live";
  exports.METRIC_OTEL_SDK_SPAN_STARTED = "otel.sdk.span.started";
  exports.OTEL_COMPONENT_TYPE_VALUE_BATCHING_SPAN_PROCESSOR = "batching_span_processor";
  exports.OTEL_COMPONENT_TYPE_VALUE_SIMPLE_SPAN_PROCESSOR = "simple_span_processor";
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/TracerMetrics.js
var require_TracerMetrics = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.TracerMetrics = undefined;
  var Sampler_1 = require_Sampler();
  var semconv_1 = require_semconv12();

  class TracerMetrics {
    startedSpans;
    liveSpans;
    constructor(meter) {
      this.startedSpans = meter.createCounter(semconv_1.METRIC_OTEL_SDK_SPAN_STARTED, {
        unit: "{span}",
        description: "The number of created spans."
      });
      this.liveSpans = meter.createUpDownCounter(semconv_1.METRIC_OTEL_SDK_SPAN_LIVE, {
        unit: "{span}",
        description: "The number of currently live spans."
      });
    }
    startSpan(parentSpanCtx, samplingDecision) {
      const samplingDecisionStr = samplingDecisionToString(samplingDecision);
      this.startedSpans.add(1, {
        [semconv_1.ATTR_OTEL_SPAN_PARENT_ORIGIN]: parentOrigin(parentSpanCtx),
        [semconv_1.ATTR_OTEL_SPAN_SAMPLING_RESULT]: samplingDecisionStr
      });
      if (samplingDecision === Sampler_1.SamplingDecision.NOT_RECORD) {
        return () => {};
      }
      const liveSpanAttributes = {
        [semconv_1.ATTR_OTEL_SPAN_SAMPLING_RESULT]: samplingDecisionStr
      };
      this.liveSpans.add(1, liveSpanAttributes);
      return () => {
        this.liveSpans.add(-1, liveSpanAttributes);
      };
    }
  }
  exports.TracerMetrics = TracerMetrics;
  function parentOrigin(parentSpanContext) {
    if (!parentSpanContext) {
      return "none";
    }
    if (parentSpanContext.isRemote) {
      return "remote";
    }
    return "local";
  }
  function samplingDecisionToString(decision) {
    switch (decision) {
      case Sampler_1.SamplingDecision.RECORD_AND_SAMPLED:
        return "RECORD_AND_SAMPLE";
      case Sampler_1.SamplingDecision.RECORD:
        return "RECORD_ONLY";
      case Sampler_1.SamplingDecision.NOT_RECORD:
        return "DROP";
    }
  }
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/version.js
var require_version5 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.VERSION = undefined;
  exports.VERSION = "2.11.0";
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/Tracer.js
var require_Tracer = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.Tracer = undefined;
  var api = require_src();
  var core_1 = require_src5();
  var Span_1 = require_Span();
  var TracerMetrics_1 = require_TracerMetrics();
  var version_1 = require_version5();
  var inspect_1 = require_inspect();

  class Tracer {
    _sampler;
    _spanLimits;
    _idGenerator;
    instrumentationScope;
    _resource;
    _spanProcessor;
    _tracerMetrics;
    constructor(instrumentationScope, options) {
      this.instrumentationScope = instrumentationScope;
      this._sampler = options.sampler;
      this._spanLimits = options.spanLimits;
      this._resource = options.resource;
      this._idGenerator = options.idGenerator;
      this._spanProcessor = options.spanProcessor;
      const meter = options.meterProvider.getMeter("@opentelemetry/sdk-trace", version_1.VERSION);
      this._tracerMetrics = new TracerMetrics_1.TracerMetrics(meter);
    }
    startSpan(name, options = {}, context2 = api.context.active()) {
      if (options.root) {
        context2 = api.trace.deleteSpan(context2);
      }
      const parentSpan = api.trace.getSpan(context2);
      if ((0, core_1.isTracingSuppressed)(context2)) {
        api.diag.debug("Instrumentation suppressed, returning Noop Span");
        const nonRecordingSpan = api.trace.wrapSpanContext(api.INVALID_SPAN_CONTEXT);
        return nonRecordingSpan;
      }
      const parentSpanContext = parentSpan?.spanContext();
      const spanId = this._idGenerator.generateSpanId();
      let validParentSpanContext;
      let traceId;
      let traceState;
      if (!parentSpanContext || !api.trace.isSpanContextValid(parentSpanContext)) {
        traceId = this._idGenerator.generateTraceId();
      } else {
        traceId = parentSpanContext.traceId;
        traceState = parentSpanContext.traceState;
        validParentSpanContext = parentSpanContext;
      }
      const spanKind = options.kind ?? api.SpanKind.INTERNAL;
      const links = (options.links ?? []).map((link) => {
        return {
          context: link.context,
          attributes: (0, core_1.sanitizeAttributes)(link.attributes)
        };
      });
      const attributes = (0, core_1.sanitizeAttributes)(options.attributes);
      const samplingResult = this._sampler.shouldSample(context2, traceId, name, spanKind, attributes, links);
      const recordEndMetrics = this._tracerMetrics.startSpan(parentSpanContext, samplingResult.decision);
      traceState = samplingResult.traceState ?? traceState;
      const traceFlags = samplingResult.decision === api.SamplingDecision.RECORD_AND_SAMPLED ? api.TraceFlags.SAMPLED : api.TraceFlags.NONE;
      const spanContext = { traceId, spanId, traceFlags, traceState };
      if (samplingResult.decision === api.SamplingDecision.NOT_RECORD) {
        api.diag.debug("Recording is off, propagating context in a non-recording span");
        const nonRecordingSpan = api.trace.wrapSpanContext(spanContext);
        return nonRecordingSpan;
      }
      const initAttributes = (0, core_1.sanitizeAttributes)(Object.assign(attributes, samplingResult.attributes));
      const span = new Span_1.SpanImpl({
        resource: this._resource,
        scope: this.instrumentationScope,
        context: context2,
        spanContext,
        name,
        kind: spanKind,
        links,
        parentSpanContext: validParentSpanContext,
        attributes: initAttributes,
        startTime: options.startTime,
        spanProcessor: this._spanProcessor,
        spanLimits: this._spanLimits,
        recordEndMetrics
      });
      return span;
    }
    startActiveSpan(name, arg2, arg3, arg4) {
      let opts;
      let ctx;
      let fn;
      if (arguments.length < 2) {
        return;
      } else if (arguments.length === 2) {
        fn = arg2;
      } else if (arguments.length === 3) {
        opts = arg2;
        fn = arg3;
      } else {
        opts = arg2;
        ctx = arg3;
        fn = arg4;
      }
      const parentContext = ctx ?? api.context.active();
      const span = this.startSpan(name, opts, parentContext);
      const contextWithSpanSet = api.trace.setSpan(parentContext, span);
      return api.context.with(contextWithSpanSet, fn, undefined, span);
    }
    [inspect_1.inspectCustom](depth, options, inspect) {
      const payload = {
        instrumentationScope: this.instrumentationScope,
        resource: { attributes: (0, inspect_1.settledResourceAttributes)(this._resource) },
        spanLimits: this._spanLimits
      };
      return (0, inspect_1.formatInspect)("Tracer", payload, depth, options, inspect);
    }
  }
  exports.Tracer = Tracer;
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/MultiSpanProcessor.js
var require_MultiSpanProcessor = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.MultiSpanProcessor = undefined;
  var core_1 = require_src5();

  class MultiSpanProcessor {
    _spanProcessors;
    constructor(spanProcessors) {
      this._spanProcessors = spanProcessors;
    }
    forceFlush() {
      const promises = [];
      for (const spanProcessor of this._spanProcessors) {
        promises.push(spanProcessor.forceFlush());
      }
      return new Promise((resolve) => {
        Promise.all(promises).then(() => {
          resolve();
        }).catch((error) => {
          (0, core_1.globalErrorHandler)(error || new Error("MultiSpanProcessor: forceFlush failed"));
          resolve();
        });
      });
    }
    onStart(span, context) {
      for (const spanProcessor of this._spanProcessors) {
        spanProcessor.onStart(span, context);
      }
    }
    onEnding(span) {
      for (const spanProcessor of this._spanProcessors) {
        if (spanProcessor.onEnding) {
          spanProcessor.onEnding(span);
        }
      }
    }
    onEnd(span) {
      for (const spanProcessor of this._spanProcessors) {
        spanProcessor.onEnd(span);
      }
    }
    shutdown() {
      const promises = [];
      for (const spanProcessor of this._spanProcessors) {
        promises.push(spanProcessor.shutdown());
      }
      return new Promise((resolve, reject) => {
        Promise.all(promises).then(() => {
          resolve();
        }, reject);
      });
    }
  }
  exports.MultiSpanProcessor = MultiSpanProcessor;
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/sampler/AlwaysOffSampler.js
var require_AlwaysOffSampler = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.AlwaysOffSampler = undefined;
  var Sampler_1 = require_Sampler();

  class AlwaysOffSampler {
    shouldSample() {
      return {
        decision: Sampler_1.SamplingDecision.NOT_RECORD
      };
    }
    toString() {
      return "AlwaysOffSampler";
    }
  }
  exports.AlwaysOffSampler = AlwaysOffSampler;
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/sampler/AlwaysOnSampler.js
var require_AlwaysOnSampler = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.AlwaysOnSampler = undefined;
  var Sampler_1 = require_Sampler();

  class AlwaysOnSampler {
    shouldSample() {
      return {
        decision: Sampler_1.SamplingDecision.RECORD_AND_SAMPLED
      };
    }
    toString() {
      return "AlwaysOnSampler";
    }
  }
  exports.AlwaysOnSampler = AlwaysOnSampler;
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/sampler/ParentBasedSampler.js
var require_ParentBasedSampler = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ParentBasedSampler = undefined;
  var api_1 = require_src();
  var core_1 = require_src5();
  var AlwaysOffSampler_1 = require_AlwaysOffSampler();
  var AlwaysOnSampler_1 = require_AlwaysOnSampler();

  class ParentBasedSampler {
    _root;
    _remoteParentSampled;
    _remoteParentNotSampled;
    _localParentSampled;
    _localParentNotSampled;
    constructor(config) {
      this._root = config.root;
      if (!this._root) {
        (0, core_1.globalErrorHandler)(new Error("ParentBasedSampler must have a root sampler configured"));
        this._root = new AlwaysOnSampler_1.AlwaysOnSampler;
      }
      this._remoteParentSampled = config.remoteParentSampled ?? new AlwaysOnSampler_1.AlwaysOnSampler;
      this._remoteParentNotSampled = config.remoteParentNotSampled ?? new AlwaysOffSampler_1.AlwaysOffSampler;
      this._localParentSampled = config.localParentSampled ?? new AlwaysOnSampler_1.AlwaysOnSampler;
      this._localParentNotSampled = config.localParentNotSampled ?? new AlwaysOffSampler_1.AlwaysOffSampler;
    }
    shouldSample(context, traceId, spanName, spanKind, attributes, links) {
      const parentContext = api_1.trace.getSpanContext(context);
      if (!parentContext || !(0, api_1.isSpanContextValid)(parentContext)) {
        return this._root.shouldSample(context, traceId, spanName, spanKind, attributes, links);
      }
      if (parentContext.isRemote) {
        if (parentContext.traceFlags & api_1.TraceFlags.SAMPLED) {
          return this._remoteParentSampled.shouldSample(context, traceId, spanName, spanKind, attributes, links);
        }
        return this._remoteParentNotSampled.shouldSample(context, traceId, spanName, spanKind, attributes, links);
      }
      if (parentContext.traceFlags & api_1.TraceFlags.SAMPLED) {
        return this._localParentSampled.shouldSample(context, traceId, spanName, spanKind, attributes, links);
      }
      return this._localParentNotSampled.shouldSample(context, traceId, spanName, spanKind, attributes, links);
    }
    toString() {
      return `ParentBased{root=${this._root.toString()}, remoteParentSampled=${this._remoteParentSampled.toString()}, remoteParentNotSampled=${this._remoteParentNotSampled.toString()}, localParentSampled=${this._localParentSampled.toString()}, localParentNotSampled=${this._localParentNotSampled.toString()}}`;
    }
  }
  exports.ParentBasedSampler = ParentBasedSampler;
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/export/SpanProcessorMetrics.js
var require_SpanProcessorMetrics = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.SpanProcessorMetrics = undefined;
  var semantic_conventions_1 = require_src4();
  var semconv_1 = require_semconv12();
  var componentCounter = new Map;

  class SpanProcessorMetrics {
    processedSpans;
    queueSize;
    queueSizeCallback;
    standardAttrs;
    droppedAttrs;
    constructor(componentType, meter, queueConfig) {
      const counter = componentCounter.get(componentType) ?? 0;
      componentCounter.set(componentType, counter + 1);
      this.standardAttrs = {
        [semconv_1.ATTR_OTEL_COMPONENT_TYPE]: componentType,
        [semconv_1.ATTR_OTEL_COMPONENT_NAME]: `${componentType}/${counter}`
      };
      this.droppedAttrs = {
        ...this.standardAttrs,
        [semantic_conventions_1.ATTR_ERROR_TYPE]: "queue_full"
      };
      this.processedSpans = meter.createCounter(semconv_1.METRIC_OTEL_SDK_PROCESSOR_SPAN_PROCESSED, {
        unit: "{span}",
        description: "The number of spans for which the processing has finished, either successful or failed."
      });
      if (queueConfig) {
        const { capacity, getQueueSize } = queueConfig;
        const queueCapacity = meter.createUpDownCounter(semconv_1.METRIC_OTEL_SDK_PROCESSOR_SPAN_QUEUE_CAPACITY, {
          unit: "{span}",
          description: "The maximum number of spans the queue of a given instance of an SDK span processor can hold."
        });
        queueCapacity.add(capacity, this.standardAttrs);
        this.queueSize = meter.createObservableUpDownCounter(semconv_1.METRIC_OTEL_SDK_PROCESSOR_SPAN_QUEUE_SIZE, {
          unit: "{span}",
          description: "The number of spans in the queue of a given instance of an SDK span processor."
        });
        this.queueSizeCallback = (result) => result.observe(getQueueSize(), this.standardAttrs);
        this.queueSize.addCallback(this.queueSizeCallback);
      }
    }
    dropSpans(count) {
      this.processedSpans.add(count, this.droppedAttrs);
    }
    finishSpans(count, error) {
      if (!error) {
        this.processedSpans.add(count, this.standardAttrs);
        return;
      }
      const attrs = {
        ...this.standardAttrs,
        [semantic_conventions_1.ATTR_ERROR_TYPE]: error.name
      };
      this.processedSpans.add(count, attrs);
    }
    shutdown() {
      if (this.queueSize && this.queueSizeCallback) {
        this.queueSize.removeCallback(this.queueSizeCallback);
      }
    }
  }
  exports.SpanProcessorMetrics = SpanProcessorMetrics;
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/export/BatchSpanProcessorBase.js
var require_BatchSpanProcessorBase = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.BatchSpanProcessorBase = undefined;
  var api_1 = require_src();
  var core_1 = require_src5();
  var SpanProcessorMetrics_1 = require_SpanProcessorMetrics();
  var semconv_1 = require_semconv12();

  class BatchSpanProcessorBase {
    _maxExportBatchSize;
    _maxQueueSize;
    _scheduledDelayMillis;
    _exportTimeoutMillis;
    _exporter;
    _metrics;
    _isExporting = false;
    _finishedSpans = [];
    _timer;
    _shutdownOnce;
    _droppedSpansCount = 0;
    constructor(options) {
      this._exporter = options.exporter;
      this._maxExportBatchSize = options.maxExportBatchSize ?? 512;
      this._maxQueueSize = options.maxQueueSize ?? 2048;
      this._scheduledDelayMillis = options.scheduledDelayMillis ?? 5000;
      this._exportTimeoutMillis = options.exportTimeoutMillis ?? 30000;
      this._shutdownOnce = new core_1.BindOnceFuture(this._shutdown, this);
      if (this._maxExportBatchSize > this._maxQueueSize) {
        api_1.diag.warn("BatchSpanProcessor: maxExportBatchSize must be smaller or equal to maxQueueSize, setting maxExportBatchSize to match maxQueueSize");
        this._maxExportBatchSize = this._maxQueueSize;
      }
      const meter = options.selfObsMeterProvider ? options.selfObsMeterProvider.getMeter("@opentelemetry/sdk-trace") : (0, api_1.createNoopMeter)();
      this._metrics = new SpanProcessorMetrics_1.SpanProcessorMetrics(semconv_1.OTEL_COMPONENT_TYPE_VALUE_BATCHING_SPAN_PROCESSOR, meter, {
        capacity: this._maxQueueSize,
        getQueueSize: () => this._finishedSpans.length
      });
    }
    forceFlush() {
      if (this._shutdownOnce.isCalled) {
        return this._shutdownOnce.promise;
      }
      return this._flushAll();
    }
    onStart(_span, _parentContext) {}
    onEnd(span) {
      if (this._shutdownOnce.isCalled) {
        return;
      }
      if ((span.spanContext().traceFlags & api_1.TraceFlags.SAMPLED) === 0) {
        return;
      }
      this._addToBuffer(span);
    }
    shutdown() {
      return this._shutdownOnce.call();
    }
    _shutdown() {
      return Promise.resolve().then(() => {
        return this.onShutdown();
      }).then(() => {
        return this._flushAll();
      }).then(() => {
        this._metrics.shutdown();
        return this._exporter.shutdown();
      });
    }
    _addToBuffer(span) {
      if (this._finishedSpans.length >= this._maxQueueSize) {
        if (this._droppedSpansCount === 0) {
          api_1.diag.debug("maxQueueSize reached, dropping spans");
        }
        this._droppedSpansCount++;
        this._metrics.dropSpans(1);
        return;
      }
      if (this._droppedSpansCount > 0) {
        api_1.diag.warn(`Dropped ${this._droppedSpansCount} spans because maxQueueSize reached`);
        this._droppedSpansCount = 0;
      }
      this._finishedSpans.push(span);
      this._maybeStartTimer();
    }
    _flushAll() {
      return new Promise((resolve, reject) => {
        const promises = [];
        const count = Math.ceil(this._finishedSpans.length / this._maxExportBatchSize);
        for (let i = 0, j = count;i < j; i++) {
          promises.push(this._flushOneBatch());
        }
        Promise.all(promises).then(() => {
          resolve();
        }).catch(reject);
      });
    }
    _flushOneBatch() {
      this._clearTimer();
      if (this._finishedSpans.length === 0) {
        return Promise.resolve();
      }
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          reject(new Error("Timeout"));
        }, this._exportTimeoutMillis);
        api_1.context.with((0, core_1.suppressTracing)(api_1.context.active()), () => {
          let spans;
          if (this._finishedSpans.length <= this._maxExportBatchSize) {
            spans = this._finishedSpans;
            this._finishedSpans = [];
          } else {
            spans = this._finishedSpans.splice(0, this._maxExportBatchSize);
          }
          const doExport = () => this._exporter.export(spans, (result) => {
            clearTimeout(timer);
            this._metrics.finishSpans(spans.length, result.error);
            if (result.code === core_1.ExportResultCode.SUCCESS) {
              resolve();
            } else {
              reject(result.error ?? new Error("BatchSpanProcessor: span export failed"));
            }
          });
          let pendingResources = null;
          for (let i = 0, len = spans.length;i < len; i++) {
            const span = spans[i];
            if (span.resource.asyncAttributesPending && span.resource.waitForAsyncAttributes) {
              pendingResources ??= [];
              pendingResources.push(span.resource.waitForAsyncAttributes());
            }
          }
          if (pendingResources === null) {
            doExport();
          } else {
            Promise.all(pendingResources).then(doExport, (err) => {
              (0, core_1.globalErrorHandler)(err);
              reject(err);
            });
          }
        });
      });
    }
    _maybeStartTimer() {
      if (this._isExporting)
        return;
      const flush = () => {
        this._isExporting = true;
        this._flushOneBatch().finally(() => {
          this._isExporting = false;
          if (this._finishedSpans.length > 0) {
            this._clearTimer();
            this._maybeStartTimer();
          }
        }).catch((e) => {
          this._isExporting = false;
          (0, core_1.globalErrorHandler)(e);
        });
      };
      if (this._finishedSpans.length >= this._maxExportBatchSize) {
        return flush();
      }
      if (this._timer !== undefined)
        return;
      this._timer = setTimeout(() => flush(), this._scheduledDelayMillis);
      if (typeof this._timer !== "number") {
        this._timer.unref();
      }
    }
    _clearTimer() {
      if (this._timer !== undefined) {
        clearTimeout(this._timer);
        this._timer = undefined;
      }
    }
  }
  exports.BatchSpanProcessorBase = BatchSpanProcessorBase;
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/platform/node/export/BatchSpanProcessor.js
var require_BatchSpanProcessor = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.BatchSpanProcessor = undefined;
  var BatchSpanProcessorBase_1 = require_BatchSpanProcessorBase();

  class BatchSpanProcessor extends BatchSpanProcessorBase_1.BatchSpanProcessorBase {
    onShutdown() {}
  }
  exports.BatchSpanProcessor = BatchSpanProcessor;
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/platform/node/RandomIdGenerator.js
var require_RandomIdGenerator = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.RandomIdGenerator = undefined;
  var SPAN_ID_BYTES = 8;
  var TRACE_ID_BYTES = 16;

  class RandomIdGenerator {
    generateTraceId = getIdGenerator(TRACE_ID_BYTES);
    generateSpanId = getIdGenerator(SPAN_ID_BYTES);
  }
  exports.RandomIdGenerator = RandomIdGenerator;
  var SHARED_BUFFER = Buffer.allocUnsafe(TRACE_ID_BYTES);
  function getIdGenerator(bytes) {
    return function generateId() {
      for (let i = 0;i < bytes / 4; i++) {
        SHARED_BUFFER.writeUInt32BE(Math.random() * 2 ** 32 >>> 0, i * 4);
      }
      for (let i = 0;i < bytes; i++) {
        if (SHARED_BUFFER[i] > 0) {
          break;
        } else if (i === bytes - 1) {
          SHARED_BUFFER[bytes - 1] = 1;
        }
      }
      return SHARED_BUFFER.toString("hex", 0, bytes);
    };
  }
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/platform/node/index.js
var require_node10 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.RandomIdGenerator = exports.BatchSpanProcessor = undefined;
  var BatchSpanProcessor_1 = require_BatchSpanProcessor();
  Object.defineProperty(exports, "BatchSpanProcessor", { enumerable: true, get: function() {
    return BatchSpanProcessor_1.BatchSpanProcessor;
  } });
  var RandomIdGenerator_1 = require_RandomIdGenerator();
  Object.defineProperty(exports, "RandomIdGenerator", { enumerable: true, get: function() {
    return RandomIdGenerator_1.RandomIdGenerator;
  } });
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/platform/index.js
var require_platform10 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.RandomIdGenerator = exports.BatchSpanProcessor = undefined;
  var node_1 = require_node10();
  Object.defineProperty(exports, "BatchSpanProcessor", { enumerable: true, get: function() {
    return node_1.BatchSpanProcessor;
  } });
  Object.defineProperty(exports, "RandomIdGenerator", { enumerable: true, get: function() {
    return node_1.RandomIdGenerator;
  } });
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/TracerProvider.js
var require_TracerProvider = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.TracerProvider = undefined;
  var api_1 = require_src();
  var resources_1 = require_src7();
  var Tracer_1 = require_Tracer();
  var MultiSpanProcessor_1 = require_MultiSpanProcessor();
  var ParentBasedSampler_1 = require_ParentBasedSampler();
  var AlwaysOnSampler_1 = require_AlwaysOnSampler();
  var platform_1 = require_platform10();
  var inspect_1 = require_inspect();
  var ForceFlushState;
  (function(ForceFlushState) {
    ForceFlushState[ForceFlushState["resolved"] = 0] = "resolved";
    ForceFlushState[ForceFlushState["timeout"] = 1] = "timeout";
    ForceFlushState[ForceFlushState["error"] = 2] = "error";
    ForceFlushState[ForceFlushState["unresolved"] = 3] = "unresolved";
  })(ForceFlushState || (ForceFlushState = {}));

  class TracerProvider {
    _resource;
    _activeSpanProcessor;
    _forceFlushTimeoutMillis;
    _tracerOptions;
    _tracers = new Map;
    constructor(options = {}) {
      this._forceFlushTimeoutMillis = options.forceFlushTimeoutMillis ?? 30000;
      this._resource = options.resource ?? (0, resources_1.defaultResource)();
      const spanProcessors = options.spanProcessors ?? [];
      this._activeSpanProcessor = new MultiSpanProcessor_1.MultiSpanProcessor(spanProcessors);
      this._tracerOptions = {
        resource: this._resource,
        sampler: options.sampler ?? new ParentBasedSampler_1.ParentBasedSampler({
          root: new AlwaysOnSampler_1.AlwaysOnSampler
        }),
        spanLimits: {
          attributeCountLimit: options.spanLimits?.attributeCountLimit ?? 128,
          attributeValueLengthLimit: options.spanLimits?.attributeValueLengthLimit ?? Infinity,
          eventCountLimit: options.spanLimits?.eventCountLimit ?? 128,
          linkCountLimit: options.spanLimits?.linkCountLimit ?? 128,
          attributePerEventCountLimit: options.spanLimits?.attributePerEventCountLimit ?? 128,
          attributePerLinkCountLimit: options.spanLimits?.attributePerLinkCountLimit ?? 128
        },
        idGenerator: options.idGenerator || new platform_1.RandomIdGenerator,
        spanProcessor: this._activeSpanProcessor,
        meterProvider: options.meterProvider ?? {
          getMeter() {
            return (0, api_1.createNoopMeter)();
          }
        }
      };
    }
    getTracer(name, version, options) {
      const key = `${name}@${version || ""}:${options?.schemaUrl || ""}`;
      if (!this._tracers.has(key)) {
        this._tracers.set(key, new Tracer_1.Tracer({ name, version, schemaUrl: options?.schemaUrl }, this._tracerOptions));
      }
      return this._tracers.get(key);
    }
    forceFlush(options) {
      const timeout = options?.timeoutMillis ?? this._forceFlushTimeoutMillis;
      const promises = this._activeSpanProcessor["_spanProcessors"].map((spanProcessor) => {
        return new Promise((resolve) => {
          let state;
          const timeoutInterval = setTimeout(() => {
            resolve(new Error(`Span processor did not completed within timeout period of ${timeout} ms`));
            state = ForceFlushState.timeout;
          }, timeout);
          spanProcessor.forceFlush().then(() => {
            clearTimeout(timeoutInterval);
            if (state !== ForceFlushState.timeout) {
              state = ForceFlushState.resolved;
              resolve(state);
            }
          }).catch((error) => {
            clearTimeout(timeoutInterval);
            state = ForceFlushState.error;
            resolve(error);
          });
        });
      });
      return new Promise((resolve, reject) => {
        Promise.all(promises).then((results) => {
          const errors = results.filter((result) => result !== ForceFlushState.resolved);
          if (errors.length > 0) {
            reject(errors);
          } else {
            resolve();
          }
        }).catch((error) => reject([error]));
      });
    }
    shutdown() {
      return this._activeSpanProcessor.shutdown();
    }
    [inspect_1.inspectCustom](depth, options, inspect) {
      const processors = this._activeSpanProcessor["_spanProcessors"];
      const payload = {
        resource: { attributes: (0, inspect_1.settledResourceAttributes)(this._resource) },
        tracers: Array.from(this._tracers.keys()),
        spanProcessors: processors.map((p) => p.constructor?.name ?? "SpanProcessor")
      };
      return (0, inspect_1.formatInspect)("TracerProvider", payload, depth, options, inspect);
    }
  }
  exports.TracerProvider = TracerProvider;
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/export/ConsoleSpanExporter.js
var require_ConsoleSpanExporter = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.ConsoleSpanExporter = undefined;
  var core_1 = require_src5();

  class ConsoleSpanExporter {
    export(spans, resultCallback) {
      return this._sendSpans(spans, resultCallback);
    }
    shutdown() {
      this._sendSpans([]);
      return this.forceFlush();
    }
    forceFlush() {
      return Promise.resolve();
    }
    _exportInfo(span) {
      return {
        resource: {
          attributes: span.resource.attributes
        },
        instrumentationScope: span.instrumentationScope,
        traceId: span.spanContext().traceId,
        parentSpanContext: span.parentSpanContext,
        traceState: span.spanContext().traceState?.serialize(),
        name: span.name,
        id: span.spanContext().spanId,
        kind: span.kind,
        timestamp: (0, core_1.hrTimeToMicroseconds)(span.startTime),
        duration: (0, core_1.hrTimeToMicroseconds)(span.duration),
        attributes: span.attributes,
        status: span.status,
        events: span.events,
        links: span.links
      };
    }
    _sendSpans(spans, done) {
      for (const span of spans) {
        console.dir(this._exportInfo(span), { depth: 3 });
      }
      if (done) {
        return done({ code: core_1.ExportResultCode.SUCCESS });
      }
    }
  }
  exports.ConsoleSpanExporter = ConsoleSpanExporter;
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/export/InMemorySpanExporter.js
var require_InMemorySpanExporter = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.InMemorySpanExporter = undefined;
  var core_1 = require_src5();

  class InMemorySpanExporter {
    _finishedSpans = [];
    _stopped = false;
    export(spans, resultCallback) {
      if (this._stopped)
        return resultCallback({
          code: core_1.ExportResultCode.FAILED,
          error: new Error("Exporter has been stopped")
        });
      this._finishedSpans.push(...spans);
      setTimeout(() => resultCallback({ code: core_1.ExportResultCode.SUCCESS }), 0);
    }
    shutdown() {
      this._stopped = true;
      this._finishedSpans = [];
      return this.forceFlush();
    }
    forceFlush() {
      return Promise.resolve();
    }
    reset() {
      this._finishedSpans = [];
    }
    getFinishedSpans() {
      return this._finishedSpans;
    }
  }
  exports.InMemorySpanExporter = InMemorySpanExporter;
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/export/SimpleSpanProcessor.js
var require_SimpleSpanProcessor = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.SimpleSpanProcessor = undefined;
  var api_1 = require_src();
  var core_1 = require_src5();
  var SpanProcessorMetrics_1 = require_SpanProcessorMetrics();
  var semconv_1 = require_semconv12();

  class SimpleSpanProcessor {
    _exporter;
    _metrics;
    _shutdownOnce;
    _pendingExports;
    constructor(options) {
      this._exporter = options.exporter;
      this._shutdownOnce = new core_1.BindOnceFuture(this._shutdown, this);
      this._pendingExports = new Set;
      const meter = options.selfObsMeterProvider ? options.selfObsMeterProvider.getMeter("@opentelemetry/sdk-trace") : (0, api_1.createNoopMeter)();
      this._metrics = new SpanProcessorMetrics_1.SpanProcessorMetrics(semconv_1.OTEL_COMPONENT_TYPE_VALUE_SIMPLE_SPAN_PROCESSOR, meter);
    }
    async forceFlush() {
      let pendingExportError;
      let pendingExportRejected = false;
      try {
        await Promise.all(Array.from(this._pendingExports));
      } catch (err) {
        pendingExportError = err;
        pendingExportRejected = true;
      }
      if (this._exporter.forceFlush) {
        await this._exporter.forceFlush();
      }
      if (pendingExportRejected) {
        throw pendingExportError;
      }
    }
    onStart(_span, _parentContext) {}
    onEnd(span) {
      if (this._shutdownOnce.isCalled) {
        return;
      }
      if ((span.spanContext().traceFlags & api_1.TraceFlags.SAMPLED) === 0) {
        return;
      }
      const pendingExport = this._doExport(span);
      this._pendingExports.add(pendingExport);
      pendingExport.then(() => {
        this._pendingExports.delete(pendingExport);
      }, (err) => {
        (0, core_1.globalErrorHandler)(err);
        this._pendingExports.delete(pendingExport);
      });
    }
    async _doExport(span) {
      if (span.resource.asyncAttributesPending) {
        await span.resource.waitForAsyncAttributes?.();
      }
      const result = await core_1.internal._export(this._exporter, [span]);
      this._metrics.finishSpans(1, result.error);
      if (result.code !== core_1.ExportResultCode.SUCCESS) {
        throw result.error ?? new Error(`SimpleSpanProcessor: span export failed (status ${result})`);
      }
    }
    shutdown() {
      return this._shutdownOnce.call();
    }
    _shutdown() {
      this._metrics.shutdown();
      return this._exporter.shutdown();
    }
  }
  exports.SimpleSpanProcessor = SimpleSpanProcessor;
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/export/NoopSpanProcessor.js
var require_NoopSpanProcessor = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.NoopSpanProcessor = undefined;

  class NoopSpanProcessor {
    onStart(_span, _context) {}
    onEnd(_span) {}
    shutdown() {
      return Promise.resolve();
    }
    forceFlush() {
      return Promise.resolve();
    }
  }
  exports.NoopSpanProcessor = NoopSpanProcessor;
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/sampler/AlwaysRecordSampler.js
var require_AlwaysRecordSampler = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.createAlwaysRecordSampler = undefined;
  var Sampler_1 = require_Sampler();
  function createAlwaysRecordSampler(delegate) {
    if (!delegate) {
      throw new Error("createAlwaysRecordSampler requires a delegate sampler");
    }
    return {
      shouldSample(context, traceId, spanName, spanKind, attributes, links) {
        const result = delegate.shouldSample(context, traceId, spanName, spanKind, attributes, links);
        if (result.decision === Sampler_1.SamplingDecision.NOT_RECORD) {
          return {
            decision: Sampler_1.SamplingDecision.RECORD,
            attributes: result.attributes,
            traceState: result.traceState
          };
        }
        return result;
      },
      toString() {
        return `AlwaysRecordSampler{${delegate.toString()}}`;
      }
    };
  }
  exports.createAlwaysRecordSampler = createAlwaysRecordSampler;
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/sampler/TraceIdRatioBasedSampler.js
var require_TraceIdRatioBasedSampler = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.TraceIdRatioBasedSampler = undefined;
  var api_1 = require_src();
  var Sampler_1 = require_Sampler();

  class TraceIdRatioBasedSampler {
    _ratio;
    _upperBound;
    constructor(ratio = 0) {
      this._ratio = this._normalize(ratio);
      this._upperBound = this._ratio === 1 ? 4294967296 : Math.floor(this._ratio * 4294967295);
    }
    shouldSample(context, traceId) {
      return {
        decision: (0, api_1.isValidTraceId)(traceId) && this._accumulate(traceId) < this._upperBound ? Sampler_1.SamplingDecision.RECORD_AND_SAMPLED : Sampler_1.SamplingDecision.NOT_RECORD
      };
    }
    toString() {
      return `TraceIdRatioBased{${this._ratio}}`;
    }
    _normalize(ratio) {
      if (typeof ratio !== "number" || isNaN(ratio))
        return 0;
      return ratio >= 1 ? 1 : ratio <= 0 ? 0 : ratio;
    }
    _accumulate(traceId) {
      let accumulation = 0;
      for (let i = 0;i < 32; i += 8) {
        let part = 0;
        for (let j = 0;j < 8; j++) {
          const c = traceId.charCodeAt(i + j);
          const v = c < 58 ? c - 48 : c < 71 ? c - 55 : c - 87;
          part = part << 4 | v;
        }
        accumulation = (accumulation ^ part) >>> 0;
      }
      return accumulation;
    }
  }
  exports.TraceIdRatioBasedSampler = TraceIdRatioBasedSampler;
});

// node_modules/.bun/@opentelemetry+sdk-trace@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace/build/src/index.js
var require_src17 = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.SamplingDecision = exports.TraceIdRatioBasedSampler = exports.ParentBasedSampler = exports.createAlwaysRecordSampler = exports.AlwaysOnSampler = exports.AlwaysOffSampler = exports.NoopSpanProcessor = exports.SimpleSpanProcessor = exports.InMemorySpanExporter = exports.ConsoleSpanExporter = exports.RandomIdGenerator = exports.BatchSpanProcessor = exports.TracerProvider = undefined;
  var TracerProvider_1 = require_TracerProvider();
  Object.defineProperty(exports, "TracerProvider", { enumerable: true, get: function() {
    return TracerProvider_1.TracerProvider;
  } });
  var platform_1 = require_platform10();
  Object.defineProperty(exports, "BatchSpanProcessor", { enumerable: true, get: function() {
    return platform_1.BatchSpanProcessor;
  } });
  Object.defineProperty(exports, "RandomIdGenerator", { enumerable: true, get: function() {
    return platform_1.RandomIdGenerator;
  } });
  var ConsoleSpanExporter_1 = require_ConsoleSpanExporter();
  Object.defineProperty(exports, "ConsoleSpanExporter", { enumerable: true, get: function() {
    return ConsoleSpanExporter_1.ConsoleSpanExporter;
  } });
  var InMemorySpanExporter_1 = require_InMemorySpanExporter();
  Object.defineProperty(exports, "InMemorySpanExporter", { enumerable: true, get: function() {
    return InMemorySpanExporter_1.InMemorySpanExporter;
  } });
  var SimpleSpanProcessor_1 = require_SimpleSpanProcessor();
  Object.defineProperty(exports, "SimpleSpanProcessor", { enumerable: true, get: function() {
    return SimpleSpanProcessor_1.SimpleSpanProcessor;
  } });
  var NoopSpanProcessor_1 = require_NoopSpanProcessor();
  Object.defineProperty(exports, "NoopSpanProcessor", { enumerable: true, get: function() {
    return NoopSpanProcessor_1.NoopSpanProcessor;
  } });
  var AlwaysOffSampler_1 = require_AlwaysOffSampler();
  Object.defineProperty(exports, "AlwaysOffSampler", { enumerable: true, get: function() {
    return AlwaysOffSampler_1.AlwaysOffSampler;
  } });
  var AlwaysOnSampler_1 = require_AlwaysOnSampler();
  Object.defineProperty(exports, "AlwaysOnSampler", { enumerable: true, get: function() {
    return AlwaysOnSampler_1.AlwaysOnSampler;
  } });
  var AlwaysRecordSampler_1 = require_AlwaysRecordSampler();
  Object.defineProperty(exports, "createAlwaysRecordSampler", { enumerable: true, get: function() {
    return AlwaysRecordSampler_1.createAlwaysRecordSampler;
  } });
  var ParentBasedSampler_1 = require_ParentBasedSampler();
  Object.defineProperty(exports, "ParentBasedSampler", { enumerable: true, get: function() {
    return ParentBasedSampler_1.ParentBasedSampler;
  } });
  var TraceIdRatioBasedSampler_1 = require_TraceIdRatioBasedSampler();
  Object.defineProperty(exports, "TraceIdRatioBasedSampler", { enumerable: true, get: function() {
    return TraceIdRatioBasedSampler_1.TraceIdRatioBasedSampler;
  } });
  var Sampler_1 = require_Sampler();
  Object.defineProperty(exports, "SamplingDecision", { enumerable: true, get: function() {
    return Sampler_1.SamplingDecision;
  } });
});

// node_modules/.bun/@opentelemetry+sdk-trace-base@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace-base/build/src/config.js
var require_config = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.buildSamplerFromEnv = exports.loadDefaultConfig = undefined;
  var api_1 = require_src();
  var core_1 = require_src5();
  var sdk_trace_1 = require_src17();
  var TracesSamplerValues;
  (function(TracesSamplerValues) {
    TracesSamplerValues["AlwaysOff"] = "always_off";
    TracesSamplerValues["AlwaysOn"] = "always_on";
    TracesSamplerValues["ParentBasedAlwaysOff"] = "parentbased_always_off";
    TracesSamplerValues["ParentBasedAlwaysOn"] = "parentbased_always_on";
    TracesSamplerValues["ParentBasedTraceIdRatio"] = "parentbased_traceidratio";
    TracesSamplerValues["TraceIdRatio"] = "traceidratio";
  })(TracesSamplerValues || (TracesSamplerValues = {}));
  var DEFAULT_RATIO = 1;
  function loadDefaultConfig() {
    return {
      sampler: buildSamplerFromEnv(),
      forceFlushTimeoutMillis: 30000,
      generalLimits: {
        attributeValueLengthLimit: (0, core_1.getNumberFromEnv)("OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT") ?? Infinity,
        attributeCountLimit: (0, core_1.getNumberFromEnv)("OTEL_ATTRIBUTE_COUNT_LIMIT") ?? 128
      },
      spanLimits: {
        attributeValueLengthLimit: (0, core_1.getNumberFromEnv)("OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT") ?? Infinity,
        attributeCountLimit: (0, core_1.getNumberFromEnv)("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT") ?? 128,
        linkCountLimit: (0, core_1.getNumberFromEnv)("OTEL_SPAN_LINK_COUNT_LIMIT") ?? 128,
        eventCountLimit: (0, core_1.getNumberFromEnv)("OTEL_SPAN_EVENT_COUNT_LIMIT") ?? 128,
        attributePerEventCountLimit: (0, core_1.getNumberFromEnv)("OTEL_SPAN_ATTRIBUTE_PER_EVENT_COUNT_LIMIT") ?? 128,
        attributePerLinkCountLimit: (0, core_1.getNumberFromEnv)("OTEL_SPAN_ATTRIBUTE_PER_LINK_COUNT_LIMIT") ?? 128
      }
    };
  }
  exports.loadDefaultConfig = loadDefaultConfig;
  function buildSamplerFromEnv() {
    const sampler = (0, core_1.getStringFromEnv)("OTEL_TRACES_SAMPLER") ?? TracesSamplerValues.ParentBasedAlwaysOn;
    switch (sampler) {
      case TracesSamplerValues.AlwaysOn:
        return new sdk_trace_1.AlwaysOnSampler;
      case TracesSamplerValues.AlwaysOff:
        return new sdk_trace_1.AlwaysOffSampler;
      case TracesSamplerValues.ParentBasedAlwaysOn:
        return new sdk_trace_1.ParentBasedSampler({
          root: new sdk_trace_1.AlwaysOnSampler
        });
      case TracesSamplerValues.ParentBasedAlwaysOff:
        return new sdk_trace_1.ParentBasedSampler({
          root: new sdk_trace_1.AlwaysOffSampler
        });
      case TracesSamplerValues.TraceIdRatio:
        return new sdk_trace_1.TraceIdRatioBasedSampler(getSamplerProbabilityFromEnv());
      case TracesSamplerValues.ParentBasedTraceIdRatio:
        return new sdk_trace_1.ParentBasedSampler({
          root: new sdk_trace_1.TraceIdRatioBasedSampler(getSamplerProbabilityFromEnv())
        });
      default:
        api_1.diag.error(`OTEL_TRACES_SAMPLER value "${sampler}" invalid, defaulting to "${TracesSamplerValues.ParentBasedAlwaysOn}".`);
        return new sdk_trace_1.ParentBasedSampler({
          root: new sdk_trace_1.AlwaysOnSampler
        });
    }
  }
  exports.buildSamplerFromEnv = buildSamplerFromEnv;
  function getSamplerProbabilityFromEnv() {
    const probability = (0, core_1.getNumberFromEnv)("OTEL_TRACES_SAMPLER_ARG");
    if (probability == null) {
      api_1.diag.error(`OTEL_TRACES_SAMPLER_ARG is blank, defaulting to ${DEFAULT_RATIO}.`);
      return DEFAULT_RATIO;
    }
    if (probability < 0 || probability > 1) {
      api_1.diag.error(`OTEL_TRACES_SAMPLER_ARG=${probability} was given, but it is out of range ([0..1]), defaulting to ${DEFAULT_RATIO}.`);
      return DEFAULT_RATIO;
    }
    return probability;
  }
});

// node_modules/.bun/@opentelemetry+sdk-trace-base@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace-base/build/src/utility.js
var require_utility = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.reconfigureLimits = exports.DEFAULT_ATTRIBUTE_VALUE_LENGTH_LIMIT = exports.DEFAULT_ATTRIBUTE_COUNT_LIMIT = undefined;
  var core_1 = require_src5();
  exports.DEFAULT_ATTRIBUTE_COUNT_LIMIT = 128;
  exports.DEFAULT_ATTRIBUTE_VALUE_LENGTH_LIMIT = Infinity;
  function reconfigureLimits(userConfig) {
    const spanLimits = Object.assign({}, userConfig.spanLimits);
    spanLimits.attributeCountLimit = userConfig.spanLimits?.attributeCountLimit ?? userConfig.generalLimits?.attributeCountLimit ?? (0, core_1.getNumberFromEnv)("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT") ?? (0, core_1.getNumberFromEnv)("OTEL_ATTRIBUTE_COUNT_LIMIT") ?? exports.DEFAULT_ATTRIBUTE_COUNT_LIMIT;
    spanLimits.attributeValueLengthLimit = userConfig.spanLimits?.attributeValueLengthLimit ?? userConfig.generalLimits?.attributeValueLengthLimit ?? (0, core_1.getNumberFromEnv)("OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT") ?? (0, core_1.getNumberFromEnv)("OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT") ?? exports.DEFAULT_ATTRIBUTE_VALUE_LENGTH_LIMIT;
    return Object.assign({}, userConfig, { spanLimits });
  }
  exports.reconfigureLimits = reconfigureLimits;
});

// node_modules/.bun/@opentelemetry+sdk-trace-base@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace-base/build/src/BasicTracerProvider-shim.js
var require_BasicTracerProvider_shim = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.BasicTracerProvider = undefined;
  var core_1 = require_src5();
  var config_1 = require_config();
  var utility_1 = require_utility();
  var sdk_trace_1 = require_src17();

  class BasicTracerProvider extends sdk_trace_1.TracerProvider {
    constructor(config = {}) {
      const mergedConfig = (0, core_1.merge)({}, (0, config_1.loadDefaultConfig)(), (0, utility_1.reconfigureLimits)(config));
      delete mergedConfig.generalLimits;
      super(mergedConfig);
    }
  }
  exports.BasicTracerProvider = BasicTracerProvider;
});

// node_modules/.bun/@opentelemetry+sdk-trace-base@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace-base/build/src/BatchSpanProcessor-shim.js
var require_BatchSpanProcessor_shim = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.BatchSpanProcessor = undefined;
  var core_1 = require_src5();
  var sdk_trace_1 = require_src17();

  class BatchSpanProcessor extends sdk_trace_1.BatchSpanProcessor {
    constructor(exporter, config) {
      if (!config) {
        config = {};
      }
      const envFallbacks = [
        ["maxExportBatchSize", "OTEL_BSP_MAX_EXPORT_BATCH_SIZE"],
        ["maxQueueSize", "OTEL_BSP_MAX_QUEUE_SIZE"],
        ["scheduledDelayMillis", "OTEL_BSP_SCHEDULE_DELAY"],
        ["exportTimeoutMillis", "OTEL_BSP_EXPORT_TIMEOUT"]
      ];
      for (const [configName, envName] of envFallbacks) {
        if (config[configName] === undefined) {
          const envFallback = (0, core_1.getNumberFromEnv)(envName);
          if (envFallback !== undefined) {
            config[configName] = envFallback;
          }
        }
      }
      super({ exporter, ...config });
    }
  }
  exports.BatchSpanProcessor = BatchSpanProcessor;
});

// node_modules/.bun/@opentelemetry+sdk-trace-base@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace-base/build/src/SimpleSpanProcessor-shim.js
var require_SimpleSpanProcessor_shim = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.SimpleSpanProcessor = undefined;
  var sdk_trace_1 = require_src17();

  class SimpleSpanProcessor extends sdk_trace_1.SimpleSpanProcessor {
    constructor(exporter) {
      super({ exporter });
    }
  }
  exports.SimpleSpanProcessor = SimpleSpanProcessor;
});

// node_modules/.bun/@opentelemetry+sdk-trace-base@2.11.0+e40b0dfdd726a224/node_modules/@opentelemetry/sdk-trace-base/build/src/index-shim.js
var require_index_shim = __commonJS(function(exports) {
  Object.defineProperty(exports, "__esModule", { value: true });
  exports.SamplingDecision = exports.TraceIdRatioBasedSampler = exports.ParentBasedSampler = exports.AlwaysOnSampler = exports.AlwaysOffSampler = exports.NoopSpanProcessor = exports.InMemorySpanExporter = exports.RandomIdGenerator = exports.ConsoleSpanExporter = exports.SimpleSpanProcessor = exports.BatchSpanProcessor = exports.BasicTracerProvider = undefined;
  var BasicTracerProvider_shim_1 = require_BasicTracerProvider_shim();
  Object.defineProperty(exports, "BasicTracerProvider", { enumerable: true, get: function() {
    return BasicTracerProvider_shim_1.BasicTracerProvider;
  } });
  var BatchSpanProcessor_shim_1 = require_BatchSpanProcessor_shim();
  Object.defineProperty(exports, "BatchSpanProcessor", { enumerable: true, get: function() {
    return BatchSpanProcessor_shim_1.BatchSpanProcessor;
  } });
  var SimpleSpanProcessor_shim_1 = require_SimpleSpanProcessor_shim();
  Object.defineProperty(exports, "SimpleSpanProcessor", { enumerable: true, get: function() {
    return SimpleSpanProcessor_shim_1.SimpleSpanProcessor;
  } });
  var sdk_trace_1 = require_src17();
  Object.defineProperty(exports, "ConsoleSpanExporter", { enumerable: true, get: function() {
    return sdk_trace_1.ConsoleSpanExporter;
  } });
  Object.defineProperty(exports, "RandomIdGenerator", { enumerable: true, get: function() {
    return sdk_trace_1.RandomIdGenerator;
  } });
  Object.defineProperty(exports, "InMemorySpanExporter", { enumerable: true, get: function() {
    return sdk_trace_1.InMemorySpanExporter;
  } });
  Object.defineProperty(exports, "NoopSpanProcessor", { enumerable: true, get: function() {
    return sdk_trace_1.NoopSpanProcessor;
  } });
  Object.defineProperty(exports, "AlwaysOffSampler", { enumerable: true, get: function() {
    return sdk_trace_1.AlwaysOffSampler;
  } });
  Object.defineProperty(exports, "AlwaysOnSampler", { enumerable: true, get: function() {
    return sdk_trace_1.AlwaysOnSampler;
  } });
  Object.defineProperty(exports, "ParentBasedSampler", { enumerable: true, get: function() {
    return sdk_trace_1.ParentBasedSampler;
  } });
  Object.defineProperty(exports, "TraceIdRatioBasedSampler", { enumerable: true, get: function() {
    return sdk_trace_1.TraceIdRatioBasedSampler;
  } });
  Object.defineProperty(exports, "SamplingDecision", { enumerable: true, get: function() {
    return sdk_trace_1.SamplingDecision;
  } });
});

// packages/core/src/telemetry/sdk.ts
var import_api = __toESM(require_src(), 1);
var import_api_logs = __toESM(require_src2(), 1);
var import_context_async_hooks = __toESM(require_src3(), 1);
var import_core = __toESM(require_src5(), 1);
var import_exporter_logs_otlp_http = __toESM(require_src10(), 1);
var import_exporter_logs_otlp_proto = __toESM(require_src11(), 1);
var import_exporter_metrics_otlp_http = __toESM(require_src12(), 1);
var import_exporter_metrics_otlp_proto = __toESM(require_src13(), 1);
var import_exporter_trace_otlp_http = __toESM(require_src14(), 1);
var import_exporter_trace_otlp_proto = __toESM(require_src15(), 1);
var import_resources = __toESM(require_src7(), 1);
var import_sdk_logs = __toESM(require_src16(), 1);
var import_sdk_metrics = __toESM(require_src8(), 1);
var import_sdk_trace_base = __toESM(require_index_shim(), 1);
import { readFileSync } from "node:fs";
import { Agent as HttpAgent } from "node:http";
import { Agent as HttpsAgent } from "node:https";
import { resolve } from "node:path";
function bearerOf(file) {
  const read = () => readTokenFile(file, "the telemetry bearer token");
  let token = read();
  let failure;
  return {
    headers: async () => {
      if (token === undefined)
        throw failure;
      return { authorization: `Bearer ${token}` };
    },
    reread: () => {
      try {
        token = read();
      } catch (error) {
        if (token !== undefined)
          process.stderr.write(`bayma: telemetry: not exported: ${error instanceof Error ? error.message : String(error)}
`);
        token = undefined;
        failure = error;
      }
    }
  };
}
function tlsFile(signal, variable, unread) {
  const file = import_core.getStringFromEnv(`OTEL_EXPORTER_OTLP_${signal.toUpperCase()}_${variable}`) ?? import_core.getStringFromEnv(`OTEL_EXPORTER_OTLP_${variable}`);
  if (file === undefined)
    return;
  try {
    return readFileSync(resolve(process.cwd(), file));
  } catch {
    import_api.diag.warn(unread);
    return;
  }
}
function exportAgents() {
  const made = [];
  return {
    of: (signal) => {
      const tls = {
        ca: tlsFile(signal, "CERTIFICATE", "Failed to read root certificate file"),
        cert: tlsFile(signal, "CLIENT_CERTIFICATE", "Failed to read client certificate chain file"),
        key: tlsFile(signal, "CLIENT_KEY", "Failed to read client certificate private key file")
      };
      return (protocol) => {
        const agent = protocol === "http:" ? new HttpAgent({ keepAlive: true }) : new HttpsAgent({ keepAlive: true, ...tls });
        made.push(agent);
        return agent;
      };
    },
    end: () => {
      for (const agent of made)
        agent.destroy();
    }
  };
}
function startSdk(exports, attributes, bearerTokenFile) {
  const agents = exportAgents();
  const bearer = bearerTokenFile === undefined ? undefined : bearerOf(bearerTokenFile);
  const exporterOptions = (signal) => ({
    ...bearer === undefined ? {} : { headers: bearer.headers },
    httpAgentOptions: agents.of(signal)
  });
  const resource = import_resources.defaultResource().merge(import_resources.resourceFromAttributes(attributes)).merge(import_resources.detectResources({
    detectors: [import_resources.hostDetector, import_resources.processDetector, import_resources.envDetector]
  }));
  const json = (signal) => exports.find((exported) => exported.signal === signal)?.protocol === "http/json";
  const exported = new Set(exports.map(({ signal }) => signal));
  const stops = [];
  const flushes = [];
  import_api.context.setGlobalContextManager(new import_context_async_hooks.AsyncLocalStorageContextManager().enable());
  import_api.propagation.setGlobalPropagator(new import_core.CompositePropagator({
    propagators: [
      new import_core.W3CTraceContextPropagator,
      new import_core.W3CBaggagePropagator
    ]
  }));
  if (exported.has("traces")) {
    const tracerProvider = new import_sdk_trace_base.BasicTracerProvider({
      resource,
      spanProcessors: [
        new import_sdk_trace_base.BatchSpanProcessor(json("traces") ? new import_exporter_trace_otlp_http.OTLPTraceExporter(exporterOptions("traces")) : new import_exporter_trace_otlp_proto.OTLPTraceExporter(exporterOptions("traces")))
      ]
    });
    import_api.trace.setGlobalTracerProvider(tracerProvider);
    stops.push(() => tracerProvider.shutdown());
    flushes.push(() => tracerProvider.forceFlush());
  }
  if (exported.has("metrics")) {
    const exportIntervalMillis = import_core.getNumberFromEnv("OTEL_METRIC_EXPORT_INTERVAL") ?? 60000;
    const meterProvider = new import_sdk_metrics.MeterProvider({
      resource,
      readers: [
        new import_sdk_metrics.PeriodicExportingMetricReader({
          exporter: json("metrics") ? new import_exporter_metrics_otlp_http.OTLPMetricExporter(exporterOptions("metrics")) : new import_exporter_metrics_otlp_proto.OTLPMetricExporter(exporterOptions("metrics")),
          exportIntervalMillis,
          exportTimeoutMillis: Math.min(import_core.getNumberFromEnv("OTEL_METRIC_EXPORT_TIMEOUT") ?? 30000, exportIntervalMillis)
        })
      ]
    });
    import_api.metrics.setGlobalMeterProvider(meterProvider);
    stops.push(() => meterProvider.shutdown());
    flushes.push(() => meterProvider.forceFlush());
  }
  if (exported.has("logs")) {
    const loggerProvider = new import_sdk_logs.LoggerProvider({
      resource,
      processors: [
        new import_sdk_logs.BatchLogRecordProcessor({
          exporter: json("logs") ? new import_exporter_logs_otlp_http.OTLPLogExporter(exporterOptions("logs")) : new import_exporter_logs_otlp_proto.OTLPLogExporter(exporterOptions("logs"))
        })
      ]
    });
    import_api_logs.logs.setGlobalLoggerProvider(loggerProvider);
    stops.push(() => loggerProvider.shutdown());
    flushes.push(() => loggerProvider.forceFlush());
  }
  return {
    shutdown: async () => {
      await Promise.all(stops.map((stop) => stop()));
    },
    reconnect: () => {
      agents.end();
      bearer?.reread();
    },
    disconnect: () => agents.end(),
    flush: async () => {
      await Promise.all(flushes.map((flush) => flush()));
    }
  };
}
export {
  startSdk
};
