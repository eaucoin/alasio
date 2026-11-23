#!/usr/bin/env -S node --disable-warning=ExperimentalWarning
import {
  assertDoctorExecResult2,
  JSON_CHECKPOINT_CODEC2,
  RUNTIME_OUTPUT_CAPTURE_POLICY2,
  RUNTIME_OUTPUT_TRUNCATION_MARKER2,
  createExecFileWorkspace2,
  payloadValue2,
  ProcessTransport2,
  buildMarkers2,
  createPromptAwareEnvelopeCollector2
} from "./main-mgny7qcs.js";

// packages/runtime-bun/src/profile.ts
var bunModelProfile = {
  runtimeId: "bun",
  heading: "Bun",
  description: "Runs JavaScript or TypeScript in Bun. Bun.*, top-level await, fetch, imports, and packages available from the session working directory are available. Checkpointed recovery preserves explicit $checkpoint state using Bayma's structured-clone codec. $displayImage(image) shows the model a PNG, JPEG, GIF, or WebP image, from a path, a file URL, or bytes."
};

// packages/runtime-bun/src/transport.ts
var BUN_PROMPT_RE = /(?:^|[\r\n])(?:> |❯ )/g;
function printingNonce(code, nonce) {
  return {
    input: `${code}process.stdout.write(Buffer.from(${JSON.stringify(nonce)}).toString("base64") + "\\n"); void 0
`,
    expectedOutput: Buffer.from(nonce).toString("base64")
  };
}
function createBunTransport() {
  return new ProcessTransport2({
    platformId: "tmux",
    promptRe: BUN_PROMPT_RE,
    lineSubmitDelayMs: 10,
    interruptStrategy: "write_ctrl_c",
    interruptProbe: (nonce) => printingNonce("", nonce),
    garbageCollectionProbe: (nonce) => printingNonce("Bun.gc(true); ", nonce),
    command: () => ({
      file: payloadValue2("BAYMA_BUN_BIN"),
      args: ["repl"]
    })
  });
}

// packages/runtime-bun/src/exec.ts
import { writeFileSync } from "node:fs";
import { dirname } from "node:path";

// packages/runtime-bun/src/inspection.ts
var BUN_INSPECTION_POLICY = {
  maxDepth: 6,
  maxEntries: 100,
  maxStringCharacters: 16384,
  maxVisitedNodes: 1000,
  maxRenderedBytes: RUNTIME_OUTPUT_CAPTURE_POLICY2.maxMessageBytes
};
function bunInspectionPrelude() {
  return [
    `const __baymaInspectionPolicy = ${JSON.stringify(BUN_INSPECTION_POLICY)};`,
    `const __baymaBoundText = (value) => {`,
    `  const text = String(value);`,
    `  const bytes = Buffer.from(text);`,
    `  const maxBytes = __baymaInspectionPolicy.maxRenderedBytes;`,
    `  if (bytes.length <= maxBytes) return text;`,
    `  const marker = Buffer.from(${JSON.stringify(RUNTIME_OUTPUT_TRUNCATION_MARKER2)});`,
    `  const payloadBytes = maxBytes - marker.length;`,
    `  const leftTarget = Math.floor(payloadBytes / 2);`,
    `  const rightTarget = bytes.length - (payloadBytes - leftTarget);`,
    `  let left = leftTarget;`,
    `  while (left > 0 && (bytes[left] & 0xc0) === 0x80) left -= 1;`,
    `  let right = rightTarget;`,
    `  while (right < bytes.length && (bytes[right] & 0xc0) === 0x80) right += 1;`,
    `  return bytes.subarray(0, left).toString("utf8") + marker.toString("utf8") + bytes.subarray(right).toString("utf8");`,
    `};`,
    `const __baymaPreviewString = (value) => {`,
    `  const limit = __baymaInspectionPolicy.maxStringCharacters;`,
    `  if (value.length <= limit) return value;`,
    `  let prefix = value.slice(0, limit);`,
    `  const last = prefix.charCodeAt(prefix.length - 1);`,
    `  if (last >= 0xd800 && last <= 0xdbff) prefix = prefix.slice(0, -1);`,
    `  return prefix + "…" + (value.length - prefix.length) + " characters omitted…";`,
    `};`,
    `const __baymaPreview = (value, depth = 0, state = { nodes: 0, seen: new WeakSet() }) => {`,
    `  if (typeof value === "string") return __baymaPreviewString(value);`,
    `  if (value === null || (typeof value !== "object" && typeof value !== "function")) return value;`,
    `  if (state.nodes >= __baymaInspectionPolicy.maxVisitedNodes) return "[inspection node limit]";`,
    `  state.nodes += 1;`,
    `  if (state.seen.has(value)) return "[Circular]";`,
    `  if (depth >= __baymaInspectionPolicy.maxDepth) return "[" + (value?.constructor?.name || typeof value) + " depth limit]";`,
    `  state.seen.add(value);`,
    `  if (value instanceof Date) return Number.isNaN(value.getTime()) ? "Invalid Date" : value.toISOString();`,
    `  if (value instanceof RegExp) return String(value);`,
    `  if (value instanceof Error) return { name: value.name, message: __baymaPreviewString(value.message), stack: __baymaPreviewString(value.stack || "") };`,
    `  if (ArrayBuffer.isView(value)) {`,
    `    const length = typeof value.length === "number" ? value.length : value.byteLength;`,
    `    const shown = Array.from(value.slice?.(0, __baymaInspectionPolicy.maxEntries) ?? []);`,
    `    if (length > shown.length) shown.push("…" + (length - shown.length) + " entries omitted…");`,
    `    return { [value.constructor?.name || "TypedArray"]: shown };`,
    `  }`,
    `  if (Array.isArray(value)) {`,
    `    const shown = [];`,
    `    const count = Math.min(value.length, __baymaInspectionPolicy.maxEntries);`,
    `    for (let index = 0; index < count; index += 1) shown.push(__baymaPreview(value[index], depth + 1, state));`,
    `    if (value.length > count) shown.push("…" + (value.length - count) + " entries omitted…");`,
    `    return shown;`,
    `  }`,
    `  if (value instanceof Map) {`,
    `    const shown = [];`,
    `    let count = 0;`,
    `    for (const [key, entry] of value) {`,
    `      if (count >= __baymaInspectionPolicy.maxEntries) break;`,
    `      shown.push([__baymaPreview(key, depth + 1, state), __baymaPreview(entry, depth + 1, state)]);`,
    `      count += 1;`,
    `    }`,
    `    if (value.size > count) shown.push(["…", value.size - count + " entries omitted"]);`,
    `    return { ["Map(" + value.size + ")"]: shown };`,
    `  }`,
    `  if (value instanceof Set) {`,
    `    const shown = [];`,
    `    let count = 0;`,
    `    for (const entry of value) {`,
    `      if (count >= __baymaInspectionPolicy.maxEntries) break;`,
    `      shown.push(__baymaPreview(entry, depth + 1, state));`,
    `      count += 1;`,
    `    }`,
    `    if (value.size > count) shown.push("…" + (value.size - count) + " entries omitted…");`,
    `    return { ["Set(" + value.size + ")"]: shown };`,
    `  }`,
    `  if (typeof value === "function") return "[Function " + (value.name || "anonymous") + "]";`,
    `  const shown = {};`,
    `  let count = 0;`,
    `  for (const key in value) {`,
    `    if (!Object.prototype.hasOwnProperty.call(value, key)) continue;`,
    `    if (count >= __baymaInspectionPolicy.maxEntries) { shown["…"] = "more properties omitted"; break; }`,
    `    const descriptor = Object.getOwnPropertyDescriptor(value, key);`,
    `    shown[key] = descriptor && "value" in descriptor ? __baymaPreview(descriptor.value, depth + 1, state) : "[Getter/Setter]";`,
    `    count += 1;`,
    `  }`,
    `  return shown;`,
    `};`,
    `const __baymaInspect = (value, rawString = false) => {`,
    `  try {`,
    `    if (rawString && typeof value === "string") return __baymaBoundText(__baymaPreviewString(value));`,
    `    return __baymaBoundText(Bun.inspect(__baymaPreview(value), { colors: false, depth: __baymaInspectionPolicy.maxDepth }));`,
    `  } catch {`,
    `    return "[Bayma could not inspect value]";`,
    `  }`,
    `};`,
    `const __baymaCaughtErrorText = (value) => {`,
    `  try {`,
    `    if (value !== null && (typeof value === "object" || typeof value === "function")) {`,
    `      const message = Reflect.get(value, "message");`,
    `      const stack = Reflect.get(value, "stack");`,
    `      const name = Reflect.get(value, "name");`,
    `      if (typeof message === "string") {`,
    `        if (typeof stack === "string" && stack) return __baymaBoundText(stack);`,
    `        return __baymaBoundText((typeof name === "string" && name ? name + ": " : "") + message);`,
    `      }`,
    `    }`,
    `  } catch {`,
    `  }`,
    `  return __baymaInspect(value);`,
    `};`
  ];
}

// packages/runtime-bun/src/exec.ts
var BUN_CHECKPOINT_CODEC = {
  codecId: "bun-jsc-structured-clone-v1",
  codecVersion: 1,
  payloadKind: "binary-sidecar"
};
function writeBunExecFile(input) {
  const markers = buildMarkers2();
  const workspace = createExecFileWorkspace2(input.rootDir, input.sessionId, input.execId);
  try {
    const execFile = workspace.file("x.ts");
    const checkpointManifestLiteral = JSON.stringify(input.checkpoint?.manifest ?? null);
    const checkpointPayloadPathLiteral = JSON.stringify(input.checkpoint?.payloadAbsolutePath ?? null);
    const checkpointOutputPath = workspace.file("checkpoint.bin");
    const source = [
      `const __baymaEventPrefix = ${JSON.stringify(markers.eventPrefix)};`,
      `const __baymaDurabilityMode = ${JSON.stringify(input.durabilityMode)};`,
      `const __baymaCheckpointManifest = ${checkpointManifestLiteral};`,
      `const __baymaCheckpointPayloadPath = ${checkpointPayloadPathLiteral};`,
      `const __baymaCheckpointOutputPath = ${JSON.stringify(checkpointOutputPath)};`,
      `const { serialize: __baymaSerialize, deserialize: __baymaDeserialize } = await import("bun:jsc");`,
      `const { readFileSync: __baymaReadFileSync, writeFileSync: __baymaWriteFileSync } = await import("node:fs");`,
      `const { join: __baymaJoin } = await import("node:path");`,
      `const __baymaOriginalStdoutWrite = process.stdout.write.bind(process.stdout);`,
      `const __baymaOriginalStderrWrite = process.stderr.write.bind(process.stderr);`,
      `const __baymaTextDecoders = { stdout: new TextDecoder(), stderr: new TextDecoder() };`,
      `const __baymaState = (globalThis.__bayma ??= {});`,
      `const __baymaLoadInitialCheckpoint = () => {`,
      `  if (__baymaDurabilityMode !== "checkpointed" || !__baymaCheckpointManifest) return null;`,
      `  if (__baymaCheckpointManifest.payloadKind === "binary-sidecar") {`,
      `    if (!__baymaCheckpointPayloadPath) throw new Error("checkpoint payload path is missing");`,
      `    return __baymaDeserialize(__baymaReadFileSync(__baymaCheckpointPayloadPath));`,
      `  }`,
      `  if (__baymaCheckpointManifest.payloadKind === "json-inline") {`,
      `    return __baymaCheckpointManifest.inlineJson ?? null;`,
      `  }`,
      `  throw new Error("unsupported Bun checkpoint payload kind " + __baymaCheckpointManifest.payloadKind);`,
      `};`,
      `if (__baymaDurabilityMode === "checkpointed" && !Object.prototype.hasOwnProperty.call(__baymaState, "checkpoint")) {`,
      `  __baymaState.checkpoint = __baymaLoadInitialCheckpoint();`,
      `}`,
      `Object.defineProperty(globalThis, "$checkpoint", {`,
      `  configurable: true,`,
      `  get() {`,
      `    return __baymaState.checkpoint;`,
      `  },`,
      `  set(value) {`,
      `    __baymaState.checkpoint = value;`,
      `  },`,
      `});`,
      ...bunInspectionPrelude(),
      `const __baymaEmit = (kind, payload = {}) => {`,
      `  const boundedPayload = typeof payload.text === "string" ? { ...payload, text: __baymaBoundText(payload.text) } : payload;`,
      `  __baymaOriginalStdoutWrite(__baymaEventPrefix + JSON.stringify({ kind, ...boundedPayload }) + "\\n");`,
      `};`,
      `let __baymaExecRunning = true;`,
      `const __baymaExecDirectory = ${JSON.stringify(dirname(execFile))};`,
      `Object.defineProperty(globalThis, "$displayImage", {`,
      `  configurable: true,`,
      `  value(image) {`,
      `    if (!__baymaExecRunning) throw new Error("$displayImage shows images only while an exec runs");`,
      `    let bytes;`,
      `    if (typeof image === "string" || image instanceof URL) bytes = __baymaReadFileSync(image);`,
      `    else if (ArrayBuffer.isView(image)) bytes = new Uint8Array(image.buffer, image.byteOffset, image.byteLength);`,
      `    else if (image instanceof ArrayBuffer) bytes = new Uint8Array(image);`,
      `    else throw new TypeError("$displayImage takes a path, a file URL, or bytes");`,
      `    const payloadPath = __baymaJoin(__baymaExecDirectory, "image-" + crypto.randomUUID());`,
      `    __baymaWriteFileSync(payloadPath, bytes);`,
      `    __baymaEmit("image", { payloadPath });`,
      `  },`,
      `});`,
      `const __baymaDecodeChunk = (kind, chunk) => {`,
      `  const decoder = __baymaTextDecoders[kind];`,
      `  if (typeof chunk === "string") return decoder.decode() + chunk;`,
      `  if (ArrayBuffer.isView(chunk)) {`,
      `    const bytes = new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength);`,
      `    return decoder.decode(bytes, { stream: true });`,
      `  }`,
      `  throw new TypeError("process stream writes require a string or ArrayBuffer view");`,
      `};`,
      `const __baymaWrapWrite = (kind) => (...args) => {`,
      `  const [chunk] = args;`,
      `  const encodingOrCallback = args[1];`,
      `  const trailingCallback = args[2];`,
      `  if (encodingOrCallback !== undefined && typeof encodingOrCallback !== "string" && typeof encodingOrCallback !== "function") {`,
      `    throw new TypeError("process stream write encoding must be a string");`,
      `  }`,
      `  if (trailingCallback !== undefined && typeof trailingCallback !== "function") {`,
      `    throw new TypeError("process stream write callback must be a function");`,
      `  }`,
      `  const callback = typeof trailingCallback === "function" ? trailingCallback : typeof encodingOrCallback === "function" ? encodingOrCallback : undefined;`,
      `  const text = __baymaDecodeChunk(kind, chunk);`,
      `  if (text) __baymaEmit(kind, { text });`,
      `  if (callback) queueMicrotask(callback);`,
      `  return true;`,
      `};`,
      `const __baymaConsoleMethods = {`,
      `  log: console.log.bind(console),`,
      `  info: console.info.bind(console),`,
      `  debug: console.debug.bind(console),`,
      `  warn: console.warn.bind(console),`,
      `  error: console.error.bind(console),`,
      `};`,
      `const __baymaWrapConsole = (kind) => (...args) => {`,
      `  const rendered = args.map((value) => __baymaInspect(value, true)).join(" ");`,
      `  __baymaEmit(kind, { text: rendered + "\\n" });`,
      `};`,
      `process.stdout.write = __baymaWrapWrite("stdout");`,
      `process.stderr.write = __baymaWrapWrite("stderr");`,
      `console.log = __baymaWrapConsole("stdout");`,
      `console.info = __baymaWrapConsole("stdout");`,
      `console.debug = __baymaWrapConsole("stdout");`,
      `console.warn = __baymaWrapConsole("stderr");`,
      `console.error = __baymaWrapConsole("stderr");`,
      `try {`,
      `  const __baymaTranspiler = new Bun.Transpiler({ loader: "ts", replMode: true });`,
      `  const __baymaWrappedResult = await (0, eval)(__baymaTranspiler.transformSync(${JSON.stringify(input.code)}));`,
      `  const __baymaHasValue =`,
      `    __baymaWrappedResult !== null &&`,
      `    typeof __baymaWrappedResult === "object" &&`,
      `    Object.prototype.hasOwnProperty.call(__baymaWrappedResult, "value");`,
      `  const __baymaValue = __baymaHasValue ? __baymaWrappedResult.value : undefined;`,
      `  if (__baymaHasValue) {`,
      `    if (__baymaValue !== undefined) {`,
      `      globalThis._ = __baymaValue;`,
      `      __baymaEmit("result", { text: __baymaInspect(__baymaValue) });`,
      `    }`,
      `  }`,
      `} catch (__baymaError) {`,
      `  globalThis._error = __baymaError;`,
      `  const __baymaText = __baymaCaughtErrorText(__baymaError);`,
      `  __baymaEmit("error", { text: __baymaBoundText(__baymaText) });`,
      `} finally {`,
      `  if (__baymaDurabilityMode === "checkpointed") {`,
      `    try {`,
      `      const __baymaSerializedCheckpoint = __baymaSerialize(__baymaState.checkpoint ?? null);`,
      `      __baymaWriteFileSync(__baymaCheckpointOutputPath, Buffer.from(__baymaSerializedCheckpoint));`,
      `      __baymaEmit("checkpoint", { checkpoint: {`,
      `        runtimeId: "bun",`,
      `        codecId: ${JSON.stringify(BUN_CHECKPOINT_CODEC.codecId)},`,
      `        codecVersion: ${BUN_CHECKPOINT_CODEC.codecVersion},`,
      `        payloadKind: ${JSON.stringify(BUN_CHECKPOINT_CODEC.payloadKind)},`,
      `        payloadPath: __baymaCheckpointOutputPath,`,
      `        compatibility: { runtimeVersion: Bun.version, platform: process.platform, arch: process.arch },`,
      `      } });`,
      `    } catch (__baymaCheckpointError) {`,
      `      const __baymaCheckpointText = __baymaCaughtErrorText(__baymaCheckpointError);`,
      `      __baymaEmit("error", { text: __baymaBoundText(__baymaCheckpointText) });`,
      `    }`,
      `  }`,
      `  for (const __baymaKind of ["stdout", "stderr"]) {`,
      `    const __baymaTail = __baymaTextDecoders[__baymaKind].decode();`,
      `    if (__baymaTail) __baymaEmit(__baymaKind, { text: __baymaTail });`,
      `  }`,
      `  process.stdout.write = __baymaOriginalStdoutWrite;`,
      `  process.stderr.write = __baymaOriginalStderrWrite;`,
      `  console.log = __baymaConsoleMethods.log;`,
      `  console.info = __baymaConsoleMethods.info;`,
      `  console.debug = __baymaConsoleMethods.debug;`,
      `  console.warn = __baymaConsoleMethods.warn;`,
      `  console.error = __baymaConsoleMethods.error;`,
      `  __baymaExecRunning = false;`,
      `  __baymaEmit("done");`,
      `}`,
      `void 0;`,
      ""
    ].join(`
`);
    writeFileSync(execFile, source, "utf8");
    return {
      submitText: `.load ${execFile}
`,
      collector: createPromptAwareEnvelopeCollector2(markers, [">", "❯", "..."]),
      dispose: workspace.dispose
    };
  } catch (error) {
    workspace.dispose();
    throw error;
  }
}
var bunDoctor = {
  probeCode: "40 + 2",
  successMessage: "ok: Bayma executed JavaScript in the Bun runtime",
  assertSuccess: (exec) => assertDoctorExecResult2(exec, "42")
};

// packages/runtime-bun/src/index.ts
var bunAdapter = {
  runtimeId: "bun",
  displayName: "Bun",
  checkpointCodecs: [JSON_CHECKPOINT_CODEC2, BUN_CHECKPOINT_CODEC],
  modelProfile: bunModelProfile,
  createTransport: createBunTransport,
  prepareExec: writeBunExecFile,
  doctor: bunDoctor
};
export {
  bunAdapter
};
