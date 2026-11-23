#!/usr/bin/env -S node --disable-warning=ExperimentalWarning

// packages/core/src/errors.ts
function describeFailureInner(error, activeAggregates) {
  if (error instanceof AggregateError) {
    if (activeAggregates.has(error)) {
      return "AggregateError: circular aggregate cause";
    }
    activeAggregates.add(error);
    const nested = error.errors.map((nestedError) => describeFailureInner(nestedError, activeAggregates)).join("; ");
    activeAggregates.delete(error);
    const suffix = nested ? `: ${nested}` : "";
    return suffix && error.message.endsWith(suffix) ? error.message : `${error.message}${suffix}`;
  }
  if (error instanceof Error)
    return `${error.name}: ${error.message}`;
  return String(error);
}
function describeFailure(error) {
  return describeFailureInner(error, new Set);
}
function aggregateFailure(message, failures) {
  const details = failures.map(describeFailure).join("; ");
  return new AggregateError(failures, details ? `${message}: ${details}` : message);
}
function failureDetail(error) {
  return error instanceof Error ? error.stack || error.message : String(error);
}
function failureMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

// packages/core/src/mcp/sandbox-hook-client.ts
import { Buffer } from "node:buffer";
import { realpathSync } from "node:fs";
import net from "node:net";
import { join, resolve } from "node:path";

// packages/core/src/cli-options.ts
function parseCliOptions(args, allowedNames) {
  const allowed = new Set(allowedNames);
  const parsed = new Map;
  for (let index = 0;index < args.length; index += 2) {
    const name = args[index];
    const value = args[index + 1];
    if (!allowed.has(name))
      throw new Error(`unknown option ${name}`);
    if (parsed.has(name))
      throw new Error(`option ${name} may be provided only once`);
    if (value === undefined || value.startsWith("--")) {
      throw new Error(`option ${name} requires one value`);
    }
    parsed.set(name, value);
  }
  return parsed;
}

// packages/core/src/mcp/sandbox-hook-client.ts
var SANDBOX_HOOK_MODES = [
  "save",
  "restore",
  "resume",
  "stop"
];
var SANDBOX_HOOK_MODE_VARIABLE = "GVISOR_SAVE_RESTORE_AUTO_EXEC_MODE";
var SANDBOX_STOP_REASON_VARIABLE = "BAYMA_SANDBOX_STOP_REASON";
var MAX_STOP_REASON_BYTES = 256;
var LEAVE_RUNNING_OPTION = "--leave-running";
var LEAVES_RUNNING = "leave-running";
function sandboxHookSocket(stateDir) {
  return join(stateDir, "sandbox-hook.sock");
}
function parseSandboxHookMode(value) {
  if (!SANDBOX_HOOK_MODES.includes(value))
    throw new Error(`the sandbox hook's mode must be ${SANDBOX_HOOK_MODES.join(", ")}, not ${value ?? "none"}`);
  return value;
}
function formatSandboxHookRequest(request) {
  switch (request.mode) {
    case "save":
      return request.leavesRunning ? `save ${LEAVES_RUNNING}` : "save";
    case "restore":
      return request.stopReason === undefined ? "restore" : `restore ${request.stopReason}`;
    default:
      return request.mode;
  }
}
function parseSandboxHookRequest(line) {
  const space = line.indexOf(" ");
  const mode = parseSandboxHookMode(space === -1 ? line : line.slice(0, space));
  const told = space === -1 ? undefined : line.slice(space + 1);
  switch (mode) {
    case "save":
      if (told !== undefined && told !== LEAVES_RUNNING)
        throw new Error(`a save is told nothing past its mode but ${LEAVES_RUNNING}`);
      return { mode, leavesRunning: told !== undefined };
    case "restore":
      return { mode, stopReason: parseSandboxStopReason(told) };
    default:
      if (told !== undefined)
        throw new Error(`a ${mode} is told nothing past its mode`);
      return { mode };
  }
}
function parseSandboxStopReason(value, from) {
  if (value === undefined || value === "")
    return;
  const of = from === undefined ? "" : ` (${from})`;
  if (/[\r\n]/u.test(value))
    throw new Error(`the reason the sandbox stopped must be one line${of}`);
  if (Buffer.byteLength(value) > MAX_STOP_REASON_BYTES)
    throw new Error(`the reason the sandbox stopped must be at most ${MAX_STOP_REASON_BYTES} bytes${of}`);
  return value;
}
async function runSandboxHook(stateDir, mode, {
  leavesRunning = false,
  stopReason
} = {}) {
  const request = `${formatSandboxHookRequest(mode === "save" ? { mode, leavesRunning } : mode === "restore" ? {
    mode,
    stopReason: parseSandboxStopReason(stopReason, SANDBOX_STOP_REASON_VARIABLE)
  } : { mode })}
`;
  const path = sandboxHookSocket(realpathSync(resolve(stateDir)));
  const answer = await new Promise((resolveAnswer, rejectAnswer) => {
    let received = "";
    const socket = net.connect(path, () => socket.write(request));
    socket.setEncoding("utf8");
    socket.on("data", (chunk) => {
      received += chunk;
    });
    socket.on("end", () => resolveAnswer(received));
    socket.on("error", (error) => rejectAnswer(new Error(`no bayma server answers on ${path}: ${failureMessage(error)}`, { cause: error })));
  });
  if (answer === `ok
`)
    return;
  if (answer.startsWith("ok ") && answer.endsWith(`
`))
    return answer.slice("ok ".length, -1);
  throw new Error(`the bayma server of ${stateDir} could not ${mode}: ${answer.trimEnd() || "it closed the connection"}`);
}
async function runSandboxHookCommand(args) {
  const options = args.filter((arg) => arg !== LEAVE_RUNNING_OPTION);
  if (args.length - options.length > 1)
    throw new Error(`option ${LEAVE_RUNNING_OPTION} may be provided only once`);
  const named = options.length % 2 === 1 ? options[0] : undefined;
  const parsed = parseCliOptions(named === undefined ? options : options.slice(1), ["--state-dir"]);
  const stateDir = parsed.get("--state-dir");
  if (stateDir === undefined)
    throw new Error("sandbox-hook needs --state-dir, its server's");
  const mode = parseSandboxHookMode(named ?? process.env[SANDBOX_HOOK_MODE_VARIABLE]);
  const caught = await runSandboxHook(stateDir, mode, {
    leavesRunning: options.length < args.length,
    stopReason: process.env[SANDBOX_STOP_REASON_VARIABLE]
  });
  if (caught !== undefined)
    console.log(caught);
}

// packages/server/src/main.ts
async function main() {
  const [command, ...args] = process.argv.slice(2);
  if (command === "sandbox-hook")
    return runSandboxHookCommand(args);
  const [
    { runCli },
    { bunAdapter },
    { cAdapter, cppAdapter },
    { dotnetScriptAdapter },
    { goAdapter },
    { leanAdapter },
    { pythonAdapter },
    { rustAdapter }
  ] = await Promise.all([
    import("./index-rh2bd4s0.js"),
    import("./index-y57sz0ak.js"),
    import("./index-s7mgct2h.js"),
    import("./index-qfqa7kde.js"),
    import("./index-wdmbxvcs.js"),
    import("./index-pjwfyfh9.js"),
    import("./index-29rh582a.js"),
    import("./index-k3fhkpx4.js")
  ]);
  return runCli([
    bunAdapter,
    pythonAdapter,
    dotnetScriptAdapter,
    rustAdapter,
    cAdapter,
    cppAdapter,
    leanAdapter,
    goAdapter
  ]);
}
main().catch((error) => {
  process.stderr.write(failureDetail(error) + `
`);
  process.exit(1);
});
export {
  aggregateFailure,
  failureDetail,
  failureMessage,
  parseCliOptions,
  MAX_STOP_REASON_BYTES,
  sandboxHookSocket,
  parseSandboxHookRequest,
  runSandboxHookCommand
};
