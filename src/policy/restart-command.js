import { resolveCommandTokens, tokenizeShellCommand, unwrapShellCommandOnce } from "./shell-command.js";

// Both the deployment unit (alasio.service) and the standalone checkout unit
// (alasio-standalone.service) count as self-restarts; each has its own wrapper.
const SERVICE_RESTART_PATTERN = /^(?:(?:\/usr\/bin\/)?sudo(?:\s+-n)?\s+)?(?:\/usr\/bin\/)?systemctl\s+restart\s+alasio(?:-standalone)?(?:\.service)?$/i;
const SERVICE_RESTART_NEAR_MISS_PATTERN = /^(?:(?:\/usr\/bin\/)?sudo(?:\s+-n)?\s+)?(?:\/usr\/bin\/)?systemctl\s+restart\s+alasio(?:-standalone)?(?:\.service)?(?:\s+.+)?$/i;
const ALASIO_CD_PREFIX_PATTERN = /^cd\s+(?:"\/home\/operator\/monorepo-alasio-runtime(?:\/bots\/alasio)?"|'\/home\/operator\/monorepo-alasio-runtime(?:\/bots\/alasio)?'|\/home\/operator\/monorepo-alasio-runtime(?:\/bots\/alasio)?|"\/home\/operator\/alasio"|'\/home\/operator\/alasio'|\/home\/operator\/alasio|"bots\/alasio"|'bots\/alasio'|bots\/alasio)\s*&&\s*([\s\S]+)$/i;
const RESTART_WRAPPER_NAMES = new Set(["restart-alasio-operator.sh", "restart-alasio-standalone.sh"]);

function candidateCommands(command) {
  const trimmed = command.trim();
  const candidates = [trimmed];
  const innerCommand = unwrapShellCommandOnce(trimmed);
  if (innerCommand) {
    candidates.push(innerCommand.trim());
  }
  return candidates;
}

function stripAlasioCdPrefix(command) {
  const match = ALASIO_CD_PREFIX_PATTERN.exec(command.trim());
  return match?.[1]?.trim() ?? command.trim();
}

function isRestartScriptInvocation(command) {
  const normalized = stripAlasioCdPrefix(command);
  const { exe, args } = resolveCommandTokens(tokenizeShellCommand(normalized));
  if (RESTART_WRAPPER_NAMES.has(exe)) {
    return args.length === 0;
  }
  if ((exe === "bash" || exe === "sh") && args.length === 1) {
    return RESTART_WRAPPER_NAMES.has(args[0].split("/").pop());
  }
  return false;
}

function isRestartScriptNearMiss(command) {
  const normalized = stripAlasioCdPrefix(command);
  const { exe, args } = resolveCommandTokens(tokenizeShellCommand(normalized));
  if (RESTART_WRAPPER_NAMES.has(exe)) {
    return args.length > 0;
  }
  if (exe === "bash" || exe === "sh") {
    return args.some((arg) => RESTART_WRAPPER_NAMES.has(arg.split("/").pop())) && !isRestartScriptInvocation(normalized);
  }
  return false;
}

export function looksLikeSelfRestartCommand(command) {
  return candidateCommands(command).some((candidate) => (
    SERVICE_RESTART_PATTERN.test(candidate) || isRestartScriptInvocation(candidate)
  ));
}

export function looksLikeSelfRestartNearMiss(command) {
  if (looksLikeSelfRestartCommand(command)) {
    return false;
  }
  return candidateCommands(command).some((candidate) => (
    SERVICE_RESTART_NEAR_MISS_PATTERN.test(candidate) || isRestartScriptNearMiss(candidate)
  ));
}

export function recordSelfRestartEvent(persistence, sessionId, threadKey, chatId, messageId, command) {
  persistence.recordRestartEvent({
    cause: "self_induced",
    thread_key: threadKey,
    channel: chatId,
    thread_ts: messageId,
    session_id: sessionId ?? null,
    command,
    timestamp: Date.now() / 1000,
  });
}
