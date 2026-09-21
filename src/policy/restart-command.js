import { resolveCommandTokens, tokenizeShellCommand, unwrapShellCommandOnce } from "./shell-command.js";

const SERVICE_RESTART_PATTERN = /^(?:(?:\/usr\/bin\/)?sudo(?:\s+-n)?\s+)?(?:\/usr\/bin\/)?systemctl\s+restart\s+alasio(?:\.service)?$/i;
const SERVICE_RESTART_NEAR_MISS_PATTERN = /^(?:(?:\/usr\/bin\/)?sudo(?:\s+-n)?\s+)?(?:\/usr\/bin\/)?systemctl\s+restart\s+alasio(?:\.service)?(?:\s+.+)?$/i;
const ALASIO_CD_PREFIX_PATTERN = /^cd\s+(?:"\/home\/operator\/monorepo-alasio-runtime(?:\/bots\/alasio)?"|'\/home\/operator\/monorepo-alasio-runtime(?:\/bots\/alasio)?'|\/home\/operator\/monorepo-alasio-runtime(?:\/bots\/alasio)?|"bots\/alasio"|'bots\/alasio'|bots\/alasio)\s*&&\s*([\s\S]+)$/i;

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
  if (exe === "restart-alasio-operator.sh") {
    return args.length === 0;
  }
  if ((exe === "bash" || exe === "sh") && args.length === 1) {
    return args[0].split("/").pop() === "restart-alasio-operator.sh";
  }
  return false;
}

function isRestartScriptNearMiss(command) {
  const normalized = stripAlasioCdPrefix(command);
  const { exe, args } = resolveCommandTokens(tokenizeShellCommand(normalized));
  if (exe === "restart-alasio-operator.sh") {
    return args.length > 0;
  }
  if (exe === "bash" || exe === "sh") {
    return args.some((arg) => arg.split("/").pop() === "restart-alasio-operator.sh") && !isRestartScriptInvocation(normalized);
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
