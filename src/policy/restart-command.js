import { resolveCommandTokens, tokenizeShellCommand, unwrapShellCommandOnce } from "./shell-command.js";

// Both the deployment unit (alasio.service) and the standalone checkout unit
// (alasio-standalone.service) count as self-restarts; each has its own wrapper.
const SERVICE_RESTART_PATTERN = /^(?:(?:\/usr\/bin\/)?sudo(?:\s+-n)?\s+)?(?:\/usr\/bin\/)?systemctl\s+restart\s+alasio(?:-standalone)?(?:\.service)?$/i;
const SERVICE_RESTART_NEAR_MISS_PATTERN = /^(?:(?:\/usr\/bin\/)?sudo(?:\s+-n)?\s+)?(?:\/usr\/bin\/)?systemctl\s+restart\s+alasio(?:-standalone)?(?:\.service)?(?:\s+.+)?$/i;
const ALASIO_CD_PREFIX_PATTERN = /^cd\s+(?:"\/home\/operator\/monorepo-alasio-runtime(?:\/bots\/alasio)?"|'\/home\/operator\/monorepo-alasio-runtime(?:\/bots\/alasio)?'|\/home\/operator\/monorepo-alasio-runtime(?:\/bots\/alasio)?|"\/home\/operator\/alasio"|'\/home\/operator\/alasio'|\/home\/operator\/alasio|"bots\/alasio"|'bots\/alasio'|bots\/alasio)\s*&&\s*([\s\S]+)$/i;
const RESTART_WRAPPER_NAMES = new Set(["restart-alasio-operator.sh", "restart-alasio-standalone.sh"]);

// kubectl's global flags that take a value, which may come before or after the verb.
const KUBECTL_VALUE_FLAGS = new Set(["-n", "--namespace", "--context", "--kubeconfig", "--cluster", "--user", "-s", "--server"]);

/**
 * `kubectl rollout restart` of alasio's own Deployment, `ALASIO_DEPLOYMENT` (alasio unless
 * set): on Kubernetes, how alasio is restarted. Flags may come anywhere, and the
 * Deployment is named as `deployment/<name>`, `deploy/<name>` or `deployment <name>`.
 */
function isRolloutRestart(command, env) {
  const { exe, args } = resolveCommandTokens(tokenizeShellCommand(stripAlasioCdPrefix(command)));
  if (exe !== "kubectl") return false;
  const words = [];
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (KUBECTL_VALUE_FLAGS.has(arg)) index += 1;
    else if (!arg.startsWith("-")) words.push(arg);
  }
  const name = env.ALASIO_DEPLOYMENT?.trim() || "alasio";
  const [verb, action, ...targets] = words;
  if (verb !== "rollout" || action !== "restart") return false;
  const target = targets.length === 2 ? targets.join("/") : targets[0];
  return targets.length <= 2 && /^(?:deployments?|deploy)(?:\.apps)?\//u.test(target ?? "") && target.split("/")[1] === name;
}

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

export function looksLikeSelfRestartCommand(command, env = process.env) {
  return candidateCommands(command).some((candidate) => (
    SERVICE_RESTART_PATTERN.test(candidate) || isRestartScriptInvocation(candidate) || isRolloutRestart(candidate, env)
  ));
}

export function looksLikeSelfRestartNearMiss(command, env = process.env) {
  if (looksLikeSelfRestartCommand(command, env)) {
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
