/**
 * Recognises an agent restarting alasio, so the restart is recorded as its own doing and
 * its turn continues afterwards as one it caused. alasio is a Kubernetes Deployment,
 * restarted by rolling it out again: `kubectl rollout restart` of `ALASIO_DEPLOYMENT`
 * (alasio unless set), which the chart's host profile lets folder workspaces' agents do.
 */
import { resolveCommandTokens, tokenizeShellCommand, unwrapShellCommandOnce } from "./shell-command.js";

// kubectl's global flags that take a value, which may come before or after the verb.
const KUBECTL_VALUE_FLAGS = new Set(["-n", "--namespace", "--context", "--kubeconfig", "--cluster", "--user", "-s", "--server"]);

/** A `kubectl rollout restart` command's targets, or null for any other command. */
function rolloutRestartTargets(command) {
  const { exe, args } = resolveCommandTokens(tokenizeShellCommand(command));
  if (exe !== "kubectl") return null;
  const words = [];
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (KUBECTL_VALUE_FLAGS.has(arg)) index += 1;
    else if (!arg.startsWith("-")) words.push(arg);
  }
  const [verb, action, ...targets] = words;
  return verb === "rollout" && action === "restart" ? targets : null;
}

const DEPLOYMENT_KIND = /^(?:deployments?|deploy)(?:\.apps)?$/u;

/** Whether `targets` name alasio's Deployment alone, as `deployment/<name>`, `deploy/<name>` or `deployment <name>`. */
function namesAlasio(targets, env) {
  const name = env.ALASIO_DEPLOYMENT?.trim() || "alasio";
  let kind;
  let named;
  if (targets.length === 1 && targets[0].split("/").length === 2) [kind, named] = targets[0].split("/");
  else if (targets.length === 2 && DEPLOYMENT_KIND.test(targets[0])) [kind, named] = targets;
  else return false;
  return DEPLOYMENT_KIND.test(kind ?? "") && named === name;
}

function candidateCommands(command) {
  const trimmed = command.trim();
  const inner = unwrapShellCommandOnce(trimmed);
  return inner ? [trimmed, inner.trim()] : [trimmed];
}

/** Whether `command` restarts alasio. Flags may come anywhere. */
export function looksLikeSelfRestartCommand(command, { env = process.env } = {}) {
  return candidateCommands(command).some((candidate) => {
    const targets = rolloutRestartTargets(candidate);
    return targets !== null && namesAlasio(targets, env);
  });
}

/**
 * Whether `command` is a rollout restart that mentions alasio's Deployment without being
 * alasio's own restart (more targets, say), which is worth a warning in the log.
 */
export function looksLikeSelfRestartNearMiss(command, { env = process.env } = {}) {
  if (looksLikeSelfRestartCommand(command, { env })) return false;
  const name = env.ALASIO_DEPLOYMENT?.trim() || "alasio";
  return candidateCommands(command).some((candidate) => (
    rolloutRestartTargets(candidate)?.some((target) => target === name || target.endsWith(`/${name}`)) ?? false
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
