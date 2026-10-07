/**
 * Recognises an agent restarting alasio, so the restart is recorded as its own doing and
 * its turn continues afterwards as one it caused. alasio is a Kubernetes Deployment,
 * restarted by rolling it out again: `kubectl rollout restart` of `ALASIO_DEPLOYMENT`
 * (alasio unless set), which the host profile lets folder workspaces' agents do.
 */
import type { Effect } from "effect";

import type { StoreError } from "../persistence/sql.ts";
import type { Store } from "../persistence/store.ts";
import { resolveCommandTokens, tokenizeShellCommand, unwrapShellCommandOnce } from "./shell-command.ts";

/** Where the restart detectors read ALASIO_DEPLOYMENT from; the process's environment unless given. */
export interface RestartCommandOptions {
  readonly env?: NodeJS.ProcessEnv;
}

/** Where a self-restart is recorded: alasio's store. */
export type RestartEventRecorder = Pick<Store["Service"], "recordRestartEvent">;

// kubectl's global flags that take a value, which may come before or after the verb.
const KUBECTL_VALUE_FLAGS = new Set(["-n", "--namespace", "--context", "--kubeconfig", "--cluster", "--user", "-s", "--server"]);

/** A `kubectl rollout restart` command's targets, or null for any other command. */
function rolloutRestartTargets(command: string): string[] | null {
  const { exe, args } = resolveCommandTokens(tokenizeShellCommand(command));
  if (exe !== "kubectl") return null;
  const words: string[] = [];
  const rest = args.values();
  for (const arg of rest) {
    if (KUBECTL_VALUE_FLAGS.has(arg)) rest.next();
    else if (!arg.startsWith("-")) words.push(arg);
  }
  const [verb, action, ...targets] = words;
  return verb === "rollout" && action === "restart" ? targets : null;
}

const DEPLOYMENT_KIND = /^(?:deployments?|deploy)(?:\.apps)?$/u;

/** Whether `targets` name alasio's Deployment alone, as `deployment/<name>`, `deploy/<name>` or `deployment <name>`. */
function namesAlasio(targets: readonly string[], env: NodeJS.ProcessEnv): boolean {
  const name = env["ALASIO_DEPLOYMENT"]?.trim() || "alasio";
  const [first] = targets;
  let kind: string | undefined;
  let named: string | undefined;
  if (targets.length === 1 && first?.split("/").length === 2) [kind, named] = first.split("/");
  else if (targets.length === 2 && first !== undefined && DEPLOYMENT_KIND.test(first)) [kind, named] = targets;
  else return false;
  return DEPLOYMENT_KIND.test(kind ?? "") && named === name;
}

function candidateCommands(command: string): string[] {
  const trimmed = command.trim();
  const inner = unwrapShellCommandOnce(trimmed);
  return inner ? [trimmed, inner.trim()] : [trimmed];
}

/** Whether `command` restarts alasio. Flags may come anywhere. */
export function looksLikeSelfRestartCommand(command: string, { env = process.env }: RestartCommandOptions = {}): boolean {
  return candidateCommands(command).some((candidate) => {
    const targets = rolloutRestartTargets(candidate);
    return targets !== null && namesAlasio(targets, env);
  });
}

/**
 * Whether `command` is a rollout restart that mentions alasio's Deployment without being
 * alasio's own restart (more targets, say), which is worth a warning in the log.
 */
export function looksLikeSelfRestartNearMiss(command: string, { env = process.env }: RestartCommandOptions = {}): boolean {
  if (looksLikeSelfRestartCommand(command, { env })) return false;
  const name = env["ALASIO_DEPLOYMENT"]?.trim() || "alasio";
  return candidateCommands(command).some((candidate) => (
    rolloutRestartTargets(candidate)?.some((target) => target === name || target.endsWith(`/${name}`)) ?? false
  ));
}

export function recordSelfRestartEvent(
  persistence: RestartEventRecorder,
  sessionId: string | null | undefined,
  threadKey: string,
  chatId: string,
  messageId: string,
  command: string,
): Effect.Effect<void, StoreError> {
  return persistence.recordRestartEvent({
    cause: "self_induced",
    thread_key: threadKey,
    channel: chatId,
    thread_ts: messageId,
    session_id: sessionId ?? null,
    command,
  });
}
