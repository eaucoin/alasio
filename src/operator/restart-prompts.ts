import { harnessDisplayName, type HarnessName } from "../harness/names.ts";
import type { RestartCause } from "../persistence/restart-repository.ts";

/** How the post-restart prompts name what alasio runs as and how it is restarted. */
interface RestartPaths {
  readonly unit: string;
  readonly restartFact: string;
  readonly restartPath: string;
  readonly rawRestart: string;
}

/**
 * What this process runs as and how it is restarted, in the words the post-restart
 * prompts use: its Deployment (ALASIO_DEPLOYMENT and ALASIO_NAMESPACE, which the chart
 * sets) and a rollout restart of it.
 */
function resolveRestartPaths(env: NodeJS.ProcessEnv = process.env): RestartPaths {
  const deployment = env["ALASIO_DEPLOYMENT"]?.trim() || "alasio";
  const namespace = env["ALASIO_NAMESPACE"]?.trim() || "alasio";
  return {
    unit: `the Kubernetes Deployment ${deployment} in namespace ${namespace}`,
    restartFact: `Fact: the documented Alasio restart is \`kubectl -n ${namespace} rollout restart deployment/${deployment}\`.\n\n`,
    restartPath: "that command",
    rawRestart: "deleting alasio's pod",
  };
}

export function buildRestartSyntheticText(
  cause: RestartCause,
  harness: HarnessName | null = "codex",
  env: NodeJS.ProcessEnv = process.env,
): string {
  const agentName = harnessDisplayName(harness);
  const { unit, restartFact, restartPath, rawRestart } = resolveRestartPaths(env);
  const header = "[SYSTEM RESTART EVENT]\n\n" +
    `You are ${agentName}, connected through ${unit}.\n\n`;
  if (cause === "self_induced") {
    return header +
      "Fact: the service restart that just occurred was initiated by your own prior action in this conversation.\n" +
      "Fact: the restart completed successfully and this is the post-restart continuation context for the same session.\n\n" +
      restartFact +
      "Instruction: continue exactly where you left off.\n" +
      "Instruction: do not ask the user whether to continue solely because of this restart.\n" +
      `Instruction: use and document ${restartPath} as the normal Alasio restart path, not ${rawRestart}.\n` +
      "Instruction: if the restart was intended to apply a configuration or code change, verify the expected post-restart state, then proceed with the interrupted task.";
  }
  if (cause === "operator_induced") {
    return header +
      "Fact: the service restart that just occurred was explicitly initiated by an operator outside your prior action in this conversation.\n" +
      "Fact: it was not recorded as user-initiated and not recorded as self-induced.\n" +
      restartFact +
      "Instruction: do not attribute this restart to the user.\n" +
      "Instruction: if the interrupted task was to restart or reload the service, verify the expected post-restart state and continue from the latest checkpoint.\n" +
      "Instruction: otherwise briefly acknowledge the operator interruption and continue only if the remaining intent is still clear.";
  }
  return header +
    "Fact: the service restart that just occurred was external to your prior action in this conversation.\n" +
    "Fact: the runtime cannot verify whether it was initiated by the user, an operator, or other external automation.\n" +
    restartFact +
    "Instruction: do not automatically resume your prior task as though nothing changed.\n" +
    "Instruction: acknowledge the interruption and ask how the user wants to proceed.\n" +
    "Instruction: you may briefly summarize the interrupted task, but do not attribute the restart to the user unless that fact is explicitly recorded.";
}
