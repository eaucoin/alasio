import { harnessDisplayName } from "../harness/names.js";

export function buildRestartSyntheticText(cause, harness = "codex") {
  const agentName = harnessDisplayName(harness);
  if (cause === "self_induced") {
    return "[SYSTEM RESTART EVENT]\n\n" +
      `You are ${agentName}, connected through \`alasio.service\` on this machine.\n\n` +
      "Fact: the service restart that just occurred was initiated by your own prior action in this conversation.\n" +
      "Fact: the restart completed successfully and this is the post-restart continuation context for the same session.\n\n" +
      "Fact: the documented Alasio restart path is `/home/operator/monorepo-alasio-runtime/bots/alasio/restart-alasio-operator.sh`; from that directory use `./restart-alasio-operator.sh`.\n\n" +
      "Instruction: continue exactly where you left off.\n" +
      "Instruction: do not ask the user whether to continue solely because of this restart.\n" +
      "Instruction: use and document the wrapper path as the normal Alasio restart path, not raw `sudo systemctl restart alasio.service`.\n" +
      "Instruction: if the restart was intended to apply a configuration or code change, verify the expected post-restart state, then proceed with the interrupted task.";
  }
  if (cause === "operator_induced") {
    return "[SYSTEM RESTART EVENT]\n\n" +
      `You are ${agentName}, connected through \`alasio.service\` on this machine.\n\n` +
      "Fact: the service restart that just occurred was explicitly initiated by an operator outside your prior action in this conversation.\n" +
      "Fact: it was not recorded as user-initiated and not recorded as self-induced.\n" +
      "Fact: the documented Alasio restart path is `/home/operator/monorepo-alasio-runtime/bots/alasio/restart-alasio-operator.sh`; from that directory use `./restart-alasio-operator.sh`.\n\n" +
      "Instruction: do not attribute this restart to the user.\n" +
      "Instruction: if the interrupted task was to restart or reload the service, verify the expected post-restart state and continue from the latest checkpoint.\n" +
      "Instruction: otherwise briefly acknowledge the operator interruption and continue only if the remaining intent is still clear.";
  }
  return "[SYSTEM RESTART EVENT]\n\n" +
    `You are ${agentName}, connected through \`alasio.service\` on this machine.\n\n` +
    "Fact: the service restart that just occurred was external to your prior action in this conversation.\n" +
    "Fact: the runtime cannot verify whether it was initiated by the user, an operator, or other external automation.\n" +
    "Fact: the documented Alasio restart path is `/home/operator/monorepo-alasio-runtime/bots/alasio/restart-alasio-operator.sh`; from that directory use `./restart-alasio-operator.sh`.\n\n" +
    "Instruction: do not automatically resume your prior task as though nothing changed.\n" +
    "Instruction: acknowledge the interruption and ask how the user wants to proceed.\n" +
    "Instruction: you may briefly summarize the interrupted task, but do not attribute the restart to the user unless that fact is explicitly recorded.";
}
