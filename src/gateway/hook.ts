/**
 * Bayma's hook, which a node agent runs in a bound pod's sandbox as it saves it whole
 * (../node-agent/agent.ts checkpoint): bayma, told to stop and save what it serves.
 */
import type { V1Container } from "@kubernetes/client-node";

/**
 * The command that runs bayma's hook in bayma's container: bayma, as its container runs
 * it after the init that starts it (the arguments after its `--`), told the state
 * directory it serves. Null when the container runs no bayma with one. Pure, for tests.
 */
export function hookOf(bayma: V1Container): string[] | null {
  const command = bayma.command ?? [];
  const program = command.includes("--") ? command.slice(command.indexOf("--") + 1) : command;
  const args = bayma.args ?? [];
  const stateDir = args[args.indexOf("--state-dir") + 1];
  return program.length > 0 && args.includes("--state-dir") && stateDir ? [...program, "sandbox-hook", "--state-dir", stateDir] : null;
}
