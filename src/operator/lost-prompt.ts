/**
 * The prompt a conversation's turn is run again with once the harness instance it ran in,
 * its pool's pod, was lost before the turn ended (Store.stageLostRecovery): what it ran
 * stopped with the pod, and its session resumes on another, its workspace as it was left;
 * and what the turn's status message says of what became of it.
 */
import { harnessDisplayName, type HarnessName } from "../harness/names.ts";
import type { LostRecoveryOutcome } from "../persistence/store.ts";

/** The recovery prompt of a turn of `harness` whose harness instance was lost. */
export function buildLostHarnessText(harness: HarnessName): string {
  return "[HARNESS INSTANCE LOST]\n\n" +
    `You are ${harnessDisplayName(harness)}, connected through alasio.\n\n` +
    "Fact: the harness instance your prior turn in this conversation ran in was lost before the turn ended, and what it ran stopped with it.\n" +
    "Fact: this session resumes on another instance, its workspace as the lost one left it.\n\n" +
    "Instruction: do not automatically resume your prior task as though nothing changed.\n" +
    "Instruction: check what of the interrupted task was done, briefly say so, and ask how the user wants to proceed.";
}

/** What the status message of a lost turn of `harness` says, its recovery come to `outcome`. */
export function lostStatusText(outcome: Exclude<LostRecoveryOutcome["outcome"], "none">, harness: HarnessName): string {
  const harnessName = harnessDisplayName(harness);
  if (outcome === "completed") return `${harnessName} finished this turn as its harness was let go of.`;
  if (outcome === "staged") return `${harnessName}'s harness was lost during this turn; it runs again on another.`;
  return `${harnessName}'s harness was lost during this turn, which was itself run again as one was lost before. Send the message again to run it.`;
}
