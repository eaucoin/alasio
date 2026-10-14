import { Effect } from "effect";

import type { TurnPersistence, TurnWrite } from "../persistence/store.ts";
import {
  buildDbGuardrailFallbackText,
  buildDbGuardrailSyntheticText,
  isBlockedDbCommand,
  MAX_DB_GUARDRAIL_RECOVERY_ATTEMPTS,
} from "../policy/db-guardrail.ts";
import { looksLikeSelfRestartCommand, looksLikeSelfRestartNearMiss, recordSelfRestartEvent } from "../policy/restart-command.ts";
import { detectWorkflowWait, workflowWaitText } from "../policy/workflow-wait.ts";

export {
  buildDbGuardrailFallbackText,
  buildDbGuardrailSyntheticText,
  MAX_DB_GUARDRAIL_RECOVERY_ATTEMPTS,
};

/** The turn whose commands a policy inspects, and what it does when one is blocked. */
export interface CommandEventPolicyOptions {
  /** Where the turn records a restart its command causes, and a workflow run it waits on. */
  readonly persistence: Pick<TurnPersistence, "recordRestartEvent" | "setWaiting">;
  readonly chatId: string;
  readonly messageId: string;
  /** Run once, with why, when the DB guardrail blocks the turn's first command. */
  readonly onBlocked?: ((reason: string) => Effect.Effect<void>) | undefined;
}

/** A command a turn ran, and the session it ran in. */
export interface InspectedCommand {
  readonly command: string;
  readonly sessionId: string | null | undefined;
}

/** Whether the DB guardrail blocked a command of the turn, and which. */
export interface GuardrailResult {
  readonly guardrailBlocked: boolean;
  readonly blockedGuardrailCommand: string | null;
}

/** What a turn's shell commands set off: restart provenance, workflow waits, and the DB guardrail. */
export interface CommandEventPolicy {
  readonly inspectCommand: (command: InspectedCommand) => TurnWrite<{ readonly blocked: boolean }>;
  readonly getGuardrailResult: () => GuardrailResult;
}

/**
 * The policy of a turn's commands. A command that waits on a workflow run says so on the
 * turn (its status message shows it), until the turn's next command, which works again,
 * or its end.
 */
export function createCommandEventPolicy({ persistence, chatId, messageId, onBlocked }: CommandEventPolicyOptions): CommandEventPolicy {
  let waitingOn: string | null = null;
  let guardrailBlocked = false;
  let blockedGuardrailCommand: string | null = null;

  return {
    inspectCommand: Effect.fnUntraced(function*({ command, sessionId }) {
      if (looksLikeSelfRestartCommand(command)) {
        yield* recordSelfRestartEvent(persistence, sessionId, chatId, messageId, command);
        yield* Effect.logInfo("Recorded self-induced restart event");
      } else if (looksLikeSelfRestartNearMiss(command)) {
        yield* Effect.logWarning(`Saw potential alasio restart command that did not match self-restart detector: ${command}`);
      }
      const workflowWait = detectWorkflowWait(command);
      const waits = workflowWait ? workflowWaitText(workflowWait) : null;
      // What the turn waits on is only shown: the store failing to record it fails no
      // turn, but a turn superseded is told so.
      if (waits !== waitingOn) {
        waitingOn = waits;
        yield* persistence.setWaiting(waits).pipe(
          Effect.catchTag("StoreError", (error) => Effect.logWarning(`could not record that the turn waits on ${waits ?? "nothing"}: ${error.message}`)),
        );
      }
      if (!guardrailBlocked && isBlockedDbCommand(command)) {
        guardrailBlocked = true;
        blockedGuardrailCommand = command;
        if (onBlocked) {
          yield* onBlocked("Blocked by DB guardrail");
        }
        return { blocked: true };
      }
      return { blocked: false };
    }),

    getGuardrailResult: () => ({
      guardrailBlocked,
      blockedGuardrailCommand,
    }),
  };
}
