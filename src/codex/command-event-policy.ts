import { Effect } from "effect";

import type { StoreError } from "../persistence/sql.ts";
import {
  buildDbGuardrailFallbackText,
  buildDbGuardrailSyntheticText,
  isBlockedDbCommand,
  MAX_DB_GUARDRAIL_RECOVERY_ATTEMPTS,
} from "../policy/db-guardrail.ts";
import {
  type RestartEventRecorder,
  looksLikeSelfRestartCommand,
  looksLikeSelfRestartNearMiss,
  recordSelfRestartEvent,
} from "../policy/restart-command.ts";
import { detectWorkflowWait, notifyWorkflowWait } from "../policy/workflow-wait.ts";

export {
  buildDbGuardrailFallbackText,
  buildDbGuardrailSyntheticText,
  MAX_DB_GUARDRAIL_RECOVERY_ATTEMPTS,
};

/** The turn whose commands a policy inspects, and what it does when one is blocked. */
export interface CommandEventPolicyOptions {
  readonly persistence: RestartEventRecorder;
  readonly threadKey: string;
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
  readonly inspectCommand: (command: InspectedCommand) => Effect.Effect<{ readonly blocked: boolean }, StoreError>;
  readonly getGuardrailResult: () => GuardrailResult;
}

export function createCommandEventPolicy({ persistence, threadKey, chatId, messageId, onBlocked }: CommandEventPolicyOptions): CommandEventPolicy {
  const notifiedWorkflowWaits = new Set<string>();
  let guardrailBlocked = false;
  let blockedGuardrailCommand: string | null = null;

  return {
    inspectCommand: Effect.fnUntraced(function*({ command, sessionId }) {
      if (looksLikeSelfRestartCommand(command)) {
        yield* recordSelfRestartEvent(persistence, sessionId, threadKey, chatId, messageId, command);
        yield* Effect.logInfo("Recorded self-induced restart event");
      } else if (looksLikeSelfRestartNearMiss(command)) {
        yield* Effect.logWarning(`Saw potential alasio restart command that did not match self-restart detector: ${command}`);
      }
      if (sessionId) {
        const workflowWait = detectWorkflowWait(command);
        if (workflowWait) {
          const dedupeKey = `${sessionId}:${workflowWait.runId}:${workflowWait.waitType}`;
          if (!notifiedWorkflowWaits.has(dedupeKey)) {
            notifiedWorkflowWaits.add(dedupeKey);
            // The turn goes on while the hook server is told.
            yield* Effect.forkDetach(notifyWorkflowWait({ session_id: sessionId, run_id: workflowWait.runId, wait_type: workflowWait.waitType, command }));
          }
        }
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
