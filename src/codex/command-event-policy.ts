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
import type { Logger } from "../shared/log.ts";

export {
  buildDbGuardrailFallbackText,
  buildDbGuardrailSyntheticText,
  MAX_DB_GUARDRAIL_RECOVERY_ATTEMPTS,
};

/** The turn whose commands a policy inspects, and the controller it aborts when one is blocked. */
export interface CommandEventPolicyOptions {
  readonly persistence: RestartEventRecorder;
  readonly threadKey: string;
  readonly chatId: string;
  readonly messageId: string;
  readonly controller: Pick<AbortController, "abort">;
  readonly log: Logger;
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
  inspectCommand(command: InspectedCommand): { readonly blocked: boolean };
  getGuardrailResult(): GuardrailResult;
}

export function createCommandEventPolicy({ persistence, threadKey, chatId, messageId, controller, log }: CommandEventPolicyOptions): CommandEventPolicy {
  const notifiedWorkflowWaits = new Set<string>();
  let guardrailBlocked = false;
  let blockedGuardrailCommand: string | null = null;

  return {
    inspectCommand({ command, sessionId }) {
      if (looksLikeSelfRestartCommand(command)) {
        recordSelfRestartEvent(persistence, sessionId, threadKey, chatId, messageId, command);
        log.info("Recorded self-induced restart event");
      } else if (looksLikeSelfRestartNearMiss(command)) {
        log.warn(`Saw potential alasio restart command that did not match self-restart detector: ${command}`);
      }
      if (sessionId) {
        const workflowWait = detectWorkflowWait(command);
        if (workflowWait) {
          const dedupeKey = `${sessionId}:${workflowWait.runId}:${workflowWait.waitType}`;
          if (!notifiedWorkflowWaits.has(dedupeKey)) {
            notifiedWorkflowWaits.add(dedupeKey);
            void notifyWorkflowWait(sessionId, workflowWait.runId, workflowWait.waitType, command);
          }
        }
      }
      if (!guardrailBlocked && isBlockedDbCommand(command)) {
        guardrailBlocked = true;
        blockedGuardrailCommand = command;
        controller.abort("Blocked by DB guardrail");
        return { blocked: true };
      }
      return { blocked: false };
    },

    getGuardrailResult() {
      return {
        guardrailBlocked,
        blockedGuardrailCommand,
      };
    },
  };
}
