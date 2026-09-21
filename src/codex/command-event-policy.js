import {
  buildDbGuardrailFallbackText,
  buildDbGuardrailSyntheticText,
  isBlockedDbCommand,
  MAX_DB_GUARDRAIL_RECOVERY_ATTEMPTS,
} from "../policy/db-guardrail.js";
import { looksLikeSelfRestartCommand, looksLikeSelfRestartNearMiss, recordSelfRestartEvent } from "../policy/restart-command.js";
import { detectWorkflowWait, notifyWorkflowWait } from "../policy/workflow-wait.js";

export {
  buildDbGuardrailFallbackText,
  buildDbGuardrailSyntheticText,
  MAX_DB_GUARDRAIL_RECOVERY_ATTEMPTS,
};

export function createCommandEventPolicy({ persistence, threadKey, chatId, messageId, controller, log }) {
  const notifiedWorkflowWaits = new Set();
  let guardrailBlocked = false;
  let blockedGuardrailCommand = null;

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
