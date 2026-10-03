// @ts-nocheck
import { resolveHookPort } from "../shared/runtime-constants.ts";

const WORKFLOW_WAIT_PATTERNS = [
  { pattern: /gh run watch (\d+)/, waitType: "watch" },
  { pattern: /sleep \d+.*gh run (?:view|list).*?(\d{8,})/, waitType: "poll" },
  { pattern: /gh run view (\d+).*--json.*status/, waitType: "check" },
];

export function detectWorkflowWait(command) {
  for (const { pattern, waitType } of WORKFLOW_WAIT_PATTERNS) {
    const match = pattern.exec(command);
    if (match?.[1]) {
      return { runId: match[1], waitType };
    }
  }
  return null;
}

export async function notifyWorkflowWait(sessionId, runId, waitType, command, log = console) {
  try {
    const response = await fetch(`http://localhost:${resolveHookPort()}/hook/workflow`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        session_id: sessionId,
        run_id: runId,
        wait_type: waitType,
        command,
      }),
    });
    if (!response.ok) {
      log.warn(`Workflow hook POST returned ${response.status}`);
    }
  } catch (error) {
    log.warn(`Workflow hook POST failed: ${error}`);
  }
}
