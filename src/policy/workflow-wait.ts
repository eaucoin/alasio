import type { Logger } from "../shared/log.ts";
import { resolveHookPort } from "../shared/runtime-constants.ts";

/** How an agent waits on a GitHub Actions run: watching it, polling it, or checking its status. */
export type WorkflowWaitType = "watch" | "poll" | "check";

/** A wait on a workflow run, as a command an agent ran reveals it. */
export interface DetectedWorkflowWait {
  readonly runId: string;
  readonly waitType: WorkflowWaitType;
}

/** What notifyWorkflowWait posts to the workflow hook server. */
export interface WorkflowHookNotification {
  readonly session_id: string;
  readonly run_id: string;
  readonly wait_type: WorkflowWaitType;
  readonly command: string;
}

const WORKFLOW_WAIT_PATTERNS: readonly { readonly pattern: RegExp; readonly waitType: WorkflowWaitType }[] = [
  { pattern: /gh run watch (\d+)/, waitType: "watch" },
  { pattern: /sleep \d+.*gh run (?:view|list).*?(\d{8,})/, waitType: "poll" },
  { pattern: /gh run view (\d+).*--json.*status/, waitType: "check" },
];

export function detectWorkflowWait(command: string): DetectedWorkflowWait | null {
  for (const { pattern, waitType } of WORKFLOW_WAIT_PATTERNS) {
    const match = pattern.exec(command);
    if (match?.[1]) {
      return { runId: match[1], waitType };
    }
  }
  return null;
}

export async function notifyWorkflowWait(
  sessionId: string,
  runId: string,
  waitType: WorkflowWaitType,
  command: string,
  log: Pick<Logger, "warn"> = console,
): Promise<void> {
  try {
    const response = await fetch(`http://localhost:${resolveHookPort()}/hook/workflow`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        session_id: sessionId,
        run_id: runId,
        wait_type: waitType,
        command,
      } satisfies WorkflowHookNotification),
    });
    if (!response.ok) {
      log.warn(`Workflow hook POST returned ${response.status}`);
    }
  } catch (error) {
    log.warn(`Workflow hook POST failed: ${error}`);
  }
}
