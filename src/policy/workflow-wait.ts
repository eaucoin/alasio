import { Effect } from "effect";

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

/** Tells the workflow hook server (src/workflow/hook-server.ts) of a wait; a failure to is only logged. */
export const notifyWorkflowWait = (notification: WorkflowHookNotification): Effect.Effect<void> =>
  Effect.tryPromise(() =>
    fetch(`http://localhost:${resolveHookPort()}/hook/workflow`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(notification),
    })
  ).pipe(
    Effect.flatMap((response) => (response.ok ? Effect.void : Effect.logWarning(`Workflow hook POST returned ${response.status}`))),
    Effect.catch((error) => Effect.logWarning(`Workflow hook POST failed: ${error.cause}`)),
  );
