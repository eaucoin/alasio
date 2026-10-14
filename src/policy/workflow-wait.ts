/** How an agent waits on a GitHub Actions run: watching it, polling it, or checking its status. */
export type WorkflowWaitType = "watch" | "poll" | "check";

/** A wait on a workflow run, as a command an agent ran reveals it. */
export interface DetectedWorkflowWait {
  readonly runId: string;
  readonly waitType: WorkflowWaitType;
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

/** What a turn waiting on `wait` says it waits on (turns.waiting_on), as its status message shows it. */
export function workflowWaitText({ runId, waitType }: DetectedWorkflowWait): string {
  return `workflow run ${runId} (${waitType})`;
}
