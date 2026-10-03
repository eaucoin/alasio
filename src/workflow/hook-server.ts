import { createServer, type IncomingMessage, type Server } from "node:http";

import type { Turn } from "../persistence/turn-repository.ts";
import type { WorkflowHookNotification, WorkflowWaitType } from "../policy/workflow-wait.ts";
import type { Logger } from "../shared/log.ts";

/** A wait on a workflow run an agent reported for its session, shown in the turn's status. */
export interface WorkflowWait {
  readonly runId: string;
  readonly waitType: WorkflowWaitType | "unknown";
  readonly command: string;
  readonly threadKey: string;
  readonly startedAt: number;
}

/** Wakes a turn's status loop as soon as a workflow wait is reported for its session. */
export interface WorkflowWakeEvent {
  readonly promise: Promise<void>;
  resolve(): void;
}

/** Where the active turns are read from: alasio's store. */
export interface ActiveTurnSource {
  getActiveTurns(): readonly Pick<Turn, "session_id" | "thread_key">[];
}

export interface WorkflowHookServerOptions {
  readonly port: number;
  readonly store: ActiveTurnSource;
  readonly workflowWaits: Map<string, WorkflowWait>;
  readonly workflowWakeEvents: ReadonlyMap<string, WorkflowWakeEvent>;
  readonly log: Logger;
}

function findThreadKeyBySessionId(store: ActiveTurnSource, sessionId: string): string {
  for (const turn of store.getActiveTurns()) {
    if (turn.session_id === sessionId) {
      return turn.thread_key;
    }
  }
  return "";
}

function readJsonBody(request: IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    let body = "";
    request.on("data", (chunk: Buffer) => {
      body += chunk.toString();
    });
    request.on("end", () => {
      try {
        resolve(JSON.parse(body));
      } catch (error) {
        reject(error);
      }
    });
    request.on("error", reject);
  });
}

export function startWorkflowHookServer({ port, store, workflowWaits, workflowWakeEvents, log }: WorkflowHookServerOptions): Server {
  const server = createServer(async (request, response) => {
    if (request.method !== "POST" || request.url !== "/hook/workflow") {
      response.writeHead(404);
      response.end("Not found");
      return;
    }
    try {
      // The body is what notifyWorkflowWait posts; its fields are checked or defaulted below.
      const data = await readJsonBody(request) as Partial<WorkflowHookNotification>;
      const sessionId = data.session_id;
      const runId = data.run_id;
      if (!sessionId || !runId) {
        response.writeHead(400);
        response.end("Missing session_id or run_id");
        return;
      }
      workflowWaits.set(sessionId, {
        runId,
        waitType: data.wait_type ?? "unknown",
        command: data.command ?? "",
        threadKey: findThreadKeyBySessionId(store, sessionId),
        startedAt: Date.now() / 1000,
      });
      workflowWakeEvents.get(sessionId)?.resolve();
      response.writeHead(200);
      response.end("OK");
    } catch (error) {
      log.error(`Error handling workflow hook: ${error}`);
      response.writeHead(500);
      response.end(String(error));
    }
  });
  server.listen(port, "localhost", () => {
    log.info(`Hook server started on localhost:${port}`);
  });
  return server;
}
