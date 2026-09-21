import { createServer } from "node:http";

function findThreadKeyBySessionId(store, sessionId) {
  for (const turn of store.getActiveTurns()) {
    if (turn.session_id === sessionId) {
      return turn.thread_key;
    }
  }
  return "";
}

function readJsonBody(request) {
  return new Promise((resolve, reject) => {
    let body = "";
    request.on("data", (chunk) => {
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

export function startWorkflowHookServer({ port, store, workflowWaits, workflowWakeEvents, log }) {
  const server = createServer(async (request, response) => {
    if (request.method !== "POST" || request.url !== "/hook/workflow") {
      response.writeHead(404);
      response.end("Not found");
      return;
    }
    try {
      const data = await readJsonBody(request);
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
