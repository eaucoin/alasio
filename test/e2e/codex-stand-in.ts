#!/usr/bin/env node
/**
 * A stand-in for Codex's app-server, for the end-to-end run, which has no Codex login:
 * the run's install has alasio run it as Codex (ALASIO_CODEX_BIN), from the image
 * ./codex-stand-in.Dockerfile builds. It speaks the app-server's JSON-RPC over stdio as
 * Codex does, as far as alasio's turns go: threads start, resume and are listed, and each
 * turn is answered at once with ANSWER as its final answer. Every request is a server
 * span named by its method, of the trace alasio sends it in when it does, with the name
 * the client initialized with, exported where alasio's `-c otel.trace_exporter=` says,
 * from the service codex-app-server, as Codex's own are. Each thread's rollout is written
 * as Codex writes it, under `$CODEX_HOME/sessions/`: its meta, and each turn's context,
 * start, messages and completion, all written before the turn is said to be done, so
 * alasio mirrors and the lake loads them as Codex's. It keeps its threads in memory
 * only, so a thread it is asked to resume that it has not seen, one an earlier process
 * ran, is resumed empty, in a rollout of its own.
 *
 *   codex-stand-in.ts app-server [-c key=value]... --listen stdio://
 */
import { randomUUID } from "node:crypto";
import { appendFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { createInterface } from "node:readline";

import { context, propagation, type Span, SpanKind, trace } from "@opentelemetry/api";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { NodeSDK } from "@opentelemetry/sdk-node";

import type { ClientRequest, RequestId, ServerNotification, v2 } from "../../.types/codex/index.js";
import { ALASIO_CODEX_MODEL } from "../../src/codex/model.ts";
import {
  agentMessage,
  codexModel,
  codexThread,
  codexTurn,
  initializeResponse,
  itemCompleted,
  threadResumeResponse,
  threadStartResponse,
  turnCompleted,
  turnStarted,
  userMessage,
} from "../support/codex-protocol.ts";

/** What every turn answers. */
export const ANSWER = "Answered by the end-to-end run's stand-in for Codex.";

/** A line of a rollout, as Codex writes one: its time, its type, and what it says. */
function rolloutLine(type: string, payload: object): string {
  return `${JSON.stringify({ timestamp: new Date().toISOString(), type, payload })}\n`;
}

/**
 * Where a thread's rollout is under the Codex home `home`, as Codex names it: by the day
 * and the second it was made, and the thread's id.
 */
export function rolloutPath(home: string, threadId: string, made: Date): string {
  const [day = "", time = ""] = made.toISOString().split("T");
  return join(home, "sessions", ...day.split("-"), `rollout-${day}T${time.slice(0, 8).replaceAll(":", "-")}-${threadId}.jsonl`);
}

/** A request as alasio writes it: with the trace it is part of, when it is part of one. */
type Request = ClientRequest & { readonly trace?: { readonly traceparent?: string; readonly tracestate?: string } };

/** What answers each request of a method the stand-in answers: its result, given its params. */
type Answers = { readonly [M in ClientRequest["method"]]?: (params: Extract<ClientRequest, { method: M }>["params"], span: Span) => unknown };

/** A TOML inline table of quoted keys and strings, as alasio writes a `-c` value (src/codex/app-server/telemetry.ts), read as JSON. */
export function inlineTable(value: string): unknown {
  let json = "";
  for (let index = 0; index < value.length; index++) {
    if (value[index] === '"') {
      let end = index + 1;
      while (value[end] !== '"') end += value[end] === "\\" ? 2 : 1;
      json += value.slice(index, end + 1);
      index = end;
    } else {
      json += value[index] === "=" ? ":" : value[index];
    }
  }
  return JSON.parse(json);
}

/** The exporter a `-c otel.trace_exporter=` value names, as Codex reads it. */
interface TraceExporter {
  readonly "otlp-http"?: { readonly endpoint: string; readonly headers: Record<string, string>; readonly protocol: "binary" | "json" };
  readonly "otlp-grpc"?: { readonly endpoint: string; readonly headers: Record<string, string> };
}

/** The OpenTelemetry SDK's environment for exporting spans as `exporter` says, and nothing else. */
export function traceExportEnv(exporter: TraceExporter): Record<string, string> {
  const http = exporter["otlp-http"];
  const { endpoint, headers } = http ?? exporter["otlp-grpc"] ?? { endpoint: "", headers: {} };
  return {
    OTEL_TRACES_EXPORTER: "otlp",
    OTEL_METRICS_EXPORTER: "none",
    OTEL_LOGS_EXPORTER: "none",
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: endpoint,
    OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: http ? (http.protocol === "json" ? "http/json" : "http/protobuf") : "grpc",
    OTEL_EXPORTER_OTLP_TRACES_HEADERS: Object.entries(headers).map(([key, value]) => `${key}=${value}`).join(","),
  };
}

/** Runs the app-server stand-in, given Codex's arguments after `app-server`, until alasio closes its input or stops it. */
async function serve(args: readonly string[]): Promise<void> {
  const settings = new Map(args.flatMap((arg, index) => {
    const setting = args[index - 1] === "-c" ? /^([^=]+)=(.*)$/su.exec(arg) : null;
    return setting?.[1] && setting[2] !== undefined ? [[setting[1], setting[2]] as const] : [];
  }));
  const exporter = settings.get("otel.trace_exporter");
  // Read by the SDK as it starts; set as alasio sets Codex's.
  if (exporter) Object.assign(process.env, traceExportEnv(inlineTable(exporter) as TraceExporter));
  const sdk = exporter ? new NodeSDK({ resource: resourceFromAttributes({ "service.name": "codex-app-server" }) }) : null;
  sdk?.start();
  const tracer = trace.getTracer("codex-app-server");

  const threads = new Map<string, v2.Thread>();
  /** The client's name, once it has initialized. */
  let client: string | undefined;
  const write = (message: object) => process.stdout.write(`${JSON.stringify(message)}\n`);
  const notify = (notification: ServerNotification) => write(notification);
  const now = () => Math.floor(Date.now() / 1000);

  /** Each thread's rollout, by its id. */
  const rollouts = new Map<string, string>();
  const record = (threadId: string, type: string, payload: object) => {
    const path = rollouts.get(threadId);
    if (path) appendFileSync(path, rolloutLine(type, payload));
  };

  const newThread = (id: string, cwd: string) => {
    const thread = codexThread(id, { cwd, model: ALASIO_CODEX_MODEL, createdAt: now(), updatedAt: now() });
    threads.set(id, thread);
    const path = rolloutPath(process.env["CODEX_HOME"] ?? "", id, new Date());
    mkdirSync(join(path, ".."), { recursive: true });
    rollouts.set(id, path);
    record(id, "session_meta", { id, timestamp: new Date().toISOString(), cwd, originator: client ?? "", cli_version: "stand-in" });
    return thread;
  };

  const answers: Answers = {
    initialize: ({ clientInfo }) => {
      client = clientInfo.name;
      return { ...initializeResponse(), codexHome: process.env["CODEX_HOME"] ?? "" };
    },
    "thread/start": ({ cwd }) => {
      const thread = newThread(randomUUID(), cwd ?? process.cwd());
      notify({ method: "thread/started", params: { thread } });
      return threadStartResponse(thread);
    },
    "thread/resume": ({ threadId, cwd }) => threadResumeResponse(threads.get(threadId) ?? newThread(threadId, cwd ?? process.cwd())),
    "thread/loaded/list": () => ({ data: [...threads.keys()], nextCursor: null }),
    "thread/list": ({ cwd }) => ({
      data: [...threads.values()].filter((thread) => cwd == null || thread.cwd === cwd).map((thread) => ({ ...thread, turns: [] })),
      nextCursor: null,
      backwardsCursor: null,
    }),
    "thread/turns/list": ({ threadId }) => ({ data: [...(threads.get(threadId)?.turns ?? [])].reverse(), nextCursor: null, backwardsCursor: null }),
    "model/list": () => ({ data: [codexModel(ALASIO_CODEX_MODEL, { isDefault: true })], nextCursor: null }),
    "thread/goal/get": () => ({ goal: null }),
    "turn/interrupt": () => ({}),
    "turn/start": ({ threadId, input }, span) => {
      const thread = threads.get(threadId);
      if (!thread) throw new Error(`thread not found: ${threadId}`);
      const startedAt = now();
      const turn = codexTurn(randomUUID(), { startedAt });
      span.setAttribute("turn.id", turn.id);
      const answer = agentMessage(randomUUID(), ANSWER, "final_answer");
      const done = codexTurn(turn.id, { items: [userMessage(randomUUID(), input), answer], itemsView: "full", status: "completed", startedAt, completedAt: now(), durationMs: 0 });
      const text = input.map((item) => (item.type === "text" ? item.text : "")).join("");
      record(threadId, "turn_context", { turn_id: turn.id, cwd: thread.cwd, model: ALASIO_CODEX_MODEL });
      record(threadId, "event_msg", { type: "task_started", turn_id: turn.id });
      record(threadId, "response_item", { type: "message", role: "user", content: [{ type: "input_text", text }] });
      record(threadId, "response_item", { type: "message", role: "assistant", content: [{ type: "output_text", text: ANSWER }] });
      record(threadId, "event_msg", { type: "task_complete", turn_id: turn.id, last_agent_message: ANSWER, duration_ms: 0 });
      threads.set(threadId, { ...thread, preview: thread.preview || text, updatedAt: now(), turns: [...thread.turns, done] });
      // After the answer to turn/start, as Codex reports a turn once it has accepted it.
      setImmediate(() => {
        notify(turnStarted(threadId, turn));
        notify(itemCompleted(threadId, turn.id, answer));
        notify(turnCompleted(threadId, { ...done, items: [answer], itemsView: "summary" }));
      });
      return { turn };
    },
  };

  const answer = (request: Request) => {
    const parent = request.trace ? propagation.extract(context.active(), request.trace) : context.active();
    tracer.startActiveSpan(request.method, {
      kind: SpanKind.SERVER,
      attributes: {
        "rpc.system": "jsonrpc",
        "rpc.method": request.method,
        "rpc.transport": "stdio",
        "rpc.request_id": String(request.id),
        ...(client === undefined ? {} : { "app_server.client_name": client }),
      },
    }, parent, (span) => {
      const respond = answers[request.method] as ((params: unknown, span: Span) => unknown) | undefined;
      try {
        if (!respond) throw new Error(`the stand-in for Codex does not answer ${request.method}`);
        write({ id: request.id, result: respond(request.params, span) });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        span.recordException(message);
        write({ id: request.id, error: { code: respond ? -32603 : -32601, message } });
      } finally {
        span.end();
      }
    });
  };

  const stop = async () => {
    await sdk?.shutdown();
    process.exit(0);
  };
  process.on("SIGTERM", stop);
  const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
  lines.on("line", (line) => {
    // alasio writes JSON-RPC, a message a line: requests carry an id, its notifications none.
    const message = JSON.parse(line) as Request | { readonly id?: RequestId; readonly method?: string };
    if ("id" in message && message.id != null && "method" in message) answer(message as Request);
  });
  lines.on("close", stop);
}

if (import.meta.main) {
  const [command, ...args] = process.argv.slice(2);
  if (command !== "app-server") {
    console.error(`the stand-in for Codex runs as its app-server alone, not ${command ?? "with no command"}`);
    process.exit(2);
  }
  await serve(args);
}
