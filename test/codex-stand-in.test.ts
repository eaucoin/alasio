/**
 * The end-to-end run's stand-in for Codex (test/e2e/codex-stand-in.ts), as alasio runs
 * it in Codex's place: alasio's own app-server client runs a turn on it, answered with
 * the stand-in's answer, and its spans reach the exporter alasio gives Codex, in the
 * traces alasio's requests carry.
 */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer, type IncomingHttpHeaders, type Server } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { after, before, test } from "node:test";
import { fileURLToPath } from "node:url";

import { Effect, Logger, Stream } from "effect";

import { makeAppServer } from "../src/codex/app-server/client.ts";
import type { AppServerEvent } from "../src/codex/app-server/protocol.ts";
import { codexTelemetryArgs } from "../src/codex/app-server/telemetry.ts";
import { buildCodexEnv } from "../src/codex/env.ts";
import { ANSWER } from "./e2e/codex-stand-in.ts";
import { eventually } from "./support/wait.ts";

const STAND_IN = fileURLToPath(new URL("./e2e/codex-stand-in.ts", import.meta.url));

/** A span as OTLP/JSON carries it, as far as these tests read it. */
interface ExportedSpan {
  readonly traceId: string;
  readonly spanId: string;
  readonly parentSpanId?: string;
  readonly name: string;
  readonly kind: number;
  readonly attributes: readonly { readonly key: string; readonly value: { readonly stringValue?: string } }[];
}

/** What the OTLP/JSON stand-in received: each span, with its service's name and the export's headers. */
interface Received {
  readonly service: string | undefined;
  readonly headers: IncomingHttpHeaders;
  readonly span: ExportedSpan;
}

const SERVER = 2;
const attribute = (span: ExportedSpan, key: string) => span.attributes.find((each) => each.key === key)?.value.stringValue;

let sink: Server;
const received: Received[] = [];
let home: string;
const savedEnv = { ...process.env };

before(async () => {
  sink = createServer((request, response) => {
    const chunks: Buffer[] = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => {
      // OTLP/JSON, as the exporter alasio names sends it.
      const body = JSON.parse(Buffer.concat(chunks).toString()) as {
        resourceSpans: { resource: { attributes: ExportedSpan["attributes"] }; scopeSpans: { spans: ExportedSpan[] }[] }[];
      };
      for (const { resource, scopeSpans } of body.resourceSpans) {
        const service = resource.attributes.find(({ key }) => key === "service.name")?.value.stringValue;
        for (const { spans } of scopeSpans) for (const span of spans) received.push({ service, headers: request.headers, span });
      }
      response.writeHead(200, { "content-type": "application/json" }).end("{}");
    });
  });
  await new Promise<void>((resolve) => sink.listen(0, "127.0.0.1", resolve));
  const address = sink.address();
  assert.ok(address !== null && typeof address === "object", "a TCP server's address");
  home = mkdtempSync(join(tmpdir(), "alasio-codex-stand-in-"));
  // As the run's install sets alasio's: the stand-in as Codex, and spans exported over OTLP/JSON.
  Object.assign(process.env, {
    ALASIO_CODEX_BIN: STAND_IN,
    CODEX_HOME: home,
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: `http://127.0.0.1:${address.port}/v1/traces`,
    OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: "http/json",
    OTEL_EXPORTER_OTLP_TRACES_HEADERS: "x-sent-by=alasio",
  });
});

after(async () => {
  process.env = savedEnv;
  await new Promise((resolve) => sink.close(resolve));
  rmSync(home, { recursive: true, force: true });
});

test("alasio's app-server client runs a turn on the stand-in, which answers it with its answer", async () => {
  const events = await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const client = yield* makeAppServer();
    const thread = { cwd: home, env: buildCodexEnv(), threadKey: "conversation-1", config: { developer_instructions: "", mcp_servers: {} } };
    const threadId = yield* client.startThread(thread);
    const turnId = yield* client.startTurn({ ...thread, threadId, prompt: "hello" });
    const seen = yield* Stream.runCollect(client.eventsForTurn(threadId, turnId).pipe(Stream.takeUntil((event) => event.type === "turn.completed")));
    const [turn] = yield* client.listTurns({ ...thread, threadId });
    assert.equal(turn?.id, turnId);
    return seen;
  })).pipe(Effect.provideService(Logger.CurrentLoggers, new Set())));
  const answered = events.flatMap((event: AppServerEvent) => (event.type === "item.completed" && event.item.type === "agent_message" ? [event.item.text] : []));
  assert.deepEqual(answered, [ANSWER]);
  assert.equal(events.at(-1)?.type, "turn.completed");
});

test("each request is a server span of the trace it is sent in, exported as alasio tells Codex to, from codex-app-server", async () => {
  const traceId = "0af7651916cd43dd8448eb211c80319c";
  const parentId = "b7ad6b7169203331";
  const child = spawn(STAND_IN, ["app-server", ...codexTelemetryArgs(), "--listen", "stdio://"], { cwd: home, stdio: ["pipe", "pipe", "inherit"] });
  /** The stand-in's answers, by their request's id. */
  type Answer = { readonly id?: number; readonly method?: string; readonly result?: { readonly thread?: { readonly id: string }; readonly turn?: { readonly id: string } } };
  const answers = new Map<number, Answer>();
  const methods: string[] = [];
  createInterface({ input: child.stdout }).on("line", (line) => {
    // The stand-in writes JSON-RPC, a message a line.
    const message = JSON.parse(line) as Answer;
    if (message.id !== undefined) answers.set(message.id, message);
    else if (message.method) methods.push(message.method);
  });
  const request = async (id: number, method: string, params: object) => {
    child.stdin.write(`${JSON.stringify({ id, method, params, trace: { traceparent: `00-${traceId}-${parentId}-01` } })}\n`);
    return eventually(`the answer to ${method}`, () => answers.get(id));
  };
  await request(0, "initialize", { clientInfo: { name: "alasio", title: null, version: "0" } });
  const threadId = (await request(1, "thread/start", { cwd: home })).result?.thread?.id;
  assert.ok(threadId);
  const turnId = (await request(2, "turn/start", { threadId, input: [{ type: "text", text: "hello", text_elements: [] }] })).result?.turn?.id;
  await eventually("the turn to complete", () => methods.includes("turn/completed") || undefined);
  child.stdin.end();
  await new Promise((resolve) => child.on("exit", resolve));

  const spans = received.filter(({ span }) => span.traceId === traceId);
  assert.deepEqual(spans.map(({ service, span }) => [service, span.name, span.kind, span.parentSpanId, attribute(span, "rpc.method"), attribute(span, "app_server.client_name")]), [
    ["codex-app-server", "initialize", SERVER, parentId, "initialize", undefined],
    ["codex-app-server", "thread/start", SERVER, parentId, "thread/start", "alasio"],
    ["codex-app-server", "turn/start", SERVER, parentId, "turn/start", "alasio"],
  ]);
  assert.equal(attribute(spans[2]!.span, "turn.id"), turnId);
  for (const { headers } of spans) assert.equal(headers["x-sent-by"], "alasio");
});
