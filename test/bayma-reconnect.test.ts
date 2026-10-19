/**
 * A harness's process outlives its bayma's binding: the gateway releases a workspace idle
 * for its time, and the next request binds it anew, to a pod whose bayma knows nothing of the
 * MCP sessions of the one before, which answers their requests 404, as bayma does a session
 * it does not know. Claude Code and Codex, each run here as alasio runs it, with alasio's
 * options and bayma's server as alasio gives it, keep their process across such a release
 * and reach the new bayma's tools on the next turn, their MCP client initializing a session
 * anew, with the endpoint's bearer still.
 *
 * A process outlives its bearer too, a caller's token good for TOKEN_TTL_MS: given bayma's
 * server anew, its bearer fresh, as alasio gives it once the bearer is about to lapse (Claude
 * Code's by the Agent SDK's setMcpServers, Codex's thread loaded anew with the config its turn
 * gives), it reaches bayma's tools with the new bearer, the old one refused.
 *
 * Each is run against a model API of this test's own, which calls bayma's tool on a prompt
 * that asks for it, and answers with what it returned.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer, type IncomingMessage, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { query, type SDKMessage, type SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import { Effect, Exit, Scope, Stream } from "effect";

import { makeAppServer } from "../src/codex/app-server/client.ts";
import type { AppServerEvent } from "../src/codex/app-server/protocol.ts";
import { buildCodexThreadConfig } from "../src/codex/thread-config.ts";
import { claudeMcpServers } from "../src/harness/claude/mcp.ts";
import { buildClaudeQueryOptions } from "../src/harness/claude/runtime.ts";

const BEARER = "Bearer the-binding's";

async function bodyOf(incoming: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of incoming) chunks.push(chunk as Buffer);
  const text = Buffer.concat(chunks).toString("utf8");
  return text ? JSON.parse(text) : undefined;
}

async function listening(server: Server): Promise<string> {
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
}

/**
 * A stand-in for a binding's bayma, through the gateway: its tool `ping` says which pod
 * answers, and `rebind` puts another pod in its place, which knows none of the sessions of
 * the one before. What it was asked, in order.
 */
async function bayma() {
  let pod = 1;
  let bearer = BEARER;
  const sessions = new Map<string, StreamableHTTPServerTransport>();
  const asked: string[] = [];
  const server = createServer(async (incoming, outgoing) => {
    if (incoming.headers.authorization !== bearer) {
      asked.push(`refused ${incoming.headers.authorization}`);
      return void outgoing.writeHead(401).end();
    }
    const id = incoming.headers["mcp-session-id"];
    const body = incoming.method === "POST" ? await bodyOf(incoming) : undefined;
    if (typeof id === "string") {
      const session = sessions.get(id);
      if (session) return session.handleRequest(incoming, outgoing, body);
      asked.push(`${incoming.method} of a session pod ${pod} does not know`);
      return void outgoing.writeHead(404, { "content-type": "application/json" }).end(JSON.stringify({ jsonrpc: "2.0", error: { code: -32004, message: "unknown MCP session" }, id: null }));
    }
    if (incoming.method !== "POST" || !isInitializeRequest(body)) return void outgoing.writeHead(400).end();
    asked.push(`initialize of pod ${pod} with ${bearer}`);
    const mcp = new McpServer({ name: "bayma", version: "1.0.0" });
    const answering = pod;
    mcp.tool("ping", "Says which pod answers", async () => ({ content: [{ type: "text", text: `pong from pod ${answering}` }] }));
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: () => randomUUID(), onsessioninitialized: (made) => void sessions.set(made, transport) });
    // The transport is one; the SDK declares its onclose `(() => void) | undefined` where Transport has it optional.
    await mcp.connect(transport as Transport);
    await transport.handleRequest(incoming, outgoing, body);
  });
  const url = `${await listening(server)}/mcp`;
  return {
    url,
    asked,
    rebind: async () => {
      for (const session of sessions.values()) await session.close();
      sessions.clear();
      pod += 1;
    },
    /** The bearer it takes from now on, the one before lapsed. */
    take: (fresh: string) => {
      bearer = fresh;
    },
    close: () => server.close(),
  };
}

/** The text of a message's content, a tool's result in it named so. */
type Block = { readonly type: string; readonly text?: string; readonly content?: unknown };
const textOf = (content: string | readonly Block[]): string =>
  typeof content === "string" ? content : content.map((block) => (block.type === "text" ? block.text : block.type === "tool_result" ? `RESULT ${JSON.stringify(block.content)}` : "")).join(" ");

/**
 * Anthropic's Messages API, as Claude Code asks it: a prompt saying PING calls bayma's ping,
 * where Claude Code offers it; its result is answered with; anything else, with "ok".
 */
function messagesApi(): Server {
  return createServer(async (incoming, outgoing) => {
    const asked = (await bodyOf(incoming)) as { messages?: { content: string | Block[] }[]; tools?: { name: string }[]; stream?: boolean } | undefined;
    if (!incoming.url?.startsWith("/v1/messages") || !asked?.messages) return void outgoing.writeHead(200, { "content-type": "application/json" }).end("{}");
    const said = textOf(asked.messages.at(-1)?.content ?? "");
    const offered = (asked.tools ?? []).some(({ name }) => name === "mcp__bayma__ping");
    const calling = !said.includes("RESULT") && said.includes("PING") && offered;
    const block = calling ? { type: "tool_use", id: `toolu_${randomUUID().replaceAll("-", "")}`, name: "mcp__bayma__ping", input: {} } : { type: "text", text: said.includes("RESULT") ? said : "ok" };
    const stop = calling ? "tool_use" : "end_turn";
    const message = { id: `msg_${randomUUID()}`, type: "message", role: "assistant", model: "claude-sonnet-4-5", stop_sequence: null, usage: { input_tokens: 1, output_tokens: 1 } };
    if (!asked.stream) return void outgoing.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ ...message, content: [block], stop_reason: stop }));
    outgoing.writeHead(200, { "content-type": "text/event-stream" });
    const send = (type: string, data: object) => outgoing.write(`event: ${type}\ndata: ${JSON.stringify({ type, ...data })}\n\n`);
    send("message_start", { message: { ...message, content: [], stop_reason: null } });
    send("content_block_start", { index: 0, content_block: calling ? block : { type: "text", text: "" } });
    send("content_block_delta", { index: 0, delta: calling ? { type: "input_json_delta", partial_json: "{}" } : { type: "text_delta", text: (block as { text: string }).text } });
    send("content_block_stop", { index: 0 });
    send("message_delta", { delta: { stop_reason: stop, stop_sequence: null }, usage: { output_tokens: 1 } });
    send("message_stop", {});
    outgoing.end();
  });
}

/**
 * OpenAI's Responses API, as Codex asks it: a prompt saying PING calls bayma's ping, in the
 * namespace Codex offers its tools in; one saying `BG <mark>` runs a command that goes on in
 * the background, its terminal left running, `<mark>` in its command line; their output is
 * answered with; anything else, with "ok".
 */
function responsesApi(): Server {
  return createServer(async (incoming, outgoing) => {
    const asked = (await bodyOf(incoming)) as { input?: { type: string }[]; tools?: { type?: string; name?: string }[] } | undefined;
    if (!incoming.url?.endsWith("/responses") || !asked?.input) return void outgoing.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ data: [], models: [] }));
    const last = asked.input.at(-1);
    const offered = (asked.tools ?? []).some(({ type, name }) => type === "namespace" && name === "mcp__bayma");
    outgoing.writeHead(200, { "content-type": "text/event-stream" });
    const send = (type: string, data: object) => outgoing.write(`event: ${type}\ndata: ${JSON.stringify({ type, ...data })}\n\n`);
    const id = `resp_${randomUUID()}`;
    const message = (text: string) => ({ type: "message", role: "assistant", id: `msg_${randomUUID()}`, content: [{ type: "output_text", text }] });
    send("response.created", { response: { id } });
    if (last?.type === "function_call_output") send("response.output_item.done", { item: message(`RESULT ${JSON.stringify(last)}`) });
    else if (/BG (\S+)/u.test(JSON.stringify(last))) {
      const mark = /BG ([\w-]+)/u.exec(JSON.stringify(last))?.[1] ?? "";
      send("response.output_item.done", { item: { type: "function_call", id: `fc_${randomUUID()}`, name: "exec_command", arguments: JSON.stringify({ cmd: `sleep 600; echo ${mark}`, yield_time_ms: 1000 }), call_id: `call_${randomUUID()}` } });
    } else if (JSON.stringify(last).includes("PING") && offered) send("response.output_item.done", { item: { type: "function_call", id: `fc_${randomUUID()}`, namespace: "mcp__bayma", name: "ping", arguments: "{}", call_id: `call_${randomUUID()}` } });
    else send("response.output_item.done", { item: message("ok") });
    send("response.completed", { response: { id, usage: { input_tokens: 1, input_tokens_details: null, output_tokens: 1, output_tokens_details: null, total_tokens: 2 } } });
    outgoing.end();
  });
}

/**
 * A long-lived Claude Code process, as alasio starts one for a folder, of alasio's options,
 * its bayma given with `bearer`, against the Messages API's stand-in: its turns, each
 * answered with what bayma's tool returned, and its query, as alasio holds it.
 */
async function claudeProcess(url: string, bearer: string) {
  const api = messagesApi();
  const home = mkdtempSync(join(tmpdir(), "alasio-reconnect-claude-"));
  const prompts: SDKUserMessage[] = [];
  const waiting: ((message: SDKUserMessage) => void)[] = [];
  async function* channel(): AsyncGenerator<SDKUserMessage> {
    for (;;) yield prompts.shift() ?? (await new Promise<SDKUserMessage>((resolve) => waiting.push(resolve)));
  }
  const controller = new AbortController();
  const claudeEnv = { PATH: process.env["PATH"] ?? "", HOME: home, CLAUDE_CONFIG_DIR: join(home, ".claude"), ANTHROPIC_BASE_URL: await listening(api), ANTHROPIC_API_KEY: "sk-test", CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: "1" };
  const options = buildClaudeQueryOptions({ workingDirectory: home, claudeEnv, mcpServers: claudeMcpServers({ type: "http", url, headers: { Authorization: bearer } }), controller, hooks: {}, env: {} });
  const running = query({ prompt: channel(), options: { ...options, model: "claude-sonnet-4-5" } });
  const results: string[] = [];
  const reading = (async () => {
    for await (const message of running as AsyncIterable<SDKMessage>) {
      if (message.type === "result") results.push(message.subtype === "success" ? message.result : message.subtype);
    }
  })();
  return {
    running,
    /** A turn of `prompt`: what it answered. */
    turn: async (prompt: string) => {
      const count = results.length + 1;
      const message: SDKUserMessage = { type: "user", message: { role: "user", content: prompt }, parent_tool_use_id: null, session_id: "" };
      const resolve = waiting.shift();
      if (resolve) resolve(message);
      else prompts.push(message);
      for (const deadline = Date.now() + 60_000; results.length < count; await new Promise((wake) => setTimeout(wake, 100))) assert.ok(Date.now() < deadline, `no answer: ${JSON.stringify(results)}`);
      return results[count - 1] ?? "";
    },
    close: async () => {
      controller.abort();
      await reading.catch(() => undefined);
      api.close();
      rmSync(home, { recursive: true, force: true });
    },
  };
}

/**
 * Codex's app-server, as alasio runs it, against the Responses API's stand-in, a thread of it
 * started with bayma's server given with `bearer`, keeping its config `configLifeMs`: its
 * turns, each giving the thread bayma's server with the bearer it is given, as alasio's turns
 * give it, and what bayma's tool call of it came to.
 */
async function codexThread(url: string, bearer: string, configLifeMs?: number) {
  const api = responsesApi();
  const home = mkdtempSync(join(tmpdir(), "alasio-reconnect-codex-"));
  const codexHome = join(home, ".codex");
  mkdirSync(codexHome);
  writeFileSync(join(codexHome, "config.toml"), [
    'model_provider = "stand-in"',
    "[model_providers.stand-in]",
    'name = "stand-in"',
    `base_url = "${await listening(api)}/v1"`,
    'wire_api = "responses"',
    "requires_openai_auth = false",
    "",
  ].join("\n"));
  const codexEnv = { PATH: process.env["PATH"] ?? "", HOME: home, CODEX_HOME: codexHome };
  const scope = Effect.runSync(Scope.make());
  const options = (given: string) => ({ threadKey: "telegram:1", cwd: home, env: codexEnv, config: buildCodexThreadConfig({ codexEnv, bayma: { url, headers: { Authorization: given } } }) });
  const appServer = await Effect.runPromise(makeAppServer({ threadConfigLifeMs: configLifeMs }).pipe(Scope.provide(scope)));
  const threadId = await Effect.runPromise(appServer.startThread(options(bearer)));
  return {
    /** A turn of `prompt`, its thread ensured as alasio's turn ensures it, with `given`: bayma's tool call of it. */
    turn: (prompt: string, given = bearer) =>
      Effect.runPromise(Effect.gen(function*() {
        yield* appServer.ensureThread({ ...options(given), threadId });
        // On a model Codex offers MCP tools to as functions, which this test's API calls; the
        // MCP client that reaches bayma is the same whichever model asks it.
        const turnId = yield* appServer.startTurn({ ...options(given), threadId, prompt, model: "gpt-5" });
        const events = yield* Stream.runCollect(appServer.eventsForTurn(threadId, turnId));
        return [...events].flatMap((event: AppServerEvent) => (event.type === "item.completed" && event.item.type === "mcp_tool_call" ? [JSON.stringify(event.item)] : [])).join("\n");
      })),
    /** Whether the conversation has Codex working in it on its own, as alasio asks between turns. */
    backgroundWork: () => Effect.runPromise(appServer.backgroundWork("telegram:1")),
    close: async () => {
      await Effect.runPromise(Scope.close(scope, Exit.void));
      api.close();
      rmSync(home, { recursive: true, force: true });
    },
  };
}

test("Claude Code's process reaches its bayma's tools on the turn after its binding was released and bound anew, its MCP client initializing anew", { timeout: 120_000 }, async () => {
  const stand = await bayma();
  const claude = await claudeProcess(stand.url, BEARER);
  try {
    assert.match(await claude.turn("PING"), /pong from pod 1/u);
    await stand.rebind();
    assert.match(await claude.turn("PING"), /pong from pod 2/u);
    assert.deepEqual(stand.asked.slice(0, 3), [`initialize of pod 1 with ${BEARER}`, "POST of a session pod 2 does not know", `initialize of pod 2 with ${BEARER}`]);
  } finally {
    await claude.close();
    stand.close();
  }
});

test("Claude Code's process given its bayma anew, its bearer fresh, reaches its tools with it, the bearer before refused", { timeout: 120_000 }, async () => {
  const stand = await bayma();
  const claude = await claudeProcess(stand.url, BEARER);
  try {
    assert.match(await claude.turn("PING"), /pong from pod 1/u);
    // Its bearer lapsed: bayma takes a fresh one alone, which alasio gives the process as it would.
    stand.take("Bearer fresh");
    const given = await claude.running.setMcpServers(claudeMcpServers({ type: "http", url: stand.url, headers: { Authorization: "Bearer fresh" } }));
    assert.deepEqual(given.errors, {});
    assert.match(await claude.turn("PING"), /pong from pod 1/u);
    assert.ok(stand.asked.includes("initialize of pod 1 with Bearer fresh"), JSON.stringify(stand.asked));
  } finally {
    await claude.close();
    stand.close();
  }
});

test("Codex's app-server reaches its bayma's tools on the turn after its binding was released and bound anew, its thread loaded still, its MCP client initializing anew", { timeout: 120_000 }, async () => {
  const stand = await bayma();
  const codex = await codexThread(stand.url, BEARER);
  try {
    assert.match(await codex.turn("PING"), /pong from pod 1/u);
    await stand.rebind();
    // Its thread loaded still, as alasio's next turn of it finds it.
    assert.match(await codex.turn("PING"), /pong from pod 2/u);
    assert.deepEqual(stand.asked.slice(0, 3), [`initialize of pod 1 with ${BEARER}`, "POST of a session pod 2 does not know", `initialize of pod 2 with ${BEARER}`]);
  } finally {
    await codex.close();
    stand.close();
  }
});

test("Codex's thread past its config's time is loaded anew by its next turn, with the bearer that turn gives, which reaches bayma's tools, the bearer before refused", { timeout: 120_000 }, async () => {
  const stand = await bayma();
  const codex = await codexThread(stand.url, BEARER, 1_000);
  try {
    assert.match(await codex.turn("PING"), /pong from pod 1/u);
    stand.take("Bearer fresh");
    await new Promise((resolve) => setTimeout(resolve, 1_200));
    assert.match(await codex.turn("PING", "Bearer fresh"), /pong from pod 1/u);
    assert.ok(stand.asked.includes("initialize of pod 1 with Bearer fresh"), JSON.stringify(stand.asked));
  } finally {
    await codex.close();
    stand.close();
  }
});

/** The processes whose command line holds `mark`. */
const marked = (mark: string): number[] =>
  execFileSync("ps", ["-eo", "pid=,args="], { encoding: "utf8" }).split("\n").flatMap((line) => {
    const [pid = "", ...args] = line.trim().split(/\s+/u);
    return args.join(" ").includes(mark) && !args.join(" ").startsWith("ps ") ? [Number(pid)] : [];
  });

test("Codex's thread past its config's time that left commands running in the background is kept as it is, as letting it go ends them, and loaded anew with the bearer its turn gives once they end", { timeout: 120_000 }, async () => {
  const stand = await bayma();
  const codex = await codexThread(stand.url, BEARER, 1_000);
  const mark = `alasio-background-${randomUUID().slice(0, 8)}`;
  try {
    await codex.turn(`BG ${mark}`);
    assert.equal(await codex.backgroundWork(), true, "its terminal left running");
    stand.take("Bearer fresh");
    await new Promise((resolve) => setTimeout(resolve, 1_200));
    // Its config past its time, but its command running: kept, its bearer before kept with it.
    // Its tool call answered, failed, as bayma refused the bearer that lapsed: no wait without end.
    const refused = await codex.turn("PING", "Bearer fresh");
    assert.doesNotMatch(refused, /pong/u);
    assert.match(refused, /"status":"failed".*HTTP 401/u);
    assert.ok(stand.asked.includes(`refused ${BEARER}`), JSON.stringify(stand.asked));
    assert.deepEqual([await codex.backgroundWork(), marked(mark).length], [true, 1], "its command running on");
    // Its command ended: its next turn loads it anew, with the bearer that turn gives.
    for (const pid of marked(mark)) process.kill(pid, "SIGKILL");
    for (const deadline = Date.now() + 30_000; await codex.backgroundWork(); await new Promise((resolve) => setTimeout(resolve, 200))) assert.ok(Date.now() < deadline, "its terminal still listed");
    assert.match(await codex.turn("PING", "Bearer fresh"), /pong from pod 1/u);
  } finally {
    for (const pid of marked(mark)) process.kill(pid, "SIGKILL");
    await codex.close();
    stand.close();
  }
});
