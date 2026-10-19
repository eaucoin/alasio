/**
 * A harness's process outlives its bayma's binding: the gateway releases a workspace idle
 * for its time, and the next request binds it anew, to a pod whose bayma knows nothing of the
 * MCP sessions of the one before, which answers their requests 404, as bayma does a session
 * it does not know. Claude Code and Codex, each run here as alasio runs it, with alasio's
 * options and bayma's server as alasio gives it, keep their process across such a release
 * and reach the new bayma's tools on the next turn, their MCP client initializing a session
 * anew, with the endpoint's bearer still. Each is run against a model API of this test's own,
 * which calls bayma's tool on a prompt that asks for it, and answers with what it returned.
 */
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
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
  const sessions = new Map<string, StreamableHTTPServerTransport>();
  const asked: string[] = [];
  const server = createServer(async (incoming, outgoing) => {
    if (incoming.headers.authorization !== BEARER) return void outgoing.writeHead(401).end();
    const id = incoming.headers["mcp-session-id"];
    const body = incoming.method === "POST" ? await bodyOf(incoming) : undefined;
    if (typeof id === "string") {
      const session = sessions.get(id);
      if (session) return session.handleRequest(incoming, outgoing, body);
      asked.push(`${incoming.method} of a session pod ${pod} does not know`);
      return void outgoing.writeHead(404, { "content-type": "application/json" }).end(JSON.stringify({ jsonrpc: "2.0", error: { code: -32004, message: "unknown MCP session" }, id: null }));
    }
    if (incoming.method !== "POST" || !isInitializeRequest(body)) return void outgoing.writeHead(400).end();
    asked.push(`initialize of pod ${pod}`);
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
 * namespace Codex offers its tools in; its output is answered with; anything else, with "ok".
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
    else if (JSON.stringify(last).includes("PING") && offered) send("response.output_item.done", { item: { type: "function_call", id: `fc_${randomUUID()}`, namespace: "mcp__bayma", name: "ping", arguments: "{}", call_id: `call_${randomUUID()}` } });
    else send("response.output_item.done", { item: message("ok") });
    send("response.completed", { response: { id, usage: { input_tokens: 1, input_tokens_details: null, output_tokens: 1, output_tokens_details: null, total_tokens: 2 } } });
    outgoing.end();
  });
}

test("Claude Code's process reaches its bayma's tools on the turn after its binding was released and bound anew, its MCP client initializing anew", { timeout: 120_000 }, async () => {
  const stand = await bayma();
  const api = messagesApi();
  const home = mkdtempSync(join(tmpdir(), "alasio-reconnect-claude-"));
  const prompts: SDKUserMessage[] = [];
  const waiting: ((message: SDKUserMessage) => void)[] = [];
  async function* channel(): AsyncGenerator<SDKUserMessage> {
    for (;;) yield prompts.shift() ?? (await new Promise<SDKUserMessage>((resolve) => waiting.push(resolve)));
  }
  const say = (text: string) => {
    const message: SDKUserMessage = { type: "user", message: { role: "user", content: text }, parent_tool_use_id: null, session_id: "" };
    const resolve = waiting.shift();
    if (resolve) resolve(message);
    else prompts.push(message);
  };
  const controller = new AbortController();
  try {
    const claudeEnv = { PATH: process.env["PATH"] ?? "", HOME: home, CLAUDE_CONFIG_DIR: join(home, ".claude"), ANTHROPIC_BASE_URL: await listening(api), ANTHROPIC_API_KEY: "sk-test", CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: "1" };
    const options = buildClaudeQueryOptions({ workingDirectory: home, claudeEnv, mcpServers: claudeMcpServers({ type: "http", url: stand.url, headers: { Authorization: BEARER } }), controller, hooks: {}, env: {} });
    const results: string[] = [];
    const reading = (async () => {
      for await (const message of query({ prompt: channel(), options: { ...options, model: "claude-sonnet-4-5" } }) as AsyncIterable<SDKMessage>) {
        if (message.type === "result") results.push(message.subtype === "success" ? message.result : message.subtype);
      }
    })();
    const answered = async (count: number) => {
      for (const deadline = Date.now() + 60_000; results.length < count; await new Promise((resolve) => setTimeout(resolve, 100))) assert.ok(Date.now() < deadline, `no answer ${count}: ${JSON.stringify(results)}`);
      return results[count - 1] ?? "";
    };
    say("PING");
    assert.match(await answered(1), /pong from pod 1/u);
    await stand.rebind();
    say("PING");
    assert.match(await answered(2), /pong from pod 2/u);
    assert.deepEqual(stand.asked.slice(0, 3), ["initialize of pod 1", "POST of a session pod 2 does not know", "initialize of pod 2"]);
    controller.abort();
    await reading.catch(() => undefined);
  } finally {
    controller.abort();
    api.close();
    stand.close();
  }
});

test("Codex's app-server reaches its bayma's tools on the turn after its binding was released and bound anew, its thread loaded still, its MCP client initializing anew", { timeout: 120_000 }, async () => {
  const stand = await bayma();
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
  try {
    const said = await Effect.runPromise(Effect.gen(function*() {
      const appServer = yield* makeAppServer();
      const options = { threadKey: "telegram:1", cwd: home, env: codexEnv, config: buildCodexThreadConfig({ codexEnv, bayma: { url: stand.url, headers: { Authorization: BEARER } } }) };
      const threadId = yield* appServer.startThread(options);
      /** What bayma's tool call of a turn of `prompt` came to: its result, or its error. */
      const turn = (prompt: string) =>
        Effect.gen(function*() {
          // On a model Codex offers MCP tools to as functions, which this test's API calls; the
          // MCP client that reaches bayma is the same whichever model asks it.
          const turnId = yield* appServer.startTurn({ ...options, threadId, prompt, model: "gpt-5" });
          const events = yield* Stream.runCollect(appServer.eventsForTurn(threadId, turnId));
          const calls = [...events].flatMap((event: AppServerEvent) => (event.type === "item.completed" && event.item.type === "mcp_tool_call" ? [JSON.stringify(event.item)] : []));
          return calls.join("\n");
        });
      const first = yield* turn("PING");
      yield* Effect.promise(stand.rebind);
      // Its thread loaded still, as alasio's next turn of it finds it.
      const second = yield* turn("PING");
      return [first, second];
    }).pipe(Scope.provide(scope)));
    assert.match(said[0] ?? "", /pong from pod 1/u);
    assert.match(said[1] ?? "", /pong from pod 2/u);
    assert.deepEqual(stand.asked.slice(0, 3), ["initialize of pod 1", "POST of a session pod 2 does not know", "initialize of pod 2"]);
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    api.close();
    stand.close();
  }
});
