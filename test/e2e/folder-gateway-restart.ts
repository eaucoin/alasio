/**
 * Run inside alasio's pod, by node there (inAlasio in harness.ts), as `folder-gateway-restart.ts
 * <claude|codex> <mark>`: whether a folder conversation's harness, given its folder's bayma once,
 * at alasio's loopback (src/mcp/bayma-proxy.ts) as alasio gives it, keeps reaching it as the
 * sandbox gateway starts again, in the same process, its MCP server configured once. Its harness
 * is the one alasio pins: Claude Code's CLI, the Agent SDK's own, with the options a folder
 * conversation's is given (buildClaudeQueryOptions), against a stand-in for Anthropic's Messages
 * API; or Codex's app-server, with a folder thread's config (buildCodexThreadConfig), against a
 * stand-in for the Responses API. A prompt `CALL <tool> <arguments>` calls bayma's tool `tool`
 * with `arguments`, and its result is answered with, as `RESULT ok|error <its text>`.
 *
 * The test deletes the gateway's pods twice, and stops it twice, each time as the script asks,
 * by a file of its own, `/tmp/<mark>-<step>`, and tells it the step is done by
 * `/tmp/<mark>-<step>-done`:
 * 1. an exec keeps a value in a REPL session of the folder's instance;
 * 2. `idle`: the gateway is started again between the harness's calls;
 * 3. an exec reads the value, the harness's MCP client as the restart left it;
 * 4. `restarting`: an exec reads it as the gateway starts again, made once each of its pods is
 *    stopping (`/tmp/<mark>-restarting-deleted`), before one is ready again;
 * 5. `calling`: an exec of 20 s is under way as the gateway's pods are deleted, which let it
 *    end as they stop;
 * 6. as 3, after it;
 * 7. `down`: the gateway stopped, an exec made while it is away for longer than the loopback
 *    passes a request on again for (GATEWAY_RESTART_MS), which fails;
 * 8. `up`: the gateway started again, as 3.
 * It prints what each step came to. Its MCP client is never told to reconnect, nor its
 * session's control taken over: what it reaches after each is the loopback's alone.
 */
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer, type IncomingMessage } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { query, type SDKMessage, type SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";
import { Effect, Exit, Scope, Stream } from "effect";

import { makeAppServer } from "../../src/codex/app-server/client.ts";
import { buildCodexEnv } from "../../src/codex/env.ts";
import { buildCodexThreadConfig } from "../../src/codex/thread-config.ts";
import { loadAlasioConfig } from "../../src/config.ts";
import { buildClaudeQueryOptions } from "../../src/harness/claude/runtime.ts";
import { folderKey } from "../../src/gateway/names.ts";
import { makeBaymaProxy } from "../../src/mcp/bayma-proxy.ts";
import { makeGatewayWorkspaces } from "../../src/sandbox/gateway-client.ts";

/** What a tool call came to: whether the tool answered, or failed, what it said, and in how long. */
export interface CallSeen {
  readonly ok: boolean;
  readonly text: string;
  readonly ms: number;
}

/** What it saw, as the script's last line prints it. */
export interface FolderRestartSeen {
  readonly harness: string;
  readonly kept: CallSeen;
  readonly afterIdle: CallSeen;
  /** What an exec made as the gateway started again, before it was ready, came to. */
  readonly restarting: CallSeen;
  /** What the exec under way as the gateway's pods were deleted came to. */
  readonly underway: CallSeen;
  readonly afterUnderway: CallSeen;
  /** What an exec made while the gateway was away past GATEWAY_RESTART_MS came to. */
  readonly during: CallSeen;
  readonly afterOutage: CallSeen;
  /** How often the harness asked its model's stand-in. */
  readonly apiRequests: number;
}

const [harness, mark] = process.argv.slice(2);
if ((harness !== "claude" && harness !== "codex") || !mark) throw new Error("usage: folder-gateway-restart.ts <claude|codex> <mark>");
const threadKey = "e2e:restart";
// Codex's own app-server, the one alasio pins, not the stand-in the end-to-end installation gives alasio's pod.
delete process.env["ALASIO_CODEX_BIN"];
const sleep = (ms: number) => new Promise((wake) => setTimeout(wake, ms));

/** Asks the test for `step`, and waits until it says it is `said`: done, unless asked for another. */
async function asked(step: string, said = "done"): Promise<void> {
  writeFileSync(`/tmp/${mark}-${step}`, "");
  for (const deadline = Date.now() + 900_000; !existsSync(`/tmp/${mark}-${step}-${said}`); await sleep(100)) {
    if (Date.now() > deadline) throw new Error(`the test did not say ${step} ${said}`);
  }
}

async function bodyOf(incoming: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of incoming) chunks.push(chunk as Buffer);
  return Buffer.concat(chunks).toString("utf8");
}

/** The text of a tool's result, as a harness gives it back. */
const resultText = (content: unknown): string =>
  typeof content === "string" ? content : Array.isArray(content) ? content.map((part: { text?: string }) => part.text ?? "").join("") : JSON.stringify(content);

/** The tool of bayma's `tool` among those offered, by its name's end, as each harness names an MCP server's. */
const offered = (names: readonly string[], tool: string): string | undefined => names.find((name) => name.includes("bayma") && name.endsWith(tool.replace(/[^a-zA-Z0-9_-]/gu, "_")));

/** A call of a prompt `CALL <tool> <arguments>`, as the model's stand-in reads it. */
const callOf = (said: string) => /CALL (\S+) (\{.*\})/su.exec(said);

let apiRequests = 0;
const { sessionsGateway } = loadAlasioConfig();
const workspaces = makeGatewayWorkspaces(sessionsGateway);
const scope = Effect.runSync(Scope.make());
const home = mkdtempSync(join(tmpdir(), "alasio-folder-restart-"));
const servers: { close: () => void }[] = [];

/** The calls of the harness: one of bayma's tools, and its result's text. */
interface Harness {
  readonly call: (tool: string, args: object) => Promise<CallSeen>;
  readonly close: () => Promise<void>;
}

/** Claude Code's CLI, its folder's bayma `bayma`, against a stand-in for the Messages API. */
async function claude(bayma: { readonly url: string; readonly headers: Readonly<Record<string, string>> }): Promise<Harness> {
  type Block = { readonly type: string; readonly text?: string; readonly content?: unknown; readonly is_error?: boolean };
  const api = createServer(async (incoming, outgoing) => {
    const body = await bodyOf(incoming);
    const asked = (body ? JSON.parse(body) : undefined) as { messages?: { content: string | Block[] }[]; tools?: { name: string }[]; stream?: boolean } | undefined;
    if (!incoming.url?.startsWith("/v1/messages") || !asked?.messages) return void outgoing.writeHead(200, { "content-type": "application/json" }).end("{}");
    apiRequests += 1;
    const last = asked.messages.at(-1)?.content ?? "";
    const blocks: readonly Block[] = typeof last === "string" ? [{ type: "text", text: last }] : last;
    const result = blocks.find((block) => block.type === "tool_result");
    const call = result ? null : callOf(blocks.map((block) => block.text ?? "").join(" "));
    const name = call?.[1] ? offered((asked.tools ?? []).map((tool) => tool.name), call[1]) : undefined;
    const block = name
      ? { type: "tool_use", id: `toolu_${randomUUID().replaceAll("-", "")}`, name, input: JSON.parse(call?.[2] ?? "{}") }
      : { type: "text", text: result ? `RESULT ${result.is_error ? "error" : "ok"} ${resultText(result.content)}` : call ? `NO TOOL ${call[1]}` : "ok" };
    const stop = name ? "tool_use" : "end_turn";
    const message = { id: `msg_${randomUUID()}`, type: "message", role: "assistant", model: "claude-sonnet-4-5", stop_sequence: null, usage: { input_tokens: 1, output_tokens: 1 } };
    if (!asked.stream) return void outgoing.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ ...message, content: [block], stop_reason: stop }));
    outgoing.writeHead(200, { "content-type": "text/event-stream" });
    const send = (type: string, data: object) => outgoing.write(`event: ${type}\ndata: ${JSON.stringify({ type, ...data })}\n\n`);
    send("message_start", { message: { ...message, content: [], stop_reason: null } });
    send("content_block_start", { index: 0, content_block: name ? { ...block, input: {} } : { type: "text", text: "" } });
    send("content_block_delta", { index: 0, delta: name ? { type: "input_json_delta", partial_json: JSON.stringify((block as { input: object }).input) } : { type: "text_delta", text: (block as { text: string }).text } });
    send("content_block_stop", { index: 0 });
    send("message_delta", { delta: { stop_reason: stop, stop_sequence: null }, usage: { output_tokens: 1 } });
    send("message_stop", {});
    outgoing.end();
  });
  await new Promise<void>((resolve) => api.listen(0, "127.0.0.1", resolve));
  servers.push(api);
  const prompts: SDKUserMessage[] = [];
  const waiting: ((message: SDKUserMessage) => void)[] = [];
  async function* channel(): AsyncGenerator<SDKUserMessage> {
    for (;;) yield prompts.shift() ?? (await new Promise<SDKUserMessage>((resolve) => waiting.push(resolve)));
  }
  const controller = new AbortController();
  const claudeEnv = {
    PATH: process.env["PATH"] ?? "",
    HOME: home,
    CLAUDE_CONFIG_DIR: join(home, ".claude"),
    ANTHROPIC_BASE_URL: `http://127.0.0.1:${(api.address() as AddressInfo).port}`,
    ANTHROPIC_API_KEY: "sk-stand-in",
    CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: "1",
    ENABLE_CLAUDEAI_MCP_SERVERS: "false",
  };
  // The options of a folder conversation's Claude Code, of no override of alasio's environment: the SDK's own CLI.
  const options = buildClaudeQueryOptions({ workingDirectory: home, claudeEnv, mcpServers: { bayma: { type: "http", ...bayma } }, controller, hooks: {}, env: {} });
  const running = query({ prompt: channel(), options: { ...options, model: "claude-sonnet-4-5" } });
  const results: string[] = [];
  const reading = (async () => {
    for await (const message of running as AsyncIterable<SDKMessage>) {
      if (message.type === "result") results.push(message.subtype === "success" ? message.result : message.subtype);
    }
  })();
  return {
    call: async (tool, args) => {
      const started = Date.now();
      const count = results.length + 1;
      const message: SDKUserMessage = { type: "user", message: { role: "user", content: `CALL ${tool} ${JSON.stringify(args)}` }, parent_tool_use_id: null, session_id: "" };
      const resolve = waiting.shift();
      if (resolve) resolve(message);
      else prompts.push(message);
      for (const deadline = Date.now() + 600_000; results.length < count; await sleep(100)) {
        if (Date.now() > deadline) throw new Error(`no answer to CALL ${tool}: ${JSON.stringify(results)}`);
      }
      return seenOf(tool, results[count - 1] ?? "", started);
    },
    close: async () => {
      controller.abort();
      await reading.catch(() => undefined);
    },
  };
}

/** What the JavaScript of Codex's code mode gives back of a nested tool of bayma's that failed, ahead of what it said. */
const NESTED_ERROR = "NESTED TOOL ERROR";

/**
 * The JavaScript of Codex's code mode that calls bayma's `tool` with `args`, among the nested
 * tools (the global `tools`, each by a JavaScript identifier of its name), and gives back the
 * text of its result, NESTED_ERROR ahead of it where it failed: waited for until it ends, as a
 * call is passed on past GATEWAY_RESTART_MS, where code mode yields after 30 s by default.
 */
const nestedCall = (tool: string, args: string): string => {
  const end = tool.replace(/[^a-zA-Z0-9_]/gu, "_");
  return [
    '// @exec: {"yield_time_ms": 300000}',
    `const name = Object.keys(tools).find((name) => name.includes("bayma") && name.endsWith(${JSON.stringify(end)})) ?? ${JSON.stringify(`mcp__bayma__${end}`)};`,
    "const flat = (result) => typeof result === \"string\" ? result : Array.isArray(result?.content) ? result.content.map((part) => part.text ?? \"\").join(\"\") : JSON.stringify(result);",
    "try {",
    `  const result = await tools[name](${args});`,
    `  text(\`\${result?.isError ? ${JSON.stringify(`${NESTED_ERROR} `)} : ""}\${flat(result)}\`);`,
    "} catch (error) {",
    `  text(\`${NESTED_ERROR} \${error?.message ?? String(error)}\`);`,
    "}",
  ].join("\n");
};

/**
 * What each Codex app-server process of this pod is doing, as /proc says it: its threads'
 * names, states and waits, and the TCP connections of its pod's, every process's, with their
 * states (02, SYN_SENT: a connect not answered yet).
 */
function appServerState(): string {
  const read = (path: string) => {
    try {
      return readFileSync(path, "utf8");
    } catch {
      return "";
    }
  };
  const processes = readdirSync("/proc").filter((pid) => /^\d+$/u.test(pid) && read(`/proc/${pid}/cmdline`).split("\0").includes("app-server"));
  return processes.map((pid) => {
    const threads = readdirSync(`/proc/${pid}/task`).map((tid) =>
      `${read(`/proc/${pid}/task/${tid}/comm`).trim()} ${/^State:\s+(.*)$/mu.exec(read(`/proc/${pid}/task/${tid}/status`))?.[1] ?? "?"} ${read(`/proc/${pid}/task/${tid}/wchan`) || "?"}`
    );
    const connections = ["tcp", "tcp6"].flatMap((table) => read(`/proc/${pid}/net/${table}`).split("\n").slice(1).filter(Boolean)).map((line) => line.trim().split(/\s+/u).slice(1, 4).join(" "));
    return `process ${pid}: threads [${threads.join("; ")}]; connections (local remote state) [${connections.join("; ")}]`;
  }).join("\n") || "no app-server process";
}

/** Codex's app-server, a folder thread's bayma `bayma`, against a stand-in for the Responses API. */
async function codex(bayma: { readonly url: string; readonly headers: Readonly<Record<string, string>> }): Promise<Harness> {
  type Tool = { readonly type?: string; readonly name?: string; readonly tools?: readonly Tool[] };
  type Item = { readonly type: string; readonly role?: string; readonly content?: unknown; readonly output?: unknown; readonly tools?: readonly Tool[] };
  const answers: string[] = [];
  const api = createServer(async (incoming, outgoing) => {
    const body = await bodyOf(incoming);
    if (incoming.method !== "POST" || !incoming.url?.endsWith("/responses")) return void outgoing.writeHead(404).end();
    apiRequests += 1;
    const asked = JSON.parse(body) as { input?: Item[]; tools?: Tool[] };
    const input = asked.input ?? [];
    const last = input.at(-1);
    const output = last?.type === "function_call_output" || last?.type === "custom_tool_call_output" ? resultText(last.output) : null;
    const said = last?.type === "message" ? resultText(last.content) : "";
    const call = output === null ? callOf(said) : null;
    // Its tools, the request's own and those an input item adds (additional_tools), each in a
    // namespace, an MCP server's of its own (codex-rs tools/router.rs), as `<namespace> <name>`.
    const added = input.flatMap((item) => (item.type === "additional_tools" ? (item.tools ?? []) : []));
    const tools = [...(asked.tools ?? []), ...added].flatMap((tool) => (tool.type === "namespace" ? (tool.tools ?? []).map((nested) => `${tool.name ?? ""} ${nested.name ?? ""}`) : [` ${tool.name ?? ""}`]));
    // Code mode, as Codex runs its models: its one tool `exec`, which runs JavaScript that calls
    // the others, MCP servers' tools among them, nested (codex-rs code_mode).
    const coded = tools.some((tool) => tool.split(" ")[1] === "exec");
    const name = call?.[1] ? (coded ? "exec" : offered(tools, call[1])) : undefined;
    const id = `resp_${apiRequests}`;
    // A tool that failed, or code mode's script that called it, as Codex says one failed.
    const failed = output !== null && (/"isError"\s*:\s*true/u.test(output) || output.includes(NESTED_ERROR) || /^Script failed/u.test(output));
    const text = output !== null ? `RESULT ${failed ? "error" : "ok"} ${output}` : call ? `NO TOOL ${call[1]} among ${tools.join(", ")}` : "ok";
    const [namespace = "", tool = ""] = name?.split(" ") ?? [];
    const item = coded && name
      ? { type: "custom_tool_call", id: `ctc_${apiRequests}`, call_id: `call_${apiRequests}`, name, input: nestedCall(call?.[1] ?? "", call?.[2] ?? "{}") }
      : name
      ? { type: "function_call", id: `fc_${apiRequests}`, call_id: `call_${apiRequests}`, ...(namespace ? { namespace } : {}), name: tool, arguments: call?.[2] ?? "{}" }
      : { type: "message", role: "assistant", id: `msg_${apiRequests}`, content: [{ type: "output_text", text }] };
    // One of no tools, as Codex's own asides are (a thread's title), answered and left out of the turn's answers.
    if (!name && tools.length > 0) answers.push(text);
    outgoing.writeHead(200, { "content-type": "text/event-stream" });
    const events = [
      { type: "response.created", response: { id } },
      { type: "response.output_item.done", item },
      { type: "response.completed", response: { id, usage: { input_tokens: 0, input_tokens_details: null, output_tokens: 0, output_tokens_details: null, total_tokens: 0 } } },
    ];
    outgoing.end(events.map((event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`).join(""));
  });
  await new Promise<void>((resolve) => api.listen(0, "127.0.0.1", resolve));
  servers.push(api);
  // A Codex home of its own, whose only provider is the stand-in.
  const codexHome = join(home, "codex");
  mkdirSync(codexHome, { recursive: true });
  writeFileSync(join(codexHome, "config.toml"), `
model_provider = "stand_in"

[model_providers.stand_in]
name = "stand-in"
base_url = "http://127.0.0.1:${(api.address() as AddressInfo).port}/v1"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
`);
  // Its own log at info, so an app-server that never answers says what it was doing.
  const env = { ...buildCodexEnv(), CODEX_HOME: codexHome, RUST_LOG: "info" };
  // A folder thread's config: its bayma, at the loopback, as alasio gives it.
  const config = buildCodexThreadConfig({ codexEnv: env, bayma });
  const appServer = await Effect.runPromise(makeAppServer().pipe(Scope.provide(scope)));
  // Every 30 s the app-server has not started the thread, as it once did not answer `initialize`
  // for 5 min, what its process is doing: each thread's wait and its connections.
  const starting = Date.now();
  const stuck = setInterval(() => console.error(`the app-server has started no thread in ${Date.now() - starting} ms: ${appServerState()}`), 30_000);
  const threadId = await Effect.runPromise(appServer.startThread({ threadKey, cwd: home, env, config })).finally(() => clearInterval(stuck));
  return {
    call: async (tool, args) => {
      const started = Date.now();
      const count = answers.length + 1;
      const turnId = await Effect.runPromise(appServer.startTurn({ threadId, threadKey, prompt: `CALL ${tool} ${JSON.stringify(args)}`, cwd: home, env, config }));
      await Effect.runPromise(Stream.runDrain(appServer.eventsForTurn(threadId, turnId)).pipe(Effect.ignore));
      if (answers.length < count) throw new Error(`no answer to CALL ${tool}: ${JSON.stringify(answers)}`);
      return seenOf(tool, answers[count - 1] ?? "", started);
    },
    close: async () => {},
  };
}

/** What a call of `tool` answered `answer` came to. */
function seenOf(tool: string, answer: string, started: number): CallSeen {
  const seen = /^RESULT (ok|error) (.*)$/su.exec(answer);
  if (!seen) throw new Error(`CALL ${tool} called no tool: ${answer}`);
  return { ok: seen[1] === "ok" && !/"error_text"\s*:\s*"[^"]/u.test(seen[2] ?? ""), text: seen[2] ?? "", ms: Date.now() - started };
}

try {
  // The folder's instance bound, as a folder conversation's first turn binds it, and reached at the loopback, each request's bearer minted as it goes.
  const scopeOf = { harness, threadKey };
  await Effect.runPromise(workspaces.folders.bind(scopeOf));
  const proxy = await Effect.runPromise(makeBaymaProxy().pipe(Scope.provide(scope)));
  const bayma = await Effect.runPromise(proxy.reach(folderKey(harness, threadKey), workspaces.folders.endpoint(scopeOf)));
  const driven = harness === "claude" ? await claude(bayma) : await codex(bayma);
  try {
    const made = await driven.call("session.create", { runtime: "python", title: "e2e", cwd: "/tmp" });
    const sessionId = /"session_id"\s*:\s*"([^"]+)"/u.exec(made.text)?.[1];
    if (!sessionId) throw new Error(`no session made: ${made.text}`);
    const exec = (code: string) => driven.call("exec", { session_id: sessionId, code, yield_time_ms: 60_000 });
    const READ = "print(globals().get('kept'))";
    const kept = await exec("kept = 'kept through the restart'\nprint('kept')");
    await asked("idle");
    const afterIdle = await exec(READ);
    // Made once the gateway's pods are stopping, its connection refused until one is ready again.
    await asked("restarting", "deleted");
    const restarting = await exec(READ);
    await asked("restarting");
    // An exec of 20 s under way as the gateway's pods are deleted.
    const sleeping = exec("import time\ntime.sleep(20)\nprint('slept')");
    await sleep(5_000);
    await asked("calling");
    const underway = await sleeping;
    const afterUnderway = await exec(READ);
    await asked("down");
    const during = await exec(READ);
    await asked("up");
    const afterOutage = await exec(READ);
    const seen: FolderRestartSeen = { harness, kept, afterIdle, restarting, underway, afterUnderway, during, afterOutage, apiRequests };
    console.log(JSON.stringify(seen));
  } finally {
    await driven.close();
  }
} finally {
  for (const server of servers) server.close();
  await Effect.runPromise(Scope.close(scope, Exit.void));
  rmSync(home, { recursive: true, force: true });
}
