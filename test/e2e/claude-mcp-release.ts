/**
 * Run inside alasio's pod, by node there (inAlasio in harness.ts), as `claude-mcp-release.ts
 * <url> <token> <mark>`: whether Claude Code's MCP client, given a session's bayma once through
 * the sandbox gateway at `url` with the caller's token `token`, keeps reaching it across the
 * session's releases and its pod's loss, in the same process, its MCP server configured once;
 * `mark` names the file of the workspace its cut call writes as it starts. The CLI is the one alasio pins (the Agent
 * SDK's own), with the options a session conversation's Claude Code is given
 * (buildClaudeQueryOptions, sessionFsQueryOptions), against a stand-in for Anthropic's
 * Messages API on the loopback: a prompt `CALL <tool> <arguments>` calls bayma's tool `tool`
 * with `arguments`, and its result is answered with, as `RESULT ok|error <its text>`.
 *
 * Its steps:
 * 1. an exec writes a file of the workspace and keeps a value in its REPL;
 * 2. the binding is released through the gateway (POST /release), and waited out until the
 *    gateway says the workspace is bound nowhere;
 * 3. an exec, in the same process, reads the file and the value: the call binds it again;
 * 4. an exec of 20 s is under way as the binding is released again: the release waits for the
 *    call to end (the front holds a binding while its requests are under way), the call answered;
 * 5. an exec of 30 s writes the file `mark` names as it starts, and the test, seeing it, deletes the bound pod,
 *    a real loss mid-call: what that call came to, and whether the CLI sent it again;
 * 6. the next exec reads the file, taking the session's control over first where bayma says
 *    another holds it (session.acquire_controller), as a snapshot taken with a call's connection
 *    open keeps that connection its controller.
 * A step whose REPL session the bind after a release does not have, as where the pools run no
 * gVisor and so snapshot nothing, makes a session anew and says so. It prints what each step
 * came to, the CLI's own lines of its MCP client, and how often it asked the Messages API.
 */
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { createServer, type IncomingMessage } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { query, type SDKMessage, type SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";

import { buildClaudeQueryOptions } from "../../src/harness/claude/runtime.ts";

/** What a step's tool call came to: whether the tool answered, or failed, and what it said. */
export interface CallSeen {
  readonly ok: boolean;
  readonly text: string;
  readonly ms: number;
}

/** What it saw, as the script's last line prints it. */
export interface ReleaseSeen {
  /** Whether the CLI the SDK ran is its own, not an override. */
  readonly pinned: boolean;
  readonly wrote: CallSeen;
  /** What the release answered, and how long until the gateway said it was bound nowhere. */
  readonly released: { readonly status: number; readonly unboundMs: number };
  /** The read after the release, in the session the first exec ran in. */
  readonly read: CallSeen;
  /** Whether the read had to make a session anew, its REPL session gone with the release. */
  readonly sessionAnew: boolean;
  /** What the 20 s exec under way as the binding was released came to; what the release answered, and whether it came after the call. */
  readonly inFlight: CallSeen & { readonly releaseStatus: number; readonly releasedAfter: boolean };
  /** What the 30 s exec under way as its pod was deleted came to. */
  readonly cut: CallSeen;
  /** The read after it, and whether it had to take the session's control over first. */
  readonly after: CallSeen;
  readonly tookControl: boolean;
  /** What each takeover of the session's control answered, in turn. */
  readonly takeovers: readonly CallSeen[];
  /** Whether the CLI's MCP client sent a call again after it reconnected, as its debug log says. */
  readonly retried: boolean;
  /** The CLI's lines of its MCP client's connection to bayma, from its debug log. */
  readonly mcpLines: readonly string[];
  /** How often the CLI asked the Messages API. */
  readonly apiRequests: number;
}

const [url, token, mark] = process.argv.slice(2);
if (!url || !token || !mark) throw new Error("usage: claude-mcp-release.ts <bayma url through the gateway> <token> <mark>");
/** Its bayma through the gateway, as the arguments give it. */
const bayma: string = url;
const gateway = new URL(bayma).origin;
const authorization = `Bearer ${token}`;

type Block = { readonly type: string; readonly text?: string; readonly content?: unknown; readonly is_error?: boolean };

async function bodyOf(incoming: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of incoming) chunks.push(chunk as Buffer);
  const text = Buffer.concat(chunks).toString("utf8");
  return text ? JSON.parse(text) : undefined;
}

/** The text of a tool's result, as Claude Code gives it back. */
const resultText = (content: unknown): string =>
  typeof content === "string" ? content : Array.isArray(content) ? content.map((part: { text?: string }) => part.text ?? "").join("") : JSON.stringify(content);

/** A tool's name as Claude Code offers an MCP server's: of its characters, those a tool's name takes. */
const offeredAs = (tool: string) => `mcp__bayma__${tool.replace(/[^a-zA-Z0-9_-]/gu, "_")}`;

let apiRequests = 0;
const api = createServer(async (incoming, outgoing) => {
  const asked = (await bodyOf(incoming)) as { messages?: { content: string | Block[] }[]; tools?: { name: string }[]; stream?: boolean } | undefined;
  if (!incoming.url?.startsWith("/v1/messages") || !asked?.messages) return void outgoing.writeHead(200, { "content-type": "application/json" }).end("{}");
  apiRequests += 1;
  const last = asked.messages.at(-1)?.content ?? "";
  const blocks: readonly Block[] = typeof last === "string" ? [{ type: "text", text: last }] : last;
  const result = blocks.find((block) => block.type === "tool_result");
  const said = blocks.map((block) => block.text ?? "").join(" ");
  const call = result ? null : /CALL (\S+) (\{.*\})/su.exec(said);
  const name = call?.[1] ? offeredAs(call[1]) : null;
  const calling = name !== null && (asked.tools ?? []).some((tool) => tool.name === name);
  const block = calling
    ? { type: "tool_use", id: `toolu_${randomUUID().replaceAll("-", "")}`, name, input: JSON.parse(call?.[2] ?? "{}") }
    : { type: "text", text: result ? `RESULT ${result.is_error ? "error" : "ok"} ${resultText(result.content)}` : call ? `NO TOOL ${name}` : "ok" };
  const stop = calling ? "tool_use" : "end_turn";
  const message = { id: `msg_${randomUUID()}`, type: "message", role: "assistant", model: "claude-sonnet-4-5", stop_sequence: null, usage: { input_tokens: 1, output_tokens: 1 } };
  if (!asked.stream) return void outgoing.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ ...message, content: [block], stop_reason: stop }));
  outgoing.writeHead(200, { "content-type": "text/event-stream" });
  const send = (type: string, data: object) => outgoing.write(`event: ${type}\ndata: ${JSON.stringify({ type, ...data })}\n\n`);
  send("message_start", { message: { ...message, content: [], stop_reason: null } });
  send("content_block_start", { index: 0, content_block: calling ? { ...block, input: {} } : { type: "text", text: "" } });
  send("content_block_delta", { index: 0, delta: calling ? { type: "input_json_delta", partial_json: JSON.stringify((block as { input: object }).input) } : { type: "text_delta", text: (block as { text: string }).text } });
  send("content_block_stop", { index: 0 });
  send("message_delta", { delta: { stop_reason: stop, stop_sequence: null }, usage: { output_tokens: 1 } });
  send("message_stop", {});
  outgoing.end();
});
api.listen(0, "127.0.0.1");
await once(api, "listening");

const home = mkdtempSync(join(tmpdir(), "alasio-mcp-release-"));
const debugFile = join(home, "debug.log");
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
// The options of a session conversation's Claude Code, of no override of alasio's environment: the SDK's own CLI.
const options = buildClaudeQueryOptions({ workingDirectory: home, claudeEnv, sessionFsBayma: { url: bayma, headers: { Authorization: authorization } }, controller, hooks: {}, env: {} });
const running = query({ prompt: channel(), options: { ...options, model: "claude-sonnet-4-5", debugFile } });
const results: string[] = [];
const reading = (async () => {
  for await (const message of running as AsyncIterable<SDKMessage>) {
    if (message.type === "result") results.push(message.subtype === "success" ? message.result : message.subtype);
  }
})();

/** A turn calling bayma's `tool` with `args`: what the call came to. */
async function call(tool: string, args: object): Promise<CallSeen> {
  const started = Date.now();
  const count = results.length + 1;
  const message: SDKUserMessage = { type: "user", message: { role: "user", content: `CALL ${tool} ${JSON.stringify(args)}` }, parent_tool_use_id: null, session_id: "" };
  const resolve = waiting.shift();
  if (resolve) resolve(message);
  else prompts.push(message);
  for (const deadline = Date.now() + 600_000; results.length < count; await new Promise((wake) => setTimeout(wake, 100))) {
    if (Date.now() > deadline) throw new Error(`no answer to CALL ${tool}: ${JSON.stringify(results)}`);
  }
  const answer = results[count - 1] ?? "";
  const seen = /^RESULT (ok|error) (.*)$/su.exec(answer);
  if (!seen) throw new Error(`CALL ${tool} called no tool: ${answer}`);
  return { ok: seen[1] === "ok" && !/"error_text"\s*:\s*"[^"]/u.test(seen[2] ?? ""), text: seen[2] ?? "", ms: Date.now() - started };
}

/** A new REPL session of bayma's: its id. */
async function newSession(): Promise<string> {
  const made = await call("session.create", { runtime: "python", title: "e2e", cwd: "/workspace" });
  const id = /"session_id"\s*:\s*"([^"]+)"/u.exec(made.text)?.[1];
  if (!id) throw new Error(`no session made: ${made.text}`);
  return id;
}

const exec = (sessionId: string, code: string) => call("exec", { session_id: sessionId, code, yield_time_ms: 60_000 });
const READ = "print(open('/workspace/mcp-release').read(), globals().get('kept'))";

/** Releases the binding through the gateway, as its caller asks: what it answered. */
const release = async () => (await fetch(`${gateway}/release`, { method: "POST", headers: { authorization } })).status;

/** Waits until the gateway says the workspace is bound nowhere: how long that took. */
async function unbound(): Promise<number> {
  const started = Date.now();
  for (const deadline = started + 300_000; ; await new Promise((wake) => setTimeout(wake, 1_000))) {
    const asked = await fetch(bayma, { method: "POST", headers: { authorization, "x-alasio-bind": "never", "content-type": "application/json" }, body: "{}" });
    await asked.arrayBuffer();
    if (asked.status === 409) return Date.now() - started;
    if (Date.now() > deadline) throw new Error(`the workspace was still bound ${(Date.now() - started) / 1000} s after its release: ${asked.status}`);
  }
}

/** Reads the file and the value in `sessionId`, or, where the bind has no such session, in one made anew. */
async function readBack(sessionId: string): Promise<{ readonly seen: CallSeen; readonly sessionId: string; readonly anew: boolean }> {
  const seen = await exec(sessionId, READ);
  if (seen.ok || !/session/iu.test(seen.text)) return { seen, sessionId, anew: false };
  const made = await newSession();
  return { seen: await exec(made, READ), sessionId: made, anew: true };
}

try {
  const first = await newSession();
  const wrote = await exec(first, "open('/workspace/mcp-release', 'w').write('kept through the release')\nkept = 42\nprint('wrote')");
  const releasedStatus = await release();
  const unboundMs = await unbound();
  const read = await readBack(first);
  // A call of 20 s under way as the binding is released: the release waits for it.
  let answeredAt = 0;
  const sleeping = exec(read.sessionId, "import time\ntime.sleep(20)\nprint('slept')").then((seen) => ((answeredAt = Date.now()), seen));
  await new Promise((wake) => setTimeout(wake, 5_000));
  const releaseStatus = await release();
  const releasedAt = Date.now();
  const inFlight = await sleeping;
  await unbound();
  // A call of 30 s under way as its pod is deleted, by the test, which sees it start by its mark.
  const cut = await exec(read.sessionId, `open('/workspace/${mark}', 'w').write('cut')\nimport time\ntime.sleep(30)\nprint('slept')`);
  // Its control taken over where bayma says another's connection holds it, and asked again, as an agent told so does:
  // a few times, as the client's MCP connection may be made anew more than once as it reconnects, each its own.
  const takeovers: CallSeen[] = [];
  let asked = await exec(read.sessionId, READ);
  while (!asked.ok && /controller lease/iu.test(asked.text) && takeovers.length < 5) {
    takeovers.push(await call("session.acquire_controller", { session_id: read.sessionId }));
    await new Promise((wake) => setTimeout(wake, 2_000));
    asked = await exec(read.sessionId, READ);
  }
  const tookControl = takeovers.length > 0;
  const after = asked.ok || tookControl || !/session/iu.test(asked.text) ? { seen: asked } : await readBack(read.sessionId);

  const logged = (() => {
    try {
      return readFileSync(debugFile, "utf8").split("\n").filter((line) => /MCP server "bayma"/u.test(line));
    } catch {
      return [];
    }
  })();
  const mcpLines = logged.filter((line) => /reconnect|retry|expired|dropped|failed|closed|lost|error/iu.test(line)).slice(-40);
  const seen: ReleaseSeen = {
    pinned: options.pathToClaudeCodeExecutable === undefined,
    wrote,
    released: { status: releasedStatus, unboundMs },
    read: read.seen,
    sessionAnew: read.anew,
    inFlight: { ...inFlight, releaseStatus, releasedAfter: answeredAt > 0 && releasedAt >= answeredAt },
    cut,
    after: after.seen,
    tookControl,
    takeovers,
    retried: logged.some((line) => /Retrying tool/u.test(line)),
    mcpLines,
    apiRequests,
  };
  console.log(JSON.stringify(seen));
} finally {
  controller.abort();
  await reading.catch(() => undefined);
  api.close();
  rmSync(home, { recursive: true, force: true });
}
