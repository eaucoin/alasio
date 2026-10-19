/**
 * Run inside alasio's pod, by node there (inAlasio in harness.ts): a folder conversation's
 * REPL sessions in its bayma, at `url`, with `token`, whichever of alasio's releases made
 * the bayma, so that what brings them back across an upgrade is what is proven.
 *
 * - `set <url> <token> <value>`: a Python session holding `value` live, in a global, and in
 *   its checkpoint (bayma_write_checkpoint), beside a thread counting in it that no checkpoint
 *   keeps, and a Bun session holding `value` live and in its `$checkpoint`; lets go of its
 *   control of both, and prints their ids and the count.
 * - `read <url> <token> <python> <bun>`: takes both over, as a client that reconnected does,
 *   and prints what each holds, the count and whether the thread counts on, and what bayma told
 *   each exec of how its session came back.
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { CallToolResult, ContentBlock } from "@modelcontextprotocol/sdk/types.js";

import type { BaymaExecResult, BaymaSessionCreated } from "./session-roundtrip.ts";

/** The sessions `set` made, and the count its thread was at. */
export interface FolderReplsSet {
  readonly python: string;
  readonly bun: string;
  readonly count: number;
}

/** What `read` saw of each session, and bayma's notice to each exec, if any. */
export interface FolderReplsRead {
  readonly python: { readonly live: string | null; readonly checkpointed: string | null; readonly count: number | null; readonly counting: boolean; readonly notice: string | null };
  readonly bun: { readonly live: string | null; readonly checkpointed: string | null; readonly notice: string | null };
}

const [verb, url, token, ...rest] = process.argv.slice(2);
if (!url || !token || !((verb === "set" && rest.length === 1) || (verb === "read" && rest.length === 2))) {
  throw new Error("usage: folder-repl.ts set <url> <token> <value> | read <url> <token> <python> <bun>");
}

/** The text of a part of a tool's answer, where bayma gives its JSON. */
function text(part: ContentBlock | undefined): string {
  if (part?.type !== "text") throw new Error(`bayma answered without text: ${JSON.stringify(part)}`);
  return part.text;
}

/**
 * How long a request waits for its answer: the gateway binds the folder instance as the first
 * one comes, its pod's image pulled first where its node has none, as a bayma of another
 * release's is, for as long as the gateway waits on a pull.
 */
const ANSWER_MS = 10 * 60_000;

/** Calls bayma's tool `name`, which must succeed: what it answered, as JSON, where it answers any. */
async function call<T>(client: Client, name: string, args: Record<string, unknown>): Promise<T> {
  const result = (await client.callTool({ name, arguments: args }, undefined, { timeout: ANSWER_MS })) as CallToolResult;
  if (result.isError) throw new Error(`bayma's ${name} failed: ${text(result.content[0])}`);
  if (result.structuredContent) return result.structuredContent as T;
  const said = result.content.find((part) => part.type === "text");
  return (said ? JSON.parse(text(said)) : null) as T;
}

/** Runs `code` in the session, given a minute, as one that comes back first is brought back by its exec: what it printed, and bayma's notice to it. */
async function exec(client: Client, sessionId: string, code: string): Promise<{ readonly stdout: string; readonly notice: string | null }> {
  const done = await call<BaymaExecResult>(client, "exec", { session_id: sessionId, code, yield_time_ms: 60_000 });
  if (done.error_text) throw new Error(`the exec failed: ${done.error_text}`);
  return { stdout: done.stdout_text ?? "", notice: done.notice ?? null };
}

const client = new Client({ name: "alasio-e2e", version: "1.0.0" });
// The transport is one; the SDK declares its sessionId `string | undefined` where Transport has it optional.
await client.connect(new StreamableHTTPClientTransport(new URL(url), { requestInit: { headers: { Authorization: `Bearer ${token}` } } }) as Transport, { timeout: ANSWER_MS });
try {
  if (verb === "set") {
    const value = JSON.stringify(rest[0]);
    // In a directory every bayma's pod has, of the operator's or of a session.
    const { session: python } = await call<BaymaSessionCreated>(client, "session.create", { runtime: "python", title: "e2e", cwd: "/tmp" });
    const counted = await exec(client, python.session_id, [
      "import threading, time",
      `kept = ${value}`,
      `bayma_write_checkpoint({"kept": ${value}})`,
      'state = {"count": 0}',
      "def count():",
      "    while True:",
      '        state["count"] += 1',
      "        time.sleep(0.2)",
      "counter = threading.Thread(target=count, daemon=True)",
      "counter.start()",
      "time.sleep(1)",
      'print(state["count"])',
    ].join("\n"));
    const { session: bun } = await call<BaymaSessionCreated>(client, "session.create", { runtime: "bun", title: "e2e", cwd: "/tmp" });
    await exec(client, bun.session_id, `globalThis.kept = ${value}; $checkpoint = { kept: ${value} }; console.log("set")`);
    // Let go of, so the next client takes them over at once.
    for (const session of [python, bun]) await call(client, "session.release_controller", { session_id: session.session_id });
    const set: FolderReplsSet = { python: python.session_id, bun: bun.session_id, count: Number(counted.stdout.trim()) };
    console.log(JSON.stringify(set));
  } else {
    const [python = "", bun = ""] = rest;
    for (const session of [python, bun]) await call(client, "session.acquire_controller", { session_id: session });
    const seen = await exec(client, python, [
      "import json, time",
      "time.sleep(0.5)",
      'print(json.dumps({"live": globals().get("kept"), "checkpointed": (bayma_read_checkpoint() or {}).get("kept"), "count": globals().get("state", {}).get("count"), "counting": "counter" in globals() and counter.is_alive()}))',
    ].join("\n"));
    const live = await exec(client, bun, "console.log(JSON.stringify({ live: globalThis.kept ?? null, checkpointed: $checkpoint?.kept ?? null }))");
    const read: FolderReplsRead = { python: { ...JSON.parse(seen.stdout), notice: seen.notice }, bun: { ...JSON.parse(live.stdout), notice: live.notice } };
    console.log(JSON.stringify(read));
  }
} finally {
  await client.close().catch(() => undefined);
}
