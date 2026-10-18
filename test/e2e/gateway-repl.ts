/**
 * Run inside alasio's pod, by node there (inAlasio in harness.ts): a Python REPL of the bayma
 * of a pool's pod bound to a workspace, reached through the sandbox gateway as alasio reaches
 * it, at `url`, with a caller's token of the workspace's key, so that whatever binds the
 * workspace, a restore among them, is what is proven.
 *
 * - `hold <url> <token>`: a new session holding the workspace's files open: a writer appending
 *   a line a step to `held/counter` from a thread of its own, an SQLite database with a
 *   transaction under way, and `held/.git/index.lock`, as git holds it; prints its id and the
 *   step the writer is at.
 * - `check <url> <token> <sessionId>`: takes the session over, as a client that reconnected
 *   does, and prints what it holds of them: whether the writer runs on, at what step, whether
 *   every line it wrote is there in turn, whether the transaction under way commits whole,
 *   whether the lock is still held; and what bayma told the exec of how the session came back.
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { CallToolResult, ContentBlock } from "@modelcontextprotocol/sdk/types.js";

import type { BaymaExecResult, BaymaSessionCreated } from "./session-roundtrip.ts";

/** The session `hold` made, and the step its writer was at. */
export interface ReplHeld {
  readonly sessionId: string;
  readonly step: number;
}

/** What `check` saw of what the session held, and bayma's notice to the exec, if any. */
export interface ReplChecked {
  readonly writing: boolean;
  readonly step: number;
  readonly lines: number;
  readonly whole: boolean;
  readonly committed: number;
  readonly integrity: string;
  readonly locked: boolean;
  readonly notice: string | null;
}

const [verb, url, token, argument] = process.argv.slice(2);
if (!url || !token || (verb !== "hold" && !(verb === "check" && argument))) {
  throw new Error("usage: gateway-repl.ts hold <url> <token> | check <url> <token> <sessionId>");
}

const HOLD = [
  "import os, sqlite3, threading, time",
  'os.makedirs("/workspace/held/.git", exist_ok=True)',
  'os.chdir("/workspace/held")',
  'counter = open("counter", "a", buffering=1)',
  'lock = open(".git/index.lock", "w")',
  'db = sqlite3.connect("db.sqlite", check_same_thread=False, isolation_level=None)',
  'db.execute("create table if not exists t (n integer)")',
  'db.execute("begin")',
  'db.execute("insert into t values (1)")',
  'state = {"step": 0}',
  "def write():",
  "    while True:",
  '        state["step"] += 1',
  '        counter.write(f"{state[\'step\']}\\n")',
  "        time.sleep(0.2)",
  "writer = threading.Thread(target=write, daemon=True)",
  "writer.start()",
  "time.sleep(2)",
  'print(state["step"])',
].join("\n");

const CHECK = [
  "import json, os, time",
  "time.sleep(1)",
  'lines = open("/workspace/held/counter").read().split()',
  'db.execute("insert into t values (2)")',
  'db.execute("commit")',
  "print(json.dumps({",
  '    "writing": writer.is_alive(),',
  '    "step": state["step"],',
  '    "lines": len(lines),',
  '    "whole": all(int(line) == n + 1 for n, line in enumerate(lines)),',
  '    "committed": db.execute("select count(*) from t").fetchone()[0],',
  '    "integrity": db.execute("pragma integrity_check").fetchone()[0],',
  '    "locked": os.fstat(lock.fileno()).st_nlink == 1 and os.path.exists("/workspace/held/.git/index.lock"),',
  "}))",
].join("\n");

/** The text of a part of a tool's answer, where bayma gives its JSON. */
function text(part: ContentBlock | undefined): string {
  if (part?.type !== "text") throw new Error(`bayma answered without text: ${JSON.stringify(part)}`);
  return part.text;
}

/** Calls bayma's tool `name`, which must succeed: what it answered, as JSON. */
async function call<T>(client: Client, name: string, args: Record<string, unknown>): Promise<T> {
  const result = (await client.callTool({ name, arguments: args })) as CallToolResult;
  if (result.isError) throw new Error(`bayma's ${name} failed: ${text(result.content[0])}`);
  return (result.structuredContent as T | undefined) ?? JSON.parse(text(result.content.find((part) => part.type === "text")));
}

/** Runs `code` in the session, given a minute: what it printed, and bayma's notice to it. */
async function exec(client: Client, sessionId: string, code: string): Promise<{ readonly stdout: string; readonly notice: string | null }> {
  const done = await call<BaymaExecResult>(client, "exec", { session_id: sessionId, code, yield_time_ms: 60_000 });
  if (done.error_text) throw new Error(`the exec failed: ${done.error_text}`);
  return { stdout: done.stdout_text ?? "", notice: done.notice ?? null };
}

const client = new Client({ name: "alasio-e2e", version: "1.0.0" });
// The transport is one; the SDK declares its sessionId `string | undefined` where Transport has it optional.
await client.connect(new StreamableHTTPClientTransport(new URL(url), { requestInit: { headers: { Authorization: `Bearer ${token}` } } }) as Transport);
try {
  if (verb === "hold") {
    const { session } = await call<BaymaSessionCreated>(client, "session.create", { runtime: "python", title: "e2e", cwd: "/workspace" });
    const { stdout } = await exec(client, session.session_id, HOLD);
    const held: ReplHeld = { sessionId: session.session_id, step: Number(stdout.trim()) };
    console.log(JSON.stringify(held));
  } else {
    await call(client, "session.acquire_controller", { session_id: argument });
    const { stdout, notice } = await exec(client, argument ?? "", CHECK);
    const checked: ReplChecked = { ...JSON.parse(stdout), notice };
    console.log(JSON.stringify(checked));
  }
} finally {
  await client.close();
}
