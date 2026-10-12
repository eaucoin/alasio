/**
 * Run inside alasio's pod, by node there (inAlasio in harness.ts): a REPL of a session's
 * bayma, reached as alasio reaches it, with the session's token, and never brought up
 * here, so that whatever brings the session back is what is proven.
 *
 * - `set <volumeId> <value>`: a new Bun session holding `value` live, in a global, and in
 *   its `$checkpoint`, which bayma commits at the end of the exec; prints its id.
 * - `read <volumeId> <sessionId>`: takes the session over, as a client that reconnected
 *   does, and reads both back in an exec; prints what it held, and what bayma told the
 *   exec of how the session came back.
 *
 * alasio's modules are imported by their paths in the repository, which inAlasio in
 * harness.ts rewrites to the image's as it pipes this in.
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { CallToolResult, ContentBlock } from "@modelcontextprotocol/sdk/types.js";

import { Effect } from "effect";

import { KubeClient } from "../../src/kube/client.ts";
import { loadKubeTemplates } from "../../src/kube/config.ts";
import { type BaymaEndpoint, makeSandboxes } from "../../src/kube/sandboxes.ts";
import type { BaymaExecResult, BaymaSessionCreated } from "./session-roundtrip.ts";

/** The REPL session `set` made. */
export interface ReplSet {
  readonly sessionId: string;
}

/** What `read` saw: the value the REPL holds live and in its checkpoint, null where it holds none, and bayma's notice to the exec, if any. */
export interface ReplSeen {
  readonly live: string | null;
  readonly checkpointed: string | null;
  readonly notice: string | null;
}

const [verb, volumeId, argument] = process.argv.slice(2);
if (!volumeId || !argument || (verb !== "set" && verb !== "read")) throw new Error("usage: session-repl.ts set <volumeId> <value> | read <volumeId> <sessionId>");

/** The text of a part of a tool's answer, where bayma gives its JSON. */
function text(part: ContentBlock | undefined): string {
  if (part?.type !== "text") throw new Error(`bayma answered without text: ${JSON.stringify(part)}`);
  return part.text;
}

/** Calls bayma's tool `name`, which must succeed: what it answered, as JSON. */
async function call<T>(client: Client, name: string, args: Record<string, unknown>): Promise<T> {
  // bayma answers in the current protocol, never with the old `toolResult`.
  const result = (await client.callTool({ name, arguments: args })) as CallToolResult;
  if (result.isError) throw new Error(`bayma's ${name} failed: ${text(result.content[0])}`);
  // A tool's structured answer is its output schema's.
  return (result.structuredContent as T | undefined) ?? JSON.parse(text(result.content.find((part) => part.type === "text")));
}

/** Runs `code` in the session, given a minute, as one that comes back first is brought back by its exec: what the exec printed, and bayma's notice to it. */
async function exec(client: Client, sessionId: string, code: string): Promise<{ readonly stdout: string; readonly notice: string | null }> {
  const done = await call<BaymaExecResult>(client, "exec", { session_id: sessionId, code, yield_time_ms: 60_000 });
  if (done.error_text) throw new Error(`the exec failed: ${done.error_text}`);
  return { stdout: done.stdout_text ?? "", notice: done.notice ?? null };
}

const bayma = await Effect.runPromise(Effect.gen(function*() {
  const { sessions } = yield* loadKubeTemplates;
  if (!sessions) return yield* Effect.die(new Error("the installation gives no sessions template"));
  const token = yield* (yield* makeSandboxes({ namespace: sessions.namespace, port: sessions.port })).token(volumeId);
  if (!token) return yield* Effect.die(new Error(`the session ${volumeId} has no token`));
  const endpoint: BaymaEndpoint = { url: `http://${volumeId}.${sessions.namespace}.svc.cluster.local:${sessions.port}/mcp`, headers: { Authorization: `Bearer ${token}` } };
  return endpoint;
}).pipe(Effect.scoped, Effect.provide(KubeClient.layer)));

const client = new Client({ name: "alasio-e2e", version: "1.0.0" });
// The transport is one; the SDK declares its sessionId `string | undefined` where Transport has it optional.
await client.connect(new StreamableHTTPClientTransport(new URL(bayma.url), { requestInit: { headers: bayma.headers } }) as Transport);
try {
  if (verb === "set") {
    const { session } = await call<BaymaSessionCreated>(client, "session.create", { runtime: "bun", title: "e2e", cwd: "/workspace" });
    const value = JSON.stringify(argument);
    await exec(client, session.session_id, `globalThis.kept = ${value}; $checkpoint = { kept: ${value} }; console.log("set")`);
    const set: ReplSet = { sessionId: session.session_id };
    console.log(JSON.stringify(set));
  } else {
    await call(client, "session.acquire_controller", { session_id: argument });
    const { stdout, notice } = await exec(client, argument, "console.log(JSON.stringify({ live: globalThis.kept ?? null, checkpointed: $checkpoint?.kept ?? null }))");
    const seen: ReplSeen = { ...JSON.parse(stdout), notice };
    console.log(JSON.stringify(seen));
  }
} finally {
  await client.close();
}
