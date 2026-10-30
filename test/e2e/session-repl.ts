/**
 * Run inside alasio's pod, by node there (inAlasio in harness.ts): a REPL of a session's
 * bayma, a session without internet, reached as alasio reaches it, through the sandbox
 * gateway by alasio's environment's key, which binds it as it is reached, so that whatever
 * brings the session back is what is proven.
 *
 * - `set <volumeId> <value>`: a new Bun session holding `value` live, in a global, and in
 *   its `$checkpoint`, which bayma commits at the end of the exec; prints its id.
 * - `hold <volumeId> <path>`: a new Bun session holding `held until <path>` as `set` does,
 *   in an exec left running until there is a file at `path`; prints its id.
 * - `read <volumeId> <sessionId>`: takes the session over, as a client that reconnected
 *   does, and reads both back in an exec; prints what it held, and what bayma told the
 *   exec of how the session came back.
 * - `fill <volumeId> <mebibytes>`: a new Bun session holding that many random bytes live,
 *   which no checkpoint of bayma's keeps; prints its id and their SHA-256.
 * - `sum <volumeId> <sessionId>`: takes the session over and prints the SHA-256 of the bytes
 *   it holds live, null where it holds none, and bayma's notice to the exec.
 *
 * Each lets go of its control of the session before it ends, so that the next takes it over
 * at once: bayma keeps a controller's lease for as long as it sees its client connected, and
 * a client's close may reach it after the next client's acquire does.
 *
 * alasio's modules are imported by their paths in the repository, which inAlasio in
 * harness.ts rewrites to the image's as it pipes this in.
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { CallToolResult, ContentBlock } from "@modelcontextprotocol/sdk/types.js";

import { Effect } from "effect";

import { loadAlasioConfig } from "../../src/config.ts";
import { makeGatewayWorkspaces } from "../../src/sandbox/gateway-client.ts";
import type { BaymaExecResult, BaymaSessionCreated } from "./session-roundtrip.ts";

/** The REPL session `set` made. */
export interface ReplSet {
  readonly sessionId: string;
}

/** The REPL session `fill` made, and the SHA-256 of what it holds. */
export interface ReplFilled {
  readonly sessionId: string;
  readonly sha256: string;
}

/** What `sum` saw: the SHA-256 of the bytes the REPL holds live, null where it holds none, and bayma's notice to the exec, if any. */
export interface ReplSummed {
  readonly sha256: string | null;
  readonly notice: string | null;
}

/** What `read` saw: the value the REPL holds live and in its checkpoint, null where it holds none, and bayma's notice to the exec, if any. */
export interface ReplSeen {
  readonly live: string | null;
  readonly checkpointed: string | null;
  readonly notice: string | null;
}

const [verb, volumeId, argument] = process.argv.slice(2);
if (!volumeId || !argument || !["set", "hold", "read", "fill", "sum"].includes(verb ?? "")) {
  throw new Error("usage: session-repl.ts set <volumeId> <value> | hold <volumeId> <path> | read <volumeId> <sessionId> | fill <volumeId> <mebibytes> | sum <volumeId> <sessionId>");
}

/** The SHA-256 of the bytes the REPL holds live, as an exec prints it. */
const SUM = 'console.log(JSON.stringify(globalThis.filled ? new Bun.CryptoHasher("sha256").update(globalThis.filled).digest("hex") : null))';

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

/** Lets go of `client`'s control of the session `sessionId`, so the next client takes it over at once. */
const release = (client: Client, sessionId: string): Promise<unknown> => call(client, "session.release_controller", { session_id: sessionId });

/** Runs `code` in the session, given a minute, as one that comes back first is brought back by its exec: what the exec printed, and bayma's notice to it. */
async function exec(client: Client, sessionId: string, code: string): Promise<{ readonly stdout: string; readonly notice: string | null }> {
  const done = await call<BaymaExecResult>(client, "exec", { session_id: sessionId, code, yield_time_ms: 60_000 });
  if (done.error_text) throw new Error(`the exec failed: ${done.error_text}`);
  return { stdout: done.stdout_text ?? "", notice: done.notice ?? null };
}

const { sessionsGateway } = loadAlasioConfig();
if (!sessionsGateway) throw new Error("the installation gives alasio no sandbox gateway");
const bayma = await Effect.runPromise(makeGatewayWorkspaces(sessionsGateway).endpoint(volumeId, "none"));

const client = new Client({ name: "alasio-e2e", version: "1.0.0" });
// The transport is one; the SDK declares its sessionId `string | undefined` where Transport has it optional.
await client.connect(new StreamableHTTPClientTransport(new URL(bayma.url), { requestInit: { headers: bayma.headers } }) as Transport);
try {
  if (verb === "set") {
    const { session } = await call<BaymaSessionCreated>(client, "session.create", { runtime: "bun", title: "e2e", cwd: "/workspace" });
    const value = JSON.stringify(argument);
    await exec(client, session.session_id, `globalThis.kept = ${value}; $checkpoint = { kept: ${value} }; console.log("set")`);
    await release(client, session.session_id);
    const set: ReplSet = { sessionId: session.session_id };
    console.log(JSON.stringify(set));
  } else if (verb === "hold") {
    const { session } = await call<BaymaSessionCreated>(client, "session.create", { runtime: "bun", title: "e2e", cwd: "/workspace" });
    const value = JSON.stringify(`held until ${argument}`);
    const code = `globalThis.kept = ${value}; $checkpoint = { kept: ${value} }; while (!require("node:fs").existsSync(${JSON.stringify(argument)})) await Bun.sleep(500); console.log("went on")`;
    // Yielded at once: the exec runs on in bayma, its client gone.
    const started = await call<BaymaExecResult>(client, "exec", { session_id: session.session_id, code, yield_time_ms: 0 });
    if (started.error_text) throw new Error(`the exec failed: ${started.error_text}`);
    await release(client, session.session_id);
    const held: ReplSet = { sessionId: session.session_id };
    console.log(JSON.stringify(held));
  } else if (verb === "fill") {
    const { session } = await call<BaymaSessionCreated>(client, "session.create", { runtime: "bun", title: "e2e", cwd: "/workspace" });
    // Random, so that no page of it is one of zeros, which a save could skip.
    const { stdout } = await exec(client, session.session_id, `globalThis.filled = require("node:crypto").randomFillSync(Buffer.allocUnsafe(${Number(argument)} * 1024 * 1024)); ${SUM}`);
    await release(client, session.session_id);
    const filled: ReplFilled = { sessionId: session.session_id, sha256: JSON.parse(stdout) };
    console.log(JSON.stringify(filled));
  } else if (verb === "sum") {
    await call(client, "session.acquire_controller", { session_id: argument });
    const { stdout, notice } = await exec(client, argument, SUM);
    await release(client, argument);
    const summed: ReplSummed = { sha256: JSON.parse(stdout), notice };
    console.log(JSON.stringify(summed));
  } else {
    await call(client, "session.acquire_controller", { session_id: argument });
    const { stdout, notice } = await exec(client, argument, "console.log(JSON.stringify({ live: globalThis.kept ?? null, checkpointed: $checkpoint?.kept ?? null }))");
    await release(client, argument);
    const seen: ReplSeen = { ...JSON.parse(stdout), notice };
    console.log(JSON.stringify(seen));
  }
} finally {
  await client.close();
}
