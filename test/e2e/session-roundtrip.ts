/**
 * Run inside alasio's pod, by node there, with a session's volume id (inAlasio in
 * alasio.test.ts): drives one session through alasio's own session filesystems, through the
 * sandbox gateway, by alasio's environment's key to it. bayma answers over MCP through the
 * gateway, writes a file in the workspace, which alasio reads back through the gateway, and
 * nothing outside the session's files; the session is released, its file still read while it
 * is bound to no pod, and brought up again with it. Prints one JSON line of what it saw.
 *
 * alasio's modules are imported by their paths in the repository, which inAlasio in
 * alasio.test.ts rewrites to the image's as it pipes this in.
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { CallToolResult, ContentBlock } from "@modelcontextprotocol/sdk/types.js";

import { Effect } from "effect";

import { loadAlasioConfig } from "../../src/config.ts";
import type { BaymaEndpoint } from "../../src/kube/sandboxes.ts";
import { makeGatewayWorkspaces } from "../../src/sandbox/gateway-client.ts";
import { makeSessionSandboxes } from "../../src/sandbox/index.ts";

/** What bayma's session.create answers, as far as the end-to-end scripts read it. */
export interface BaymaSessionCreated {
  readonly session: { readonly session_id: string };
}

/** What bayma's exec answers, as far as the end-to-end scripts read it. */
export interface BaymaExecResult {
  readonly stdout_text?: string;
  readonly result_text?: string;
  readonly error_text?: string;
  /** What bayma tells the first exec a session runs after it came back, of how it did. */
  readonly notice?: string;
}

/** What the round trip saw, as its last line prints it; what it could not read is left out. */
export interface RoundtripSeen {
  exec: string;
  read?: string | undefined;
  missing?: string | undefined;
  outside?: string | undefined;
  whileReleased?: string | undefined;
  execAfterBind?: string | undefined;
}

const volumeId = process.argv[2];
if (!volumeId) throw new Error("usage: session-roundtrip.ts <volumeId>");

/** The text of a part of a tool's answer, where bayma gives its JSON. */
function text(part: ContentBlock | undefined): string {
  if (part?.type !== "text") throw new Error(`bayma answered without text: ${JSON.stringify(part)}`);
  return part.text;
}

async function exec(bayma: BaymaEndpoint, code: string): Promise<BaymaExecResult> {
  const client = new Client({ name: "alasio-e2e", version: "1.0.0" });
  // The transport is one; the SDK declares its sessionId `string | undefined` where Transport has it optional.
  await client.connect(new StreamableHTTPClientTransport(new URL(bayma.url), { requestInit: { headers: bayma.headers } }) as Transport);
  try {
    // bayma answers in the current protocol, never with the old `toolResult`.
    const created = (await client.callTool({ name: "session.create", arguments: { runtime: "bun", title: "e2e", cwd: "/workspace" } })) as CallToolResult;
    const { session }: BaymaSessionCreated = JSON.parse(text(created.content[0]));
    const result = (await client.callTool({ name: "exec", arguments: { session_id: session.session_id, code } })) as CallToolResult;
    return result.structuredContent ?? JSON.parse(text(result.content.find((part) => part.type === "text")));
  } finally {
    await client.close();
  }
}

const { sessionsGateway: gateway } = loadAlasioConfig();
if (!gateway) throw new Error("the installation gives alasio no sandbox gateway");
const workspaces = makeGatewayWorkspaces(gateway);
// As alasio records it: a session made with no internet.
const sessions = makeSessionSandboxes({ gateway, stateDir: "/tmp/roundtrip", netModeOf: () => Effect.succeed("none"), workspaces });

const seen = await Effect.runPromise(Effect.gen(function*() {
  const seen: Partial<RoundtripSeen> = {};
  const read = (path: string, maxBytes: number) => sessions.readFile(volumeId, path, maxBytes);
  const { bayma } = yield* sessions.ensureSession(volumeId);
  const wrote = yield* Effect.promise(() => exec(bayma, 'await Bun.write("/workspace/out/hello.txt", "hello from " + require("os").release()); "written"'));
  seen.exec = wrote.result_text || JSON.stringify(wrote).slice(0, 300);
  seen.read = (yield* read("out/hello.txt", 1024)).bytes?.toString("utf8");
  seen.missing = (yield* read("out/nope.txt", 1024)).note;
  seen.outside = (yield* read("/etc/passwd", 4096)).note;
  // Released: its pod gone, its files read all the same, through the gateway's node agents.
  yield* workspaces.release(volumeId, "none");
  seen.whileReleased = (yield* read("out/hello.txt", 1024)).bytes?.toString("utf8");
  // Brought up again, by a pod of its own, with them.
  const again = yield* sessions.ensureSession(volumeId);
  const after = yield* Effect.promise(() => exec(again.bayma, 'require("fs").readFileSync("/workspace/out/hello.txt", "utf8")'));
  seen.execAfterBind = after.result_text;
  return seen;
}));
console.log(JSON.stringify(seen));
