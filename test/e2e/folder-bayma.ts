/**
 * Run inside alasio's pod, by node there (inAlasio in alasio.test.ts): a folder
 * conversation's bayma through alasio's own code, its instance bound through the sandbox
 * gateway as a turn binds it, with a file written through it to the operator's home.
 * Prints one JSON line of what it saw.
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
import { makeGatewayWorkspaces } from "../../src/sandbox/gateway-client.ts";
import type { BaymaExecResult, BaymaSessionCreated } from "./session-roundtrip.ts";

/** What the folder's bayma saw, as the last line prints it. */
export interface FolderBaymaSeen {
  readonly url: string;
  readonly seen: { readonly uid: number; readonly home: string };
}

/** The text of a part of a tool's answer, where bayma gives its JSON. */
function text(part: ContentBlock | undefined): string {
  if (part?.type !== "text") throw new Error(`bayma answered without text: ${JSON.stringify(part)}`);
  return part.text;
}

const { sessionsGateway, folders } = loadAlasioConfig();
if (!sessionsGateway || !folders) throw new Error("alasio is given no sandbox gateway that serves folder instances");
// Bound ahead, as a turn binds it, then reached at its endpoint.
const { folders: instances } = makeGatewayWorkspaces(sessionsGateway);
const scope = { harness: "claude", threadKey: "e2e:folder" };
const bayma = await Effect.runPromise(Effect.andThen(instances.bind(scope), instances.endpoint(scope)));
const client = new Client({ name: "alasio-e2e", version: "1.0.0" });
// The transport is one; the SDK declares its sessionId `string | undefined` where Transport has it optional.
await client.connect(new StreamableHTTPClientTransport(new URL(bayma.url), { requestInit: { headers: bayma.headers } }) as Transport);
try {
  // bayma answers in the current protocol, never with the old `toolResult`.
  const created = (await client.callTool({ name: "session.create", arguments: { runtime: "bun", title: "e2e", cwd: process.env["HOME"] } })) as CallToolResult;
  const { session }: BaymaSessionCreated = created.structuredContent ?? JSON.parse(text(created.content[0]));
  const code = 'await Bun.write(process.env.HOME + "/e2e-folder-proof", "written by " + process.getuid()); JSON.stringify({ uid: process.getuid(), home: process.env.HOME })';
  const result = (await client.callTool({ name: "exec", arguments: { session_id: session.session_id, code } })) as CallToolResult;
  const done: BaymaExecResult = result.structuredContent ?? JSON.parse(text(result.content.find((part) => part.type === "text")));
  if (done.result_text === undefined) throw new Error(`bayma's exec answered no result: ${JSON.stringify(done)}`);
  const seen: FolderBaymaSeen = { url: bayma.url, seen: JSON.parse(JSON.parse(done.result_text)) };
  console.log(JSON.stringify(seen));
} finally {
  await client.close();
}
