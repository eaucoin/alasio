/**
 * Run inside alasio's pod (kubectl exec deploy/alasio -- node <this> <volumeId>): drives one
 * session through alasio's own Kubernetes driver and its ServiceAccount's permissions.
 * bayma answers over MCP with the session's token, writes a file in the workspace, which
 * alasio reads back through exec; the session is suspended, resumed, and the file is
 * still there. Prints one JSON line of what it saw.
 *
 * alasio's modules are imported by their paths in the repository, which inAlasio in
 * alasio.test.ts rewrites to the image's as it pipes this in.
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { CallToolResult, ContentBlock } from "@modelcontextprotocol/sdk/types.js";

import { loadKubeTemplates } from "../../src/kube/config.ts";
import { createKubeClient } from "../../src/kube/client.ts";
import { createSandboxes, type BaymaEndpoint } from "../../src/kube/sandboxes.ts";
import { createSandbox } from "../../src/sandbox/index.ts";

/** What bayma's session.create answers, as far as the end-to-end scripts read it. */
export interface BaymaSessionCreated {
  readonly session: { readonly session_id: string };
}

/** What bayma's exec answers, as far as the end-to-end scripts read it. */
export interface BaymaExecResult {
  readonly result_text?: string;
}

/** What the round trip saw, as its last line prints it; what it could not read is left out. */
export interface RoundtripSeen {
  exec: string;
  read?: string | undefined;
  missing?: string | undefined;
  outside: string;
  suspendedPod: "still there" | "gone";
  whileSuspended?: string | undefined;
  resumeMs: number;
  afterResume?: string | undefined;
  execAfterResume?: string | undefined;
}

const volumeId = process.argv[2];
if (!volumeId) throw new Error("usage: session-roundtrip.ts <volumeId>");
const templates = loadKubeTemplates();
const kube = createKubeClient();
const sandbox = createSandbox({ templates, stateDir: "/tmp/roundtrip", kube, env: {} });
if (!sandbox || !templates.sessions) throw new Error("the release renders no sessions template");
const seen: Partial<RoundtripSeen> = {};

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

let { bayma } = await sandbox.ensureSession(volumeId);
const wrote = await exec(bayma, 'await Bun.write("/workspace/out/hello.txt", "hello from " + require("os").release()); "written"');
seen.exec = wrote.result_text || JSON.stringify(wrote).slice(0, 300);
seen.read = (await sandbox.readFile(volumeId, "out/hello.txt", 1024)).bytes?.toString("utf8");
seen.missing = (await sandbox.readFile(volumeId, "out/nope.txt", 1024)).note;
seen.outside = (await sandbox.readFile(volumeId, "/etc/passwd", 4096)).bytes ? "read /etc/passwd of the sandbox, not the host" : "refused";

const sandboxes = createSandboxes({ kube, namespace: templates.sessions.namespace, port: templates.sessions.port });
await sandboxes.suspend(volumeId);
for (let i = 0; i < 60; i++) {
  if (!(await kube.read("v1", "Pod", templates.sessions.namespace, volumeId))) break;
  await new Promise((resolve) => setTimeout(resolve, 1000));
}
seen.suspendedPod = (await kube.read("v1", "Pod", templates.sessions.namespace, volumeId)) ? "still there" : "gone";
seen.whileSuspended = (await sandbox.readFile(volumeId, "out/hello.txt", 1024)).note;
const resumedAt = Date.now();
({ bayma } = await sandbox.ensureSession(volumeId));
seen.resumeMs = Date.now() - resumedAt;
seen.afterResume = (await sandbox.readFile(volumeId, "out/hello.txt", 1024)).bytes?.toString("utf8");
const again = await exec(bayma, 'require("fs").readFileSync("/workspace/out/hello.txt", "utf8")');
seen.execAfterResume = again.result_text;
console.log(JSON.stringify(seen));
await sandbox.close();
