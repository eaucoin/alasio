/**
 * Run inside alasio's pod, by node there, with a session's volume id (inAlasio in
 * alasio.test.ts): drives one session through alasio's own Kubernetes driver and its
 * ServiceAccount's permissions. bayma answers over MCP with the session's token, writes
 * a file in the workspace, which alasio reads back through exec; the session is
 * suspended, resumed, and the file is still there. Prints one JSON line of what it saw.
 *
 * alasio's modules are imported by their paths in the repository, which inAlasio in
 * alasio.test.ts rewrites to the image's as it pipes this in.
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { CallToolResult, ContentBlock } from "@modelcontextprotocol/sdk/types.js";

import { Effect, Schedule } from "effect";

import { KubeClient } from "../../src/kube/client.ts";
import { loadKubeTemplates, type SessionsProfile } from "../../src/kube/config.ts";
import {
  type BaymaEndpoint,
  makeSandboxes,
  POD_TEMPLATE_ANNOTATION,
  podTemplateHash,
  SANDBOX_API_VERSION,
  SANDBOX_KIND,
  type Sandbox,
} from "../../src/kube/sandboxes.ts";
import { SessionSandboxes } from "../../src/sandbox/index.ts";

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
  podReplaced: boolean;
  movedPodLabel?: string | undefined;
  execAfterMove?: string | undefined;
}

/** The pod label the round trip's other pod template adds. */
const MOVED_LABEL = "alasio.dev/e2e-moved";

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

/** The round trip, through alasio's session filesystems on the `sessions` profile. */
const roundtrip = (profile: SessionsProfile) => Effect.gen(function*() {
  const sessions = yield* SessionSandboxes;
  const kube = yield* KubeClient;
  const seen: Partial<RoundtripSeen> = {};
  const read = (path: string, maxBytes: number) => sessions.readFile(volumeId, path, maxBytes);

  // Brought up as alasio made it, which this script, without alasio's telemetry settings,
  // would not make it: from the Sandbox as it is, whose pod template is its own.
  const sandboxes = yield* makeSandboxes({ namespace: profile.namespace, port: profile.port });
  // A Sandbox, as the API server returns one.
  const made = (yield* kube.read(SANDBOX_API_VERSION, SANDBOX_KIND, profile.namespace, volumeId)) as Sandbox | null;
  if (!made) return yield* Effect.die(new Error(`alasio made no Sandbox for ${volumeId}`));
  const bringUp = sandboxes.ensure(volumeId, () => made);

  let bayma = yield* bringUp;
  const wrote = yield* Effect.promise(() => exec(bayma, 'await Bun.write("/workspace/out/hello.txt", "hello from " + require("os").release()); "written"'));
  seen.exec = wrote.result_text || JSON.stringify(wrote).slice(0, 300);
  seen.read = (yield* read("out/hello.txt", 1024)).bytes?.toString("utf8");
  seen.missing = (yield* read("out/nope.txt", 1024)).note;
  seen.outside = (yield* read("/etc/passwd", 4096)).bytes ? "read /etc/passwd of the sandbox, not the host" : "refused";

  yield* sandboxes.suspend(volumeId);
  // The pod goes within a minute of the suspension.
  const pod = yield* kube.read("v1", "Pod", profile.namespace, volumeId).pipe(
    Effect.repeat({ schedule: Schedule.spaced("1 second"), times: 59, while: (pod) => pod !== null }),
  );
  seen.suspendedPod = pod ? "still there" : "gone";
  seen.whileSuspended = (yield* read("out/hello.txt", 1024)).note;
  const resumedAt = Date.now();
  bayma = yield* bringUp;
  seen.resumeMs = Date.now() - resumedAt;
  seen.afterResume = (yield* read("out/hello.txt", 1024)).bytes?.toString("utf8");
  const again = yield* Effect.promise(() => exec(bayma, 'require("fs").readFileSync("/workspace/out/hello.txt", "utf8")'));
  seen.execAfterResume = again.result_text;

  // Brought up with another pod template, as after an upgrade: its pod is one of that
  // template, and its files are kept.
  const currentPod = kube.read("v1", "Pod", profile.namespace, volumeId);
  const podBefore = yield* currentPod;
  const moved = structuredClone(made);
  moved.spec.podTemplate.metadata = { ...moved.spec.podTemplate.metadata, labels: { ...moved.spec.podTemplate.metadata?.labels, [MOVED_LABEL]: "yes" } };
  moved.metadata.annotations = { ...moved.metadata.annotations, [POD_TEMPLATE_ANNOTATION]: podTemplateHash(moved.spec.podTemplate) };
  bayma = yield* sandboxes.ensure(volumeId, () => moved);
  const podAfter = yield* currentPod;
  seen.podReplaced = !!podBefore?.metadata?.uid && !!podAfter?.metadata?.uid && podBefore.metadata.uid !== podAfter.metadata.uid;
  seen.movedPodLabel = podAfter?.metadata?.labels?.[MOVED_LABEL];
  const afterMove = yield* Effect.promise(() => exec(bayma, 'require("fs").readFileSync("/workspace/out/hello.txt", "utf8")'));
  seen.execAfterMove = afterMove.result_text;
  return seen;
});

const seen = await Effect.runPromise(Effect.gen(function*() {
  const { sessions } = yield* loadKubeTemplates;
  if (!sessions) return yield* Effect.die(new Error("the installation gives no sessions template"));
  return yield* roundtrip(sessions).pipe(Effect.provide(SessionSandboxes.layer({ profile: sessions, stateDir: "/tmp/roundtrip", env: {} })));
}).pipe(Effect.scoped, Effect.provide(KubeClient.layer)));
console.log(JSON.stringify(seen));
