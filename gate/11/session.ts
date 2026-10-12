/**
 * Run inside alasio's pod, by node there (inAlasio in test/e2e/harness.ts), for 11's gate
 * (./restore.test.ts): a session made and brought up through alasio's own session
 * filesystems, with live state in its bayma, and that state read back after the gate
 * restored the session's pod. Prints one JSON line.
 *
 *   session.ts create [bare]                a session without internet, a Bun REPL in it
 *                                           with a variable, a file in /tmp, and a file
 *                                           in /tmp it holds open; prints `Created`.
 *                                           `bare`: its pod without init containers,
 *                                           never restarted, its /tmp gVisor's tmpfs
 *   session.ts read <volumeId> <replId>     the REPL's state, as a new client of its
 *                                           bayma sees it; prints `Read`
 *
 * alasio's modules are imported by their paths in the repository, which inAlasio
 * rewrites to the image's as it pipes this in.
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { Effect } from "effect";

import { KubeClient } from "../../src/kube/client.ts";
import { loadKubeTemplates, type SessionsProfile } from "../../src/kube/config.ts";
import { type BaymaEndpoint, makeSandboxes } from "../../src/kube/sandboxes.ts";
import { SessionSandboxes } from "../../src/sandbox/index.ts";
import { newVolumeId } from "../../src/sandbox/names.ts";

export interface Created {
  readonly volumeId: string;
  readonly replId: string;
  readonly said: string;
}

export interface Read {
  /** `marker + 1`: 42 when the REPL came back whole. */
  readonly marker: string;
  /** The file written to /tmp, read anew. */
  readonly tmpFile: string;
  /** The file held open in /tmp, read through the descriptor the REPL kept. */
  readonly heldFile: string;
  /** What bayma told the first exec of the restored REPL. */
  readonly notice: string | undefined;
}

interface Snapshot {
  readonly result_text: string;
  readonly error_text: string;
  readonly notice?: string;
}

async function connect(bayma: BaymaEndpoint): Promise<Client> {
  const client = new Client({ name: "alasio-gate-11", version: "1.0.0" });
  // The transport is one; the SDK declares its sessionId `string | undefined` where Transport has it optional.
  await client.connect(new StreamableHTTPClientTransport(new URL(bayma.url), { requestInit: { headers: bayma.headers } }) as Transport);
  return client;
}

async function call<T>(client: Client, name: string, args: Record<string, unknown>): Promise<T> {
  const result = (await client.callTool({ name, arguments: args }, undefined, { timeout: 300_000 })) as CallToolResult;
  if (result.isError) throw new Error(`${name}: ${JSON.stringify(result.content)}`);
  return result.structuredContent as T;
}

const exec = (client: Client, replId: string, code: string) => call<Snapshot>(client, "exec", { session_id: replId, code, yield_time_ms: 120_000 });

/** A new session's bayma, brought up as a turn brings it up. */
const create = (profile: SessionsProfile) => Effect.gen(function*() {
  const sessions = yield* SessionSandboxes;
  const volumeId = newVolumeId();
  yield* sessions.volumes.create(volumeId, "none");
  const { bayma } = yield* sessions.ensureSession(volumeId);
  return yield* Effect.promise(async (): Promise<Created> => {
    const client = await connect(bayma);
    try {
      const { session } = await call<{ session: { session_id: string } }>(client, "session.create", { runtime: "bun", title: "gate-11", cwd: profile.workspaceDir });
      const made = await exec(client, session.session_id, [
        'const fs = require("node:fs")',
        "globalThis.marker = 41",
        'fs.writeFileSync("/tmp/gate-11-written", "written to /tmp")',
        'globalThis.held = fs.openSync("/tmp/gate-11-held", "w+")',
        'fs.writeSync(globalThis.held, "held open in /tmp")',
        '"ready"',
      ].join("\n"));
      return { volumeId, replId: session.session_id, said: made.result_text || made.error_text };
    } finally {
      await client.close();
    }
  });
});

/** The REPL's state, through the session's Service and token, as a new client: alasio's ensure is left out, which would repair the pod the gate restored. */
const read = (profile: SessionsProfile, volumeId: string, replId: string) => Effect.gen(function*() {
  const sandboxes = yield* makeSandboxes({ namespace: profile.namespace, port: profile.port });
  const token = yield* sandboxes.token(volumeId);
  const bayma: BaymaEndpoint = {
    url: `http://${volumeId}.${profile.namespace}.svc.cluster.local:${profile.port}/mcp`,
    headers: { authorization: `Bearer ${token}` },
  };
  return yield* Effect.promise(async (): Promise<Read> => {
    const client = await connect(bayma);
    try {
      await call(client, "session.acquire_controller", { session_id: replId });
      const marker = await exec(client, replId, "marker + 1");
      const tmpFile = await exec(client, replId, 'require("node:fs").readFileSync("/tmp/gate-11-written", "utf8")');
      const heldFile = await exec(client, replId, [
        "const buffer = Buffer.alloc(64)",
        "const read = require(\"node:fs\").readSync(globalThis.held, buffer, 0, 64, 0)",
        'buffer.subarray(0, read).toString("utf8")',
      ].join("\n"));
      const text = (snapshot: Snapshot) => snapshot.result_text || `error: ${snapshot.error_text}`;
      return { marker: text(marker), tmpFile: text(tmpFile), heldFile: text(heldFile), notice: marker.notice };
    } finally {
      await client.close();
    }
  });
});

/**
 * `profile`, its pods without init containers, the egress gate's either, never
 * restarted, and with their emptyDir /tmp a tmpfs of gVisor's own, which a checkpoint
 * holds, by gVisor's mount hints.
 */
function bare(profile: SessionsProfile): SessionsProfile {
  const podTemplate = structuredClone(profile.podTemplate ?? {});
  podTemplate.metadata = {
    ...podTemplate.metadata,
    annotations: {
      ...podTemplate.metadata?.annotations,
      "dev.gvisor.spec.mount.tmp.share": "container",
      "dev.gvisor.spec.mount.tmp.type": "tmpfs",
      "dev.gvisor.spec.mount.tmp.options": "rw",
    },
  };
  podTemplate.spec = { ...podTemplate.spec, initContainers: [], restartPolicy: "Never", containers: podTemplate.spec?.containers ?? [] };
  return { ...profile, egressGate: false, podTemplate };
}

const [mode, volumeId, replId] = process.argv.slice(2);
const seen = await Effect.runPromise(Effect.gen(function*() {
  const { sessions } = yield* loadKubeTemplates;
  if (!sessions) return yield* Effect.die(new Error("the installation gives no sessions template"));
  if (mode === "create") {
    const profile = volumeId === "bare" ? bare(sessions) : sessions;
    return yield* create(profile).pipe(Effect.provide(SessionSandboxes.layer({ profile, stateDir: "/tmp/gate-11", env: {} })));
  }
  if (mode === "read" && volumeId && replId) return yield* read(sessions, volumeId, replId);
  return yield* Effect.die(new Error("usage: session.ts create [bare] | read <volumeId> <replId>"));
}).pipe(Effect.scoped, Effect.provide(KubeClient.layer)));
console.log(JSON.stringify(seen));
