/**
 * Run inside alasio's pod, by node there (inAlasio in test/e2e/harness.ts), for 12's gates on
 * k3s (./chain.test.ts): a session made and brought up through alasio's own session
 * filesystems, a Bun REPL in it, an exec in it that runs until the gate lets it end, and what
 * the REPL holds read back after the gate restored the session's pod. Prints one JSON line.
 *
 *   session.ts create                        a session without internet, a REPL holding 41
 *                                            and a file of the workspace open; `Created`
 *   session.ts set <volumeId> <replId> <n>   the REPL's `step` set to n
 *   session.ts start <volumeId> <replId>     an exec writing memory until /workspace/gate-12-go
 *                                            exists, then answering 42; `Started`
 *   session.ts finish <volumeId> <replId> <execId>
 *                                            that exec, followed until it ends; `Seen`
 *   session.ts read <volumeId> <replId>      `marker + 1` and `step`; `Read`
 *
 * After `create`, the REPL is reached through the session's Service and token, as a new
 * client that takes it over: alasio's ensure is left out, which would repair a pod the gate
 * restored. alasio's modules are imported by their paths in the repository, which inAlasio
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
}

export interface Started {
  readonly execId: string;
}

/** What an exec ended with, and what bayma told it of how its session came back. */
export interface Seen {
  readonly result: string;
  readonly notice: string | null;
}

export interface Read {
  /** `marker + 1`: 42 when the REPL came back whole. */
  readonly marker: string;
  readonly step: string;
  /** What bayma told the first exec of the restored REPL. */
  readonly notice: string | null;
}

/** The file the exec started by `start` waits for. */
const GO = "/workspace/gate-12-go";

interface Snapshot {
  readonly exec_id: string;
  readonly done: boolean;
  readonly next_seq: number;
  readonly result_text: string;
  readonly error_text: string;
  readonly notice?: string;
}

async function connect(bayma: BaymaEndpoint): Promise<Client> {
  const client = new Client({ name: "alasio-gate-12", version: "1.0.0" });
  // The transport is one; the SDK declares its sessionId `string | undefined` where Transport has it optional.
  await client.connect(new StreamableHTTPClientTransport(new URL(bayma.url), { requestInit: { headers: bayma.headers } }) as Transport);
  return client;
}

async function call<T>(client: Client, name: string, args: Record<string, unknown>): Promise<T> {
  const result = (await client.callTool({ name, arguments: args }, undefined, { timeout: 600_000 })) as CallToolResult;
  if (result.isError) throw new Error(`${name}: ${JSON.stringify(result.content)}`);
  return result.structuredContent as T;
}

/** An exec followed until it ends: its answer, or its error. */
async function settled(client: Client, replId: string, snapshot: Snapshot): Promise<Snapshot> {
  let current = snapshot;
  while (!current.done) {
    current = await call<Snapshot>(client, "wait", { session_id: replId, exec_id: current.exec_id, from_seq: current.next_seq, yield_time_ms: 60_000 });
  }
  return current;
}

const exec = async (client: Client, replId: string, code: string) =>
  settled(client, replId, await call<Snapshot>(client, "exec", { session_id: replId, code, yield_time_ms: 60_000 }));

const answer = (snapshot: Snapshot) => snapshot.result_text.trim() || `error: ${snapshot.error_text}`;

/** A new session's bayma, brought up as a turn brings it up. */
const create = (profile: SessionsProfile) => Effect.gen(function*() {
  const sessions = yield* SessionSandboxes;
  const volumeId = newVolumeId();
  yield* sessions.volumes.create(volumeId, "none");
  const { bayma } = yield* sessions.ensureSession(volumeId);
  return yield* Effect.promise(async (): Promise<Created> => {
    const client = await connect(bayma);
    try {
      const { session } = await call<{ session: { session_id: string } }>(client, "session.create", { runtime: "bun", title: "gate-12", cwd: profile.workspaceDir });
      const made = await exec(client, session.session_id, [
        'const fs = require("node:fs")',
        "globalThis.marker = 41",
        "globalThis.step = 0",
        // Held open, so that a restore validates it is the file it was.
        'globalThis.held = fs.openSync("/workspace/gate-12-held", "w+")',
        'fs.writeSync(globalThis.held, "held open")',
        '"ready"',
      ].join("\n"));
      if (answer(made) !== '"ready"') throw new Error(`the REPL said ${answer(made)}`);
      return { volumeId, replId: session.session_id };
    } finally {
      await client.close();
    }
  });
});

/** The session's REPL, as a new client that takes it over, given to `use`. */
const inRepl = <A>(profile: SessionsProfile, volumeId: string, replId: string, use: (client: Client) => Promise<A>) => Effect.gen(function*() {
  const sandboxes = yield* makeSandboxes({ namespace: profile.namespace, port: profile.port });
  const token = yield* sandboxes.token(volumeId);
  const bayma: BaymaEndpoint = {
    url: `http://${volumeId}.${profile.namespace}.svc.cluster.local:${profile.port}/mcp`,
    headers: { authorization: `Bearer ${token}` },
  };
  return yield* Effect.promise(async () => {
    const client = await connect(bayma);
    try {
      await call(client, "session.acquire_controller", { session_id: replId });
      return await use(client);
    } finally {
      await client.close();
    }
  });
});

const [mode, volumeId, replId, argument] = process.argv.slice(2);
const seen = await Effect.runPromise(Effect.gen(function*() {
  const { sessions } = yield* loadKubeTemplates;
  if (!sessions) return yield* Effect.die(new Error("the installation gives no sessions template"));
  if (mode === "create") return yield* create(sessions).pipe(Effect.provide(SessionSandboxes.layer({ profile: sessions, stateDir: "/tmp/gate-12", env: {} })));
  if (!volumeId || !replId) return yield* Effect.die(new Error("usage: session.ts create | set|start|finish|read <volumeId> <replId> [<n>|<execId>]"));
  if (mode === "set" && argument) {
    return yield* inRepl(sessions, volumeId, replId, async (client) => ({ result: answer(await exec(client, replId, `globalThis.step = ${Number(argument)}`)) }));
  }
  if (mode === "start") {
    return yield* inRepl(sessions, volumeId, replId, async (client): Promise<Started> => {
      const started = await call<Snapshot>(client, "exec", {
        session_id: replId,
        code: `while (!require("node:fs").existsSync(${JSON.stringify(GO)})) { globalThis.churn = new Uint8Array(1 << 20).fill(Date.now() & 255); await Bun.sleep(50); } 42`,
        yield_time_ms: 0,
      });
      return { execId: started.exec_id };
    });
  }
  if (mode === "finish" && argument) {
    return yield* inRepl(sessions, volumeId, replId, async (client): Promise<Seen> => {
      const done = await settled(client, replId, await call<Snapshot>(client, "wait", { session_id: replId, exec_id: argument, yield_time_ms: 60_000 }));
      return { result: answer(done), notice: done.notice ?? null };
    });
  }
  if (mode === "read") {
    return yield* inRepl(sessions, volumeId, replId, async (client): Promise<Read> => {
      const marker = await exec(client, replId, "marker + 1");
      const step = await exec(client, replId, "step");
      return { marker: answer(marker), step: answer(step), notice: marker.notice ?? null };
    });
  }
  return yield* Effect.die(new Error(`no such mode: ${mode}`));
}).pipe(Effect.scoped, Effect.provide(KubeClient.layer)));
console.log(JSON.stringify(seen));
