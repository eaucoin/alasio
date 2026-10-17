/**
 * alasio, end to end, as its operator runs it: installed and started by its command line
 * from its npm package (./harness.ts), driven through the Telegram stand-in as its
 * operator drives it, its sessions' confinement checked from inside them, its own code
 * paths run in its pod with its ServiceAccount, its telemetry looked for in its lake and
 * where the installation's goes, its workspaces' JuiceFS put through file operations, crashes and a
 * restore (./workspace-storage.ts), its sessions released idle into whole-sandbox snapshots and
 * restored from them, its Neon through crashes and losses (neon/test/stack.ts), an
 * installation of the release before upgraded to it with what it holds, by the cut-over the
 * box runs (upgrades/), and removed, with all it keeps, by its command line at the end.
 *
 *   ALASIO_E2E_AGENTS=2 npm run test:e2e
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { request as httpRequest } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, afterEach, before, describe, test } from "node:test";

import type { InlineKeyboardButton } from "@grammyjs/types";
import type { CoreV1Event, KubernetesObject, V1ConfigMap, V1Deployment, V1Job, V1Node, V1PersistentVolume, V1Pod, V1Service, V1StatefulSet, V1ValidatingAdmissionPolicy, V1ValidatingAdmissionPolicyBinding, V2HorizontalPodAutoscaler } from "@kubernetes/client-node";
import pg from "pg";

import { memoryBytes } from "../../cli/src/capacity.ts";
import { selectorOf } from "../../cli/src/kube/rollout.ts";
import { componentName, NAMESPACE, neonName, RELEASE, selectorLabels } from "../../cli/src/manifests/common.ts";
import { PAGESERVERS_NAME, pageserverName } from "../../cli/src/manifests/neon.ts";
import { computeId, MAIN, replicaId } from "../../neon/control/branches.ts";
import { neonStack } from "../../neon/test/stack.ts";
import { SNAPSHOT_ANNOTATION } from "../../src/kube/sandbox-shape.ts";
import type { Sandbox } from "../../src/kube/sandboxes.ts";
import type { Manifest } from "../../src/node-agent/agent.ts";
import { gvisorRuntimeClass, NODE_AGENT_PORT, NODE_AGENT_TOKEN_VARIABLE, SNAPSHOT_CACHE_DIR, UNSYNCED_DOWNLOAD_BYTES } from "../../src/node-agent/names.ts";
import type { NetMode } from "../../src/sandbox/index.ts";
import { ANSWER } from "./codex-stand-in.ts";
import type { FolderBaymaSeen } from "./folder-bayma.ts";
import {
  AGENTS,
  alasio,
  BIG_REPL_MEBIBYTES,
  alasioOk,
  alasioOkWith,
  alasioRunning,
  CLUSTER,
  cutOver,
  dumpClusterState,
  type Forward,
  HOST_PROFILE,
  HOST_USER,
  inAlasio,
  inAlasioContainer,
  inNodeAgent,
  inSession,
  inShard,
  KEEP,
  kube,
  lakeQuery,
  nodeAgents,
  onNode,
  OTHER_OPERATOR,
  paths,
  PREVIOUS,
  ref,
  releaseAlasio,
  RELEASE_AFTER_SECONDS,
  SESSIONS,
  setUp,
  tcp,
  TARGET,
  tearDown,
} from "./harness.ts";
import type { ListedExport } from "./otlp-sink.ts";
import type { ReplFilled, ReplSeen, ReplSet, ReplSummed } from "./session-repl.ts";
import type { RoundtripSeen } from "./session-roundtrip.ts";
import type { Corrupted, Released } from "./snapshot-bucket.ts";
import { OTLP, STAND_INS, type StandIn, TELEGRAM, TELEGRAM_BRANCH, urlOf } from "./stand-ins.ts";
import type { CallsListing, ControlCallback, ControlMessage, RecordedCall } from "./telegram-stub.ts";
import { workspaceStorage } from "./workspace-storage.ts";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** The host profile's namespace, where folder workspaces' bayma run. */
const HOST_NAMESPACE = "alasio-host";

// What the cluster was doing is said as something of the run fails, while the cluster is
// as it failed it: before the tests that follow change it, or remove it.
before(async () => {
  try {
    await setUp();
  } catch (error) {
    await dumpClusterState();
    throw error;
  }
}, { timeout: 120 * 60_000 });

afterEach(async (t) => {
  // Node says on a test's context whether it passed, once it has run.
  if ("passed" in t && t.passed === false) await dumpClusterState();
});

after(async () => {
  await tearDown();
}, { timeout: 30 * 60_000 });

/** A local port forwarded to the stand-in's Service, once the stand-in answers there. */
async function forward(standIn: StandIn): Promise<Forward> {
  const forwarded = await kube.forward(STAND_INS, standIn.service, standIn.port);
  for (let attempt = 0; attempt < 60; attempt++) {
    if (await fetch(`${forwarded.base}${standIn.ready}`).then(() => true, () => false)) return forwarded;
    await sleep(500);
  }
  forwarded.close();
  throw new Error(`the ${standIn.name} stand-in did not answer`);
}

/**
 * `method` of `url`, through a forward, with `body` as JSON, its answer's JSON: on a
 * connection of its own, and so a forward of its own, which no request after it reuses, as
 * a kept-alive connection that the stand-in, or the forward through the API server, ends
 * while idle may be closing as the next request is sent on it. One that closes under a
 * read anyway is asked again, up to three times; a say or a press, which the stand-in may
 * have taken, is not.
 */
async function throughForward(method: "GET" | "POST", url: string, body?: unknown): Promise<unknown> {
  const once = () =>
    new Promise<unknown>((resolve, reject) => {
      const sent = body === undefined ? undefined : JSON.stringify(body);
      const request = httpRequest(url, { method, agent: false, headers: sent === undefined ? {} : { "content-type": "application/json" } }, (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk: Buffer) => chunks.push(chunk));
        response.on("end", () => {
          try {
            resolve(JSON.parse(Buffer.concat(chunks).toString("utf8")));
          } catch (error) {
            reject(error);
          }
        });
        response.on("error", reject);
      });
      request.on("error", reject);
      request.end(sent);
    });
  for (let attempt = 1; ; attempt++) {
    try {
      return await once();
    } catch (error) {
      const closed = (error as NodeJS.ErrnoException).code === "ECONNRESET" || (error as Error).message === "socket hang up";
      if (method !== "GET" || attempt >= 3 || !closed) throw error;
      await sleep(500);
    }
  }
}

/** The operator, through the Telegram stand-in: says things, presses buttons, reads replies. */
function operator(base: string) {
  let seen = 0;
  const post = (path: string, body: ControlMessage | ControlCallback) => throughForward("POST", `${base}${path}`, body);
  const calls = async () => {
    // The stand-in answers what it was called with.
    const listed = (await throughForward("GET", `${base}/control/calls?since=${seen}`)) as CallsListing;
    seen = listed.total;
    return listed.calls;
  };
  const buttons = (call: RecordedCall): InlineKeyboardButton[] => {
    const markup = call.payload.reply_markup;
    return (markup && "inline_keyboard" in markup ? markup.inline_keyboard : []).flat();
  };
  return {
    buttons,
    /** Says `text` as the operator, or as `user`, another the bot is for, each in their own chat. */
    say: (text: string, user?: number) => post("/control/message", { text, ...(user === undefined ? {} : { chatId: user, userId: user }) }),
    press: (data: string | undefined) => post("/control/callback", { data }),
    calls,
    /** What alasio sends until `predicate` holds for one, which is returned. */
    async waitFor(predicate: (call: RecordedCall) => boolean, timeoutMs = 300_000): Promise<RecordedCall> {
      const deadline = Date.now() + timeoutMs;
      const got: RecordedCall[] = [];
      while (Date.now() < deadline) {
        got.push(...(await calls()));
        const hit = got.find(predicate);
        if (hit) return hit;
        await sleep(500);
      }
      throw new Error(`alasio sent nothing that matched; it sent ${JSON.stringify(got.map((call) => [call.method, call.payload.text?.slice(0, 120)]))}`);
    },
    /** Presses the button labelled like `label` in what alasio sends next. */
    async choose(label: RegExp) {
      const call = await this.waitFor((candidate) => buttons(candidate).some((button) => label.test(button.text)));
      const button = buttons(call).find((candidate) => label.test(candidate.text));
      await this.press(button && "callback_data" in button ? button.callback_data : undefined);
    },
  };
}

/**
 * What alasio keeps upgraded from the release before, a scenario at a time: its seed, run on
 * an installation of that release (./harness.ts), and its check, run once the installation is
 * upgraded to this checkout by the cut-over the box runs (cutOver). A change an installation
 * is upgraded to adds the scenario that proves it keeps what that installation holds.
 */
interface Scenario {
  /** What the seed makes, on the release before. */
  readonly seeds: string;
  readonly seed: () => Promise<void>;
  /** What the check proves is kept, upgraded. */
  readonly keeps: string;
  readonly check: () => Promise<void>;
}

/** Whether `call` is the answer of a turn on Codex, the stand-in's, in the chat `chat`, the operator's unless given. */
const answered = (call: RecordedCall, chat?: number) =>
  call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER && (chat === undefined || Number(call.payload.chat_id) === chat);

if (inShard("upgrade")) {
  describe(`alasio upgraded from ${PREVIOUS}`, () => {
    let branchTelegram: Forward | undefined;

    before(async () => {
      telegram = await forward(TELEGRAM);
      branchTelegram = await forward(TELEGRAM_BRANCH);
      tg = operator(telegram.base);
      await tg.waitFor((call) => call.method === "setMyCommands", 300_000).catch(() => {});
    });

    after(() => {
      telegram?.close();
      branchTelegram?.close();
    });

    /** Whether the Deployment `name` of main's runs its replicas, ready. */
    const runs = async (name: string) => {
      const deployment = await kube.get<V1Deployment>(ref("Deployment", name, NAMESPACE));
      return (deployment?.spec?.replicas ?? 0) > 0 && deployment?.status?.readyReplicas === deployment?.spec?.replicas;
    };

    let volumeId: string;
    /** The folder mounted in the other chat, and what it holds. */
    let folder: string;
    let messages: number;
    /** What Neon's storage held on PREVIOUS: the pageserver's claim, by its uid, the tenant's shards and the node each is attached to, and main's timeline. */
    let held: string[];
    /** Runs `sql` on the storage controller's database, neon-control's record among it: what psql printed. */
    const onRecord = async (sql: string) =>
      (await kube.execOk(NAMESPACE, `${neonName("controller-db")}-0`, ["psql", "-U", "storage_controller", "-d", "storage_controller", "-Atc", sql], { container: "postgres" })).trim();
    const NODES = "select node_id, listen_http_addr, listen_http_port, listen_pg_addr, listen_pg_port, availability_zone_id from nodes order by node_id";
    /** What Neon's storage holds: the pageserver's claim, by its uid, the tenant's shards and the node each is attached to, and main's timeline. */
    const neonHeld = async () => [
      (await kube.get(ref("PersistentVolumeClaim", `data-${neonName("pageserver")}-0`, NAMESPACE)))?.metadata?.uid ?? "",
      await onRecord("select tenant_id, shard_number, generation_pageserver from tenant_shards order by shard_number"),
      await onRecord("select branch->>'timelineId' from neon_control.branches where name = 'main'"),
    ];
    /** The pageservers' StatefulSet, and its pod, by their uids, as the cut-over finds them. */
    const pageservers = async () => [
      (await kube.get(ref("StatefulSet", neonName("pageserver"), NAMESPACE)))?.metadata?.uid,
      (await kube.get(ref("Pod", `${neonName("pageserver")}-0`, NAMESPACE)))?.metadata?.uid,
    ];
    const scenarios: readonly Scenario[] = [
      {
        seeds: "main at work, in a session workspace and in a folder, a file in each",
        seed: async () => {
          volumeId = await workInMain();
          await tg.say("hello", FOLDER_CHAT);
          await tg.waitFor((call) => answered(call, FOLDER_CHAT));
          folder = await onDatabase(NAMESPACE, `select working_directory from state.conversations where id = 'telegram:${FOLDER_CHAT}'`);
          assert.ok(folder.startsWith(paths().home), `the folder chat mounts ${folder}`);
          await onNode("", `printf %s 'written by main' >'${folder}/proof' && chown ${HOST_USER}:${HOST_USER} '${folder}/proof'`);
          messages = Number(await onDatabase(NAMESPACE, "select count(*) from state.messages"));
        },
        keeps: "main's conversations go on where they were mounted, in the session workspace and in the folder, each file as it was",
        check: async () => {
          assert.match(await alasioOk("status"), new RegExp(`^ {2}Deployment ${NAMESPACE}/alasio-neon-compute: awake`, "mu"));
          assert.equal(await onDatabase(NAMESPACE, "select working_directory from state.conversations where id = 'telegram:1001'"), `sessionfs:${volumeId}`);
          assert.equal(await onDatabase(NAMESPACE, `select working_directory from state.conversations where id = 'telegram:${FOLDER_CHAT}'`), folder);
          await tg.say("again");
          await tg.waitFor((call) => answered(call));
          assert.equal(await inSession(volumeId, 'process.stdout.write(require("fs").readFileSync("/workspace/proof", "utf8"))'), "written by main");
          await tg.say("again", FOLDER_CHAT);
          await tg.waitFor((call) => answered(call, FOLDER_CHAT));
          assert.equal(readFileSync(`${folder}/proof`, "utf8"), "written by main");
          assert.ok(Number(await onDatabase(NAMESPACE, "select count(*) from state.messages")) > messages);
        },
      },
      {
        seeds: "Neon's tenant on the one pageserver, holding main's work, node 1 registered as the setup named it, its StatefulSet starting its pods in order",
        seed: async () => {
          assert.equal(await onRecord(NODES), "1|alasio-neon-pageserver|9898|alasio-neon-pageserver|6400|az-pageserver");
          assert.equal((await kube.get<V1StatefulSet>(ref("StatefulSet", neonName("pageserver"), NAMESPACE)))?.spec?.podManagementPolicy, "OrderedReady");
          held = await neonHeld();
        },
        keeps: "the pageserver on its claim, holding the tenant and its timeline on node 1, registered anew at its pod's own Service, its StatefulSet made anew starting its pods at once, " +
          "its one shard's layout recorded, and the lake loading main's turns through main's read replica",
        check: async () => {
          assert.deepEqual(await neonHeld(), held);
          assert.equal(await onRecord(NODES), `1|${pageserverName(0)}|9898|${pageserverName(0)}|6400|az-pageserver`);
          const statefulSet = await kube.get<V1StatefulSet>(ref("StatefulSet", neonName("pageserver"), NAMESPACE));
          assert.equal(statefulSet?.spec?.podManagementPolicy, "Parallel");
          // Governed by a headless Service of this release's, which pod 0, made anew, is named under.
          assert.equal(statefulSet?.spec?.serviceName, PAGESERVERS_NAME);
          assert.equal((await kube.get<V1Service>(ref("Service", PAGESERVERS_NAME, NAMESPACE)))?.spec?.clusterIP, "None");
          assert.equal((await kube.get<V1Pod>(ref("Pod", pageserverName(0), NAMESPACE)))?.spec?.subdomain, PAGESERVERS_NAME);
          const said = await alasioOk("status");
          assert.match(said, /^Neon: 1 shard on 1 pageserver$/mu);
          assert.match(said, new RegExp(`^ {2}pageserver 1 at ${pageserverName(0)}\\.${NAMESPACE}\\.svc\\.cluster\\.local:6400: Active, 1 shard$`, "mu"));
          // Recorded by neon-control as it starts, and given to main's compute as it fetches its
          // spec, or, as one that ran on was given 7.0.0's, by the round of the replica that leads.
          assert.equal(await onRecord("select count(*) from neon_control.layout"), "1");
          await until("neon-control recorded the layout it gave main's compute", async () =>
            (await onRecord("select id from neon_control.compute_layouts")).split("\n").includes(computeId(MAIN)) || undefined, 180_000);
          const lake = (await kube.get<V1Deployment>(ref("Deployment", `${RELEASE}-lake`, NAMESPACE)))?.spec?.template.spec?.containers.find(({ name }) => name === "lake");
          assert.equal(lake?.env?.find(({ name }) => name === "LAKE_SOURCE_ENDPOINT")?.value, replicaId(MAIN));
          const since = Date.now();
          await tg.say("through the replica");
          await tg.waitFor((call) => answered(call));
          await untilInLake("main's turn, loaded through its read replica", `select count(*) as n from codex.turns where epoch_ms(completed_at) >= ${since}`, 600_000);
        },
      },
    ];

    for (const { seeds, seed } of scenarios) test(`on ${PREVIOUS}: ${seeds}`, seed);

    // A branch environment another release made does not run on what an upgrade changes,
    // so the cut-over refuses while one exists, as its first checks find it, before the
    // pageservers' StatefulSet is deleted.
    test(`on ${PREVIOUS}: a branch environment, which the cut-over refuses to go past, before it changes anything, until it is deleted`, { timeout: 60 * 60_000 }, async () => {
      await createBranch(branchTelegram?.base);
      const [set, pod] = await pageservers();
      const [claim] = await neonHeld();
      const refused = await cutOver();
      assert.notEqual(refused.code, 0, refused.stdout);
      assert.match(refused.stderr, new RegExp(`FAILED: alasio upgrades main alone[^\\n]*\\(alasio branch delete ${BRANCH}\\)`, "u"));
      assert.doesNotMatch(refused.stderr, /deleting the pageservers' StatefulSet/u);
      assert.deepEqual([...await pageservers(), (await neonHeld())[0]], [set, pod, claim]);
      assert.equal((await kube.get<V1StatefulSet>(ref("StatefulSet", neonName("pageserver"), NAMESPACE)))?.spec?.podManagementPolicy, "OrderedReady");
      assert.ok(await runs("alasio-neon-control"), "neon-control runs as it did");
      assert.ok(await runs(RELEASE), "alasio runs as it did");
      await alasioOk("branch", "delete", BRANCH);
    });

    // This release cannot be applied over PREVIOUS's pageservers' StatefulSet, as the cut-over
    // deletes it first: a plain upgrade refuses before anything changes.
    test(`on ${PREVIOUS}: a plain upgrade, refused before it changes anything, as the cut-over is to make the pageservers' StatefulSet anew`, async () => {
      const before = await pageservers();
      const alasioGeneration = async () => (await kube.get(ref("Deployment", RELEASE, NAMESPACE)))?.metadata?.generation;
      const generation = await alasioGeneration();
      const refused = await releaseAlasio("upgrade");
      assert.notEqual(refused.code, 0, refused.stdout);
      assert.ok(refused.stderr.includes(`StatefulSet ${NAMESPACE}/${neonName("pageserver")}`) && refused.stderr.includes(`upgrades/from-${PREVIOUS}.sh`), refused.stderr);
      assert.deepEqual([await pageservers(), await alasioGeneration()], [before, generation]);
    });

    test(`the cut-over from ${PREVIOUS}, failing once each step before the upgrade has run, its pageservers' StatefulSet deleted, makes it again over its pod as it was, ` +
      "or says last that it could not, and makes it run again", { timeout: 60 * 60_000 }, async () => {
      const [, pod] = await pageservers();
      // The cluster refusing to make it again, as the restore's failure the wrapper of the
      // box's cut-over is to report as such, from the line the run ends with.
      const held = { apiVersion: "admissionregistration.k8s.io/v1", metadata: { name: "e2e-pageservers-held" } };
      const policy: V1ValidatingAdmissionPolicy = {
        ...held,
        kind: "ValidatingAdmissionPolicy",
        spec: {
          failurePolicy: "Fail",
          matchConstraints: { resourceRules: [{ apiGroups: ["apps"], apiVersions: ["v1"], operations: ["CREATE"], resources: ["statefulsets"] }] },
          validations: [{ expression: `object.metadata.name != '${neonName("pageserver")}'`, message: "held by the end-to-end run" }],
        },
      };
      const binding: V1ValidatingAdmissionPolicyBinding = { ...held, kind: "ValidatingAdmissionPolicyBinding", spec: { policyName: held.metadata.name, validationActions: ["Deny"] } };
      await kube.apply(policy);
      await kube.apply(binding);
      const stuck = await cutOver({ failAt: "before-the-point-of-no-return" });
      await kube.remove(ref("ValidatingAdmissionPolicyBinding", held.metadata.name));
      await kube.remove(ref("ValidatingAdmissionPolicy", held.metadata.name));
      assert.notEqual(stuck.code, 0, stuck.stdout);
      assert.match(stuck.stderr, /FAILED: the pageservers' StatefulSet is deleted, its pod running unowned, and could not be made again[^\n]*\n$/u);
      assert.equal(await kube.get(ref("StatefulSet", neonName("pageserver"), NAMESPACE)), null);
      const failed = await cutOver({ failAt: "before-the-point-of-no-return" });
      assert.notEqual(failed.code, 0, failed.stdout);
      assert.match(failed.stderr, /FAILED: a failure injected before the point of no return[^]*making the pageservers' StatefulSet again/u);
      assert.doesNotMatch(failed.stderr, /could not be made again/u);
      const [set, podAfter] = await pageservers();
      assert.ok(set);
      assert.equal(podAfter, pod);
      assert.equal((await kube.get<V1StatefulSet>(ref("StatefulSet", neonName("pageserver"), NAMESPACE)))?.spec?.podManagementPolicy, "OrderedReady");
      await kube.awaitReady([ref("Deployment", "alasio-neon-control", NAMESPACE), ref("Deployment", RELEASE, NAMESPACE)], "10 minutes");
      await tg.say("between the cut-overs");
      await tg.waitFor((call) => answered(call));
    });

    const controllerReplicas = async () => (await kube.get<V1Deployment>(ref("Deployment", neonName("storage-controller"), NAMESPACE)))?.spec?.replicas;
    let controllerRan: number | undefined;

    test(`the cut-over from ${PREVIOUS}, failing past the point of no return with the storage controller stopped, leaves it stopped and node 1 as it was`, { timeout: 60 * 60_000 }, async () => {
      controllerRan = await controllerReplicas();
      assert.ok(controllerRan);
      const failed = await cutOver({ failAt: "after-the-storage-controller-stopped" });
      assert.notEqual(failed.code, 0, failed.stdout);
      assert.match(failed.stderr, /registering node 1 anew at [^]*FAILED: a failure injected once the storage controller was stopped/u);
      assert.equal(await controllerReplicas(), 0);
      assert.equal(await onRecord(NODES), "1|alasio-neon-pageserver|9898|alasio-neon-pageserver|6400|az-pageserver");
      // What it registered unread, as with its database stopped, is no registration either
      // way: the run fails there, the upgrade not run.
      const database = ref("StatefulSet", neonName("controller-db"), NAMESPACE);
      await kube.patch(database, { spec: { replicas: 0 } });
      await until("the storage controller's database stopped", async () => (await kube.get(ref("Pod", `${neonName("controller-db")}-0`, NAMESPACE))) === null || undefined, 300_000);
      const unread = await cutOver();
      await kube.patch(database, { spec: { replicas: 1 } });
      await kube.awaitReady([database], "10 minutes");
      assert.notEqual(unread.code, 0, unread.stdout);
      assert.match(unread.stderr, /FAILED: the storage controller's database did not answer/u);
      assert.doesNotMatch(unread.stderr, /registering node 1 anew|upgrading, from the cut-over/u);
    });

    test(`the cut-over from ${PREVIOUS}, run again, starts the storage controller with the replicas it ran, and, failing once node 1 is registered anew, leaves ${PREVIOUS} running on the pageserver at its new name`, { timeout: 60 * 60_000 }, async () => {
      const failed = await cutOver({ failAt: "after-the-pageserver-registered-anew" });
      assert.notEqual(failed.code, 0, failed.stdout);
      assert.match(failed.stderr, /registering node 1 anew at [^]*starting the storage controller again[^]*FAILED: a failure injected once node 1 was registered anew/u);
      assert.equal(await controllerReplicas(), controllerRan);
      assert.equal(await onRecord(NODES), `1|${pageserverName(0)}|9898|${pageserverName(0)}|6400|az-pageserver`);
      assert.equal(await kube.get(ref("StatefulSet", neonName("pageserver"), NAMESPACE)), null);
      await tg.say("between the cut-overs, past their point of no return");
      await tg.waitFor((call) => answered(call));
    });

    /** Main's session's REPL, live as the session is released, and the snapshot it is released into on PREVIOUS. */
    let releasedRepl: string;
    let releasedAs: string;
    /** The keys of what PREVIOUS keeps of the sessions' snapshots in the bucket: each under a snapshot's id, to the second. */
    const keptByPrevious = async () => {
      const [node] = (await nodeAgents()).keys();
      assert.ok(node, "no node agent runs ready");
      return (await inNodeAgent<readonly string[]>(node, "./bucket-keys.ts", `${SESSIONS}/`)).filter((key) => /^[^/]+\/[^/]+\/[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}\//u.test(key));
    };
    let keptBefore: readonly string[];

    test(`on ${PREVIOUS}: main's session released into a snapshot of its layout, a REPL live in it, which the cut-over moves`, { timeout: 30 * 60_000 }, async () => {
      ({ sessionId: releasedRepl } = await repl.set(volumeId, `released by ${volumeId}`));
      await untilReleased(volumeId, Date.now());
      releasedAs = (await kube.get<Sandbox>(ref("Sandbox", volumeId, SESSIONS)))?.metadata.annotations?.[SNAPSHOT_ANNOTATION] ?? "";
      // An id of PREVIOUS's, to the second.
      assert.match(releasedAs, /^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}$/u);
      keptBefore = await keptByPrevious();
      assert.ok(keptBefore.includes(`${SESSIONS}/${volumeId}/${releasedAs}/manifest.json`), keptBefore.join(", "));
    });

    test(`the cut-over from ${PREVIOUS}, upgrades/from-${PREVIOUS}.sh, as the box runs it, run again after those failures, through the upgrade, failing midway through the deletion of what ${PREVIOUS} kept of the snapshots`, { timeout: 60 * 60_000 }, async () => {
      // Each second it runs: whether main's compute, awake, has an address to read pages at,
      // PREVIOUS's pageservers' Service or the one neon-control gave it; whether this release's
      // pageservers' StatefulSet is applied, past which the upgrade's prune would delete
      // PREVIOUS's Service, which the cut-over keeps until neon-control has given pod 0's; and
      // whether pod 0 is ready.
      const samples: { readonly at: number; readonly asleep: boolean; readonly service: boolean; readonly given: boolean; readonly applied: boolean; readonly ready: boolean }[] = [];
      let running = true;
      const sampling = (async () => {
        while (running) {
          const at = Date.now();
          const asleep = await kube.get<V1Deployment>(ref("Deployment", neonName("compute"), NAMESPACE)).then((compute) => compute?.spec?.replicas === 0, () => false);
          const service = await kube.get(ref("Service", neonName("pageserver"), NAMESPACE)).then((found) => found !== null, () => true);
          const given = await onRecord(`select layout from neon_control.compute_layouts where id = '${computeId(MAIN)}'`).then((layout) => layout.includes(pageserverName(0)), () => false);
          const applied = await kube.get(ref("StatefulSet", neonName("pageserver"), NAMESPACE)).then((found) => found !== null, () => false);
          const pod = await kube.get<V1Pod>(ref("Pod", pageserverName(0), NAMESPACE)).catch(() => null);
          samples.push({ at, asleep, service, given, applied, ready: pod?.status?.conditions?.some(({ type, status }) => type === "Ready" && status === "True") ?? false });
          await sleep(1000);
        }
      })();
      const ran = await cutOver({ failAt: "midway-through-the-old-snapshots-deleted" }).finally(() => {
        running = false;
      });
      await sampling;
      // The seconds from each sample `pick` holds of to the next.
      const spans = (pick: (sample: (typeof samples)[number]) => boolean) =>
        samples.slice(1).reduce((sum, sample, index) => sum + (pick(samples[index]!) ? sample.at - samples[index]!.at : 0), 0) / 1000;
      const unreachable = spans(({ asleep, service, given }) => !asleep && !service && !given);
      const pruned = spans(({ asleep, given, applied }) => !asleep && applied && !given);
      console.error(`# main's compute had no pageserver address for ${unreachable}s, where ${PREVIOUS}'s Service pruned with this release applied would have left it none for ${pruned}s; ` +
        `pod 0 was not ready for ${spans(({ ready }) => !ready)}s, of the cut-over's ${(samples.at(-1)!.at - samples[0]!.at) / 1000}s`);
      assert.equal(unreachable, 0);
      assert.equal(await kube.get(ref("Service", neonName("pageserver"), NAMESPACE)), null);
      assert.notEqual(ran.code, 0, ran.stdout);
      assert.doesNotMatch(ran.stderr, /registering node 1 anew|starting the storage controller again/u);
      assert.match(ran.stderr, /deleting 7\.0\.0's pageservers' Service, main's compute given pod 0's\n[^\n]* deleting what 7\.0\.0 kept of the sessions' snapshots, the upgrade through\n[^]*FAILED: what 7\.0\.0 kept of the snapshots could not all be deleted: the run again goes on with it\n$/u);
      assert.match(ran.stderr, /a failure injected midway through the deletion of what 7\.0\.0 kept of the snapshots/u);
      // What PREVIOUS kept, held through the upgrade, and one object of it deleted as the run failed.
      const kept = await keptByPrevious();
      assert.equal(kept.length, keptBefore.length - 1, kept.join(", "));
      assert.ok(kept.every((key) => keptBefore.includes(key)), kept.join(", "));
    });

    test(`upgraded: main's session's snapshot moved to this release's layout, under its id to the millisecond, its Sandbox bound to it, and restored with its REPL whole`, { timeout: 30 * 60_000 }, async () => {
      const bound = (await kube.get<Sandbox>(ref("Sandbox", volumeId, SESSIONS)))?.metadata.annotations?.[SNAPSHOT_ANNOTATION] ?? "";
      assert.match(bound, new RegExp(`^${releasedAs.slice(0, 15)}[0-9]{3}${releasedAs.slice(15)}$`, "u"));
      const moved = await snapshotOf(volumeId);
      assert.equal(moved?.snapshot, bound);
      assert.equal(moved?.layers.length, 1);
      await tg.say("from the snapshot moved");
      await tg.waitFor((call) => answered(call));
      const seen = await repl.read(volumeId, releasedRepl);
      assert.deepEqual([seen.live, seen.checkpointed], [`released by ${volumeId}`, `released by ${volumeId}`]);
      assert.match(seen.notice ?? "", CAME_BACK_WHOLE);
    });

    test(`the cut-over from ${PREVIOUS} run again goes on with the upgrade, and the deletion of what ${PREVIOUS} kept of the snapshots, alone`, { timeout: 60 * 60_000 }, async () => {
      const again = await cutOver();
      assert.equal(again.code, 0, again.stdout);
      assert.match(again.stderr, /upgrading, from the cut-over begun at [^]* deleting what 7\.0\.0 kept of the sessions' snapshots, the upgrade through\n[^]*deleted [0-9]+ objects 7\.0\.0 kept of the sessions' snapshots\n[^\n]* upgraded\n$/u);
      assert.deepEqual(await keptByPrevious(), []);
      assert.doesNotMatch(again.stderr, /checking that the storage controller has node 1|deleting the pageservers' StatefulSet|registering node 1 anew|pageservers' Service through the upgrade|main's compute given pod 0's/u);
    });

    for (const { keeps, check } of scenarios) test(`upgraded: ${keeps}`, check);
  });
}

describe("alasio's command line, on the alasio it installed", () => {
  test("status says the cluster's nodes run, and each of alasio's workloads is ready", async () => {
    const said = await alasioOk("status");
    if (TARGET === "host") {
      assert.match(said, /^k3s v\S+ on this machine, with gVisor \S+( and \S+)?:\n {2}service k3s: active, enabled\n {2}node \S+: ready\n/u);
    } else {
      assert.match(said, new RegExp(`^cluster ${CLUSTER}, in Docker `, "u"));
      const nodes = [`${CLUSTER}-server-0`, ...Array.from({ length: AGENTS }, (_, index) => `${CLUSTER}-agent-${index}`)];
      for (const node of nodes) assert.ok(said.includes(`\n  ${node}: running\n`), said);
    }
    assert.ok(said.includes(`  Deployment ${NAMESPACE}/${RELEASE}: ready\n`), said);
    assert.ok(said.includes(`  StatefulSet ${NAMESPACE}/${RELEASE}-neon-pageserver: ready\n`), said);
    // The object store's copies, as the run gives them, and each volume server's disk, as its metrics say.
    assert.ok(said.includes(AGENTS >= 2 ? "\nthe object store: each object kept twice (replication 001)\n" : "\nthe object store: each object kept once (replication 000)\n"), said);
    const servers = AGENTS >= 2 ? 3 : 1;
    for (let ordinal = 0; ordinal < servers; ordinal += 1) {
      assert.match(said, new RegExp(`\\n {2}${RELEASE}-seaweedfs-volume-${ordinal}: \\S+ \\S+ to fill before it takes no more writes, of \\S+ \\S+ free on its disk`, "u"));
    }
  });

  test("logs shows what alasio and its components log, and names those it has when asked for another", async () => {
    assert.notEqual((await alasioOk("logs")).trim(), "");
    await alasioOk("logs", "lake", "--since", "1h");
    const unknown = await alasio("logs", "nothing");
    assert.equal(unknown.code, 1);
    assert.match(unknown.stderr, /^alasio: alasio has no component nothing; it has .*\blake\b/mu);
  });

  test("lake answers a query of the analytics lake, in the format asked for", async () => {
    assert.match(await alasioOk("lake", "SELECT 42 AS answer"), /^answer\n-+\n42\n\(1 rows\)$/mu);
    assert.equal((await alasioOk("lake", "--format", "json", "SELECT 42 AS answer")).trim(), '{"answer":42}');
  });
});

let telegram: Forward | undefined;
let sink: Forward | undefined;
let tg: ReturnType<typeof operator>;

/** Creates a session filesystem through /workspace, as the operator does: its volume id. */
async function newSession(net: NetMode) {
  await tg.calls();
  await tg.say("/workspace");
  await tg.choose(/New empty workspace/u);
  await tg.choose(net === "full" ? /Full internet/u : /No internet/u);
  const created = await tg.waitFor((call) => /Created and mounted session workspace/u.test(call.payload.text ?? ""));
  const volumeId = /fs-[0-9a-f]+/u.exec(created.payload.text ?? "")?.[0];
  assert.ok(volumeId, `alasio named no workspace: ${created.payload.text}`);
  return volumeId;
}

const probe = (target: string) => `fetch(${JSON.stringify(target)},{signal:AbortSignal.timeout(5000)}).then(r=>console.log("open"),e=>console.log("blocked"))`;

/** That the session, which has no internet, reaches neither the internet, nor the cluster's API server, nor a resolver. */
async function assertNoInternet(volumeId: string): Promise<void> {
  assert.equal((await inSession(volumeId, probe("http://1.1.1.1"))).trim(), "blocked");
  assert.equal((await inSession(volumeId, tcp("process.env.KUBERNETES_SERVICE_HOST", 443))).trim(), "blocked");
  assert.match(await inSession(volumeId, 'require("dns").promises.lookup("example.com").then(()=>console.log("resolved"),()=>console.log("no dns"))'), /no dns/u);
}

const roundtrip = (volumeId: string) => inAlasio<RoundtripSeen>("./session-roundtrip.ts", volumeId);

if (inShard("sessions")) {
  describe("alasio on Kubernetes", () => {
    let none: string;
    let full: string;

    before(async () => {
      telegram = await forward(TELEGRAM);
      tg = operator(telegram.base);
      await tg.waitFor((call) => call.method === "setMyCommands", 300_000).catch(() => {});
    });

    after(() => {
      telegram?.close();
    });

    test("a new empty workspace without internet is a session of its own, confined", async () => {
      none = await newSession("none");
      const pod = await kube.get<V1Pod>(ref("Pod", none, SESSIONS));
      if (pod?.spec?.runtimeClassName === "gvisor") assert.match(await inSession(none, 'console.log(require("fs").readFileSync("/proc/version","utf8"))'), /gvisor/u);
      await assertNoInternet(none);
      assert.match(await inSession(none, 'console.log(require("fs").existsSync("/var/run/secrets/kubernetes.io/serviceaccount"))'), /false/u);
      const gate = (await kube.get<V1Pod>(ref("Pod", none, SESSIONS)))?.status?.initContainerStatuses?.find(({ name }) => name === "egress-gate");
      assert.equal(gate?.state?.terminated?.exitCode, 0);
    });

    test("a new workspace with internet reaches the internet and nothing private", async () => {
      full = await newSession("full");
      assert.equal((await inSession(full, probe("https://example.com"))).trim(), "open");
      assert.equal((await inSession(full, tcp("process.env.KUBERNETES_SERVICE_HOST", 443))).trim(), "blocked");
      const other = (await kube.get<V1Pod>(ref("Pod", none, SESSIONS)))?.status?.podIP;
      assert.ok(other, `the session ${none} has no address`);
      assert.equal((await inSession(full, probe(`http://${other}:7290/mcp`))).trim(), "blocked");
      assert.match(await inSession(full, `require("dns").promises.lookup("${none}.${SESSIONS}.svc.cluster.local").then(()=>console.log("resolved"),()=>console.log("unresolved"))`), /unresolved/u);
    });

    test("a session's bayma answers alasio alone, and only with the session's token", async () => {
      const url = `http://${none}.${SESSIONS}.svc.cluster.local:7290/mcp`;
      const token = await kube.secret(SESSIONS, `${none}-bayma-token`, "token");
      const fromAlasio = async (headers: string[]) => (await inAlasioContainer(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "5", ...headers, url])).trim();
      assert.equal(await fromAlasio([]), "401");
      assert.equal(await fromAlasio(["-H", "Authorization: Bearer wrong"]), "401");
      assert.equal(await fromAlasio(["-H", `Authorization: Bearer ${token}`]), "400");
      const stub = await kube.runningPod(STAND_INS, TELEGRAM.name);
      const fromStub = (await kube.execOk(STAND_INS, stub, ["node", "-e", probe(url)], { container: TELEGRAM.container })).trim();
      assert.equal(fromStub, "blocked");
    });

    test("alasio reads a session's files as its agent, a suspended session resumes with them, and one recorded of another pod template is moved onto the current one with them", async () => {
      const seen = await roundtrip(none);
      assert.equal(seen.exec, '"written"');
      assert.match(seen.read ?? "", /^hello from /u);
      assert.equal(seen.missing, "file not found");
      assert.equal(seen.suspendedPod, "gone");
      assert.equal(seen.whileSuspended, "the session is not running");
      assert.equal(seen.afterResume, seen.read);
      assert.equal(seen.execAfterResume, JSON.stringify(seen.read));
      assert.equal(seen.podReplaced, true);
      assert.equal(seen.execAfterMove, JSON.stringify(seen.read));
    });

    test("a folder conversation's bayma works on the machine as the operator, in their home", { skip: !HOST_PROFILE && "folder workspaces are for a single node" }, async () => {
      const { home } = paths();
      const { url, seen } = await inAlasio<FolderBaymaSeen>("./folder-bayma.ts");
      assert.match(url, /^http:\/\/bayma-[0-9a-f]{20}\.alasio-host\.svc/u);
      assert.deepEqual(seen, { uid: HOST_USER, home });
      // The operator's home is the machine's, shared between alasio and the folder's bayma.
      assert.equal(readFileSync(`${home}/e2e-folder-proof`, "utf8"), `written by ${HOST_USER}`);
      assert.equal(await inAlasioContainer(["cat", `${home}/e2e-folder-proof`]), `written by ${HOST_USER}`);
    });

    test("alasio may make a folder's bayma's Sandbox from its template alone, and no Secret but a Sandbox's token", { skip: !HOST_PROFILE && "folder workspaces are for a single node" }, async () => {
      const made = (await kube.list<Sandbox>("Sandbox", { namespace: HOST_NAMESPACE })).find(({ metadata }) => metadata?.name?.startsWith("bayma-"));
      assert.ok(made?.metadata?.name, "the folder's bayma's Sandbox is made");
      const name = made.metadata.name;
      // As the folder's, under another name and suspended; with `args` after its bayma's.
      const copied = (as: string, args: readonly string[] = []) => {
        const sandbox: Sandbox = JSON.parse(JSON.stringify({ apiVersion: made.apiVersion, kind: made.kind, metadata: { name, namespace: HOST_NAMESPACE }, spec: { ...made.spec, operatingMode: "Suspended" } }).replaceAll(name, as));
        const bayma = sandbox.spec.podTemplate.spec?.containers.find((container) => container.name === "bayma");
        if (bayma) bayma.args = [...(bayma.args ?? []), ...args];
        return sandbox;
      };
      const sandboxes = `/apis/agents.x-k8s.io/v1beta1/namespaces/${HOST_NAMESPACE}/sandboxes`;
      assert.equal(await asAlasio("POST", sandboxes, "application/json", copied("e2e-folder-other", ["--listen", "0.0.0.0:1"])), 422);
      // Its state anywhere but under the template's state root.
      const elsewhere = copied("e2e-folder-elsewhere");
      const args = elsewhere.spec.podTemplate.spec?.containers.find((container) => container.name === "bayma")?.args ?? [];
      const at = args.indexOf("--state-dir") + 1;
      assert.ok(at > 0, "the folder's bayma has a state directory");
      args[at] = "/tmp/claude/e2e";
      assert.equal(await asAlasio("POST", sandboxes, "application/json", elsewhere), 422);
      assert.equal(await asAlasio("POST", sandboxes, "application/json", copied("e2e-folder-made")), 201);
      await kube.remove(ref("Sandbox", "e2e-folder-made", HOST_NAMESPACE));
      // An Opaque Secret named as a token's, holding more than a token; and one holding the token alone, which is made.
      const secrets = `/api/v1/namespaces/${HOST_NAMESPACE}/secrets`;
      const secret = (data: Record<string, string>) => ({ apiVersion: "v1", kind: "Secret", metadata: { name: "e2e-folder-bayma-token" }, type: "Opaque", data });
      assert.equal(await asAlasio("POST", secrets, "application/json", secret({ token: "dG9rZW4=", other: "b3RoZXI=" })), 422);
      assert.equal(await asAlasio("POST", secrets, "application/json", secret({ token: "dG9rZW4=" })), 201);
      await kube.remove(ref("Secret", "e2e-folder-bayma-token", HOST_NAMESPACE));
    });

    test("a message runs a turn on Codex, the run's stand-in for it, whose answer reaches the operator, and its spans the lake", async () => {
      await tg.calls();
      await tg.say("/service codex");
      await tg.waitFor((call) => /^Active: Codex$/mu.test(call.payload.text ?? ""));
      await tg.say("hello");
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER);
      await untilInLake("span of Codex's app-server", "select count(*) as n from otel.traces where ServiceName = 'codex-app-server' and SpanName = 'turn/start'");
    });

    test("alasio comes back from alasio restart with its conversation's workspace", async () => {
      assert.equal(await alasioOk("restart"), "alasio restarted; a turn it was running continues.\n");
      await tg.waitFor((call) => call.method === "setMyCommands", 300_000);
      await tg.say("/workspace");
      const panel = await tg.waitFor((call) => /^Session workspace: /mu.test(call.payload.text ?? ""));
      assert.match(panel.payload.text ?? "", new RegExp(`^Session workspace: ${full}, full internet$`, "mu"));
    });

    test("alasio's pod, deleted, is rescheduled on no volume, and its conversation goes on: its mount, the buttons it had sent, and its session", async () => {
      const volumes = (await kube.get<V1Deployment>(ref("Deployment", RELEASE, NAMESPACE)))?.spec?.template.spec?.volumes ?? [];
      assert.deepEqual(volumes.filter((volume) => volume.persistentVolumeClaim), []);
      await tg.say("/workspace");
      const panel = await tg.waitFor((call) => /^Session workspace: /mu.test(call.payload.text ?? ""));
      const refresh = tg.buttons(panel).find((button) => button.text === "Refresh");
      assert.ok(refresh && "callback_data" in refresh, "the panel has a Refresh button");

      const before = await kube.runningPod(NAMESPACE, RELEASE);
      await kube.remove(ref("Pod", before, NAMESPACE));
      await tg.waitFor((call) => call.method === "setMyCommands", 300_000);
      assert.notEqual(await kube.runningPod(NAMESPACE, RELEASE), before);

      // A button sent by the pod that is gone acts in the one that replaced it.
      await tg.press(refresh.callback_data);
      const refreshed = await tg.waitFor((call) => call.method === "editMessageText" && /^Session workspace: /mu.test(call.payload.text ?? ""));
      assert.match(refreshed.payload.text ?? "", new RegExp(`^Session workspace: ${full}, full internet$`, "mu"));

      // The next message goes on in the session the conversation had, which the new pod's
      // Codex, not having run it, is asked to resume.
      await tg.say("again");
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER);
      await untilInLake("resumed thread of Codex's", "select count(*) as n from otel.traces where ServiceName = 'codex-app-server' and SpanName = 'thread/resume'");
    });
  });
}

/** A REPL of a session's bayma, reached as alasio reaches it (./session-repl.ts): a new one holding `value`, and what one holds. */
const repl = {
  set: (volumeId: string, value: string) => inAlasio<ReplSet>("./session-repl.ts", "set", volumeId, value),
  read: (volumeId: string, sessionId: string) => inAlasio<ReplSeen>("./session-repl.ts", "read", volumeId, sessionId),
  fill: (volumeId: string, mebibytes: number) => inAlasio<ReplFilled>("./session-repl.ts", "fill", volumeId, String(mebibytes)),
  sum: (volumeId: string, sessionId: string) => inAlasio<ReplSummed>("./session-repl.ts", "sum", volumeId, sessionId),
};

/** What bayma tells the first exec of a session that came back whole with its sandbox, and of one that came back from its checkpoint. */
const CAME_BACK_WHOLE = /^bayma: this session came back whole with bayma's sandbox, restored from a snapshot taken at /u;
const CAME_BACK_FROM_CHECKPOINT = /^bayma: this session came back from its checkpoint /u;

/**
 * Waits until alasio has released the session, last used at `since`: its Sandbox suspended,
 * and its pod gone. How long after `since` it was.
 */
async function untilReleased(volumeId: string, since: number): Promise<number> {
  // releaseAfter, and a look at the sessions every 15 seconds, then the save.
  const within = (RELEASE_AFTER_SECONDS + 15) * 1000 + 180_000;
  await until(`the release of the session ${volumeId}`, async () => {
    const sandbox = await kube.get<Sandbox>(ref("Sandbox", volumeId, SESSIONS));
    return sandbox?.spec.operatingMode === "Suspended" && (await kube.get(ref("Pod", volumeId, SESSIONS))) === null ? true : undefined;
  }, within);
  return Date.now() - since;
}

/** Patches the status of the session `volumeId`'s pod with `status` as alasio does, merged as kubelet's patches are, its conditions by their type: the API server's answer. */
const podStatusAsAlasio = (volumeId: string, status: unknown): Promise<number> =>
  asAlasio("PATCH", `/api/v1/namespaces/${SESSIONS}/pods/${volumeId}/status`, "application/strategic-merge-patch+json", { status });

/** The resumed condition of a restored session's pod, set to `value`. */
const resumedCondition = (value: string) => ({ conditions: [{ type: "alasio.dev/resumed", status: value }] });

/**
 * What a node agent's cgroup says of its memory: its limit, what it holds now, and of that,
 * what is anonymous, cached, dirty and being written back, and the two last together (what
 * is written and not yet on the disk); and how often it was OOM-killed.
 */
interface AgentMemory {
  readonly max: string;
  readonly current: number;
  readonly anon: number;
  readonly file: number;
  readonly dirty: number;
  readonly writeback: number;
  readonly unsynced: number;
  readonly oomKills: number;
}

/**
 * What a node agent's cgroup may hold dirty beside its gets' data, which their budget
 * (UNSYNCED_DOWNLOAD_BYTES) bounds: the partial pages at each end of a get's range, and
 * the filesystem's own records of the files they write, both charged to the cgroup too. CI
 * saw 32 KiB of it restoring a 1 GiB session; the agent's memory limit leaves room for it.
 */
const FILESYSTEM_DIRTY_BYTES = 1024 ** 2;

/** The node agents' memory, as profileNodeAgents says it: each one's peaks, and how often the agents restarted while it was read. */
interface AgentsMemory {
  readonly text: string;
  readonly peaks: ReadonlyMap<string, AgentMemory>;
  readonly restarts: number;
}

/**
 * The node agents' memory, sampled every second from their own cgroups until stopped:
 * each one's peak of every measure, how often its cgroup OOM-killed, and how its
 * container last ended where it restarted.
 */
function profileNodeAgents(): { readonly stop: () => void; readonly said: () => Promise<AgentsMemory> } {
  const peaks = new Map<string, AgentMemory>();
  const agents = () => kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("node-agent")) });
  const restartsOf = (pods: readonly V1Pod[]) =>
    pods.reduce((sum, { status }) => sum + (status?.containerStatuses?.find(({ name }) => name === "node-agent")?.restartCount ?? 0), 0);
  const before = agents().then(restartsOf);
  let running = true;
  let unread: string | null = null;
  // The agent's own cgroup: the root of its cgroup namespace, or, sharing the host's, the
  // path /proc/self/cgroup names under the hierarchy.
  const readCgroup = [
    "own=/sys/fs/cgroup$(sed -n 's/^0:://p' /proc/self/cgroup)",
    "[ -f \"$own/memory.max\" ] || own=/sys/fs/cgroup",
    "cat \"$own/memory.max\" \"$own/memory.current\" \"$own/memory.stat\" \"$own/memory.events\"",
  ].join("; ");
  const sample = async () => {
    for (const [node, pod] of await nodeAgents()) {
      const read = await kube.execOk(NAMESPACE, pod, ["sh", "-c", readCgroup], { container: "node-agent" }).catch((error: unknown) => {
        unread ??= error instanceof Error ? error.message : String(error);
        return null;
      });
      if (read === null) continue;
      const [max = "", current = "0", ...rest] = read.trim().split("\n");
      const field = (name: string) => Number(rest.find((line) => line.startsWith(`${name} `))?.split(" ")[1] ?? 0);
      const dirty = field("file_dirty");
      const writeback = field("file_writeback");
      const seen: AgentMemory = { max, current: Number(current), anon: field("anon"), file: field("file"), dirty, writeback, unsynced: dirty + writeback, oomKills: field("oom_kill") };
      const peak = peaks.get(node);
      peaks.set(node, peak ? {
        max,
        current: Math.max(peak.current, seen.current),
        anon: Math.max(peak.anon, seen.anon),
        file: Math.max(peak.file, seen.file),
        dirty: Math.max(peak.dirty, seen.dirty),
        writeback: Math.max(peak.writeback, seen.writeback),
        unsynced: Math.max(peak.unsynced, seen.unsynced),
        oomKills: Math.max(peak.oomKills, seen.oomKills),
      } : seen);
    }
  };
  const looping = (async () => {
    while (running) {
      await sample().catch(() => {});
      await sleep(1000);
    }
  })();
  const mib = (bytes: number) => `${Math.round(bytes / 1024 ** 2)} MiB`;
  return {
    stop: () => {
      running = false;
    },
    said: async () => {
      await looping;
      const pods = await agents();
      const statusOf = (node: string | undefined) => pods.find(({ spec }) => spec?.nodeName === node)?.status?.containerStatuses?.find(({ name }) => name === "node-agent");
      const restarts = (node: string | undefined) => {
        const status = statusOf(node);
        const ended = status?.lastState?.terminated;
        return `${status?.restartCount ?? 0} restarts${ended ? `, last ended ${ended.reason} (exit ${ended.exitCode}) at ${ended.finishedAt}` : ""}`;
      };
      const seen = [...peaks].map(([node, { max, current, anon, file, dirty, writeback, unsynced, oomKills }]) =>
        `${node}: limit ${max === "max" ? max : mib(Number(max))}, peak ${mib(current)} (anon ${mib(anon)}, file ${mib(file)}, dirty ${mib(dirty)}, writeback ${mib(writeback)}, not yet on the disk ${mib(unsynced)}), ${oomKills} OOM kills, ${restarts(node)}`
      );
      const unseen = pods.filter(({ spec }) => !peaks.has(spec?.nodeName ?? "")).map(({ spec }) => `${spec?.nodeName}: not read (${unread ?? "no sample"}), ${restarts(spec?.nodeName)}`);
      return {
        text: [...seen, ...unseen].join("; "),
        peaks,
        restarts: restartsOf(pods) - (await before),
      };
    },
  };
}

/** The session's current snapshot, as a node agent's API gives it to alasio, from the bucket; null when it has none. */
async function snapshotOf(volumeId: string): Promise<Manifest | null> {
  const [pod] = (await nodeAgents()).values();
  assert.ok(pod, "no node agent runs ready");
  const answered = await kube.execOk(NAMESPACE, pod, [
    "sh",
    "-c",
    `curl -sS -w '\\n%{http_code}' -H "authorization: Bearer $${NODE_AGENT_TOKEN_VARIABLE}" "http://127.0.0.1:$0/snapshots/$1/$2"`,
    String(NODE_AGENT_PORT),
    SESSIONS,
    volumeId,
  ], { container: "node-agent" });
  const status = answered.trimEnd().split("\n").at(-1);
  if (status === "404") return null;
  assert.equal(status, "200", answered);
  // The agent answers a manifest, as alasio reads it (src/sandbox/node-agents.ts).
  return JSON.parse(answered.trimEnd().split("\n").slice(0, -1).join("\n")) as Manifest;
}

/** That each container of the pod holds no capability: all dropped, none added, unprivileged, and none to gain. */
function assertNoCapability(pod: V1Pod | null): void {
  const containers = [...(pod?.spec?.initContainers ?? []), ...(pod?.spec?.containers ?? [])];
  assert.ok(containers.length > 0, `there is no pod ${pod?.metadata?.name}`);
  for (const { name, securityContext } of containers) {
    const { capabilities, privileged = false, allowPrivilegeEscalation } = securityContext ?? {};
    assert.deepEqual(
      { drop: capabilities?.drop, add: capabilities?.add ?? [], privileged, allowPrivilegeEscalation },
      { drop: ["ALL"], add: [], privileged: false, allowPrivilegeEscalation: false },
      `the container ${name} of the pod ${pod?.metadata?.name}`,
    );
  }
}

if (inShard("snapshots")) {
  describe("sessions released into whole-sandbox snapshots, and restored from them", () => {
    /** The session released and restored, its REPL, the pod it ran in before, and its snapshot. */
    let session: string;
    let sessionId: string;
    let ran: V1Pod | null;
    let saved: Manifest;

    before(async () => {
      telegram = await forward(TELEGRAM);
      tg = operator(telegram.base);
      await tg.waitFor((call) => call.method === "setMyCommands", 300_000).catch(() => {});
      await tg.say("/service codex");
      await tg.waitFor((call) => /^Active: Codex$/mu.test(call.payload.text ?? ""));
    });

    after(() => {
      telegram?.close();
    });

    /** Says `text` to the conversation's session as a turn: how long its answer took to come. */
    async function turn(text: string): Promise<number> {
      await tg.calls();
      const asked = Date.now();
      await tg.say(text);
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER);
      return Date.now() - asked;
    }

    test("a session that runs no exec and no turn for releaseAfter is released: its Sandbox suspended, holding no pod, its snapshot in the bucket", async (t) => {
      session = await newSession("none");
      ({ sessionId } = await repl.set(session, `held by ${session}`));
      const since = Date.now();
      ran = await kube.get<V1Pod>(ref("Pod", session, SESSIONS));
      const waited = await untilReleased(session, since);
      assert.ok(waited >= (RELEASE_AFTER_SECONDS - 10) * 1000, `the session was released ${waited} ms after its last exec`);
      const snapshot = await snapshotOf(session);
      assert.ok(snapshot, `the session ${session} was released with no snapshot`);
      assert.deepEqual([snapshot.environment, snapshot.workspace], [SESSIONS, session]);
      saved = snapshot;
      t.diagnostic(`released ${(waited / 1000).toFixed(1)}s after its last exec: ${saved.bytes} bytes, its sandbox paused ${saved.pauseMs} ms, uploaded in ${saved.uploadMs} ms`);
    });

    test("the session's pod was held to the CPU features every node's agent said, which alasio up recorded in the sessions' template, as alasio status says", async () => {
      const agents = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("node-agent")) });
      const said = agents.map((pod) => {
        const message = pod.status?.initContainerStatuses?.find(({ name }) => name === "cpu-features")?.state?.terminated?.message;
        return (JSON.parse(message ?? "{}") as { readonly cpuFeatures?: readonly string[] }).cpuFeatures ?? [];
      });
      const [first = []] = said;
      const baseline = first.filter((feature) => said.every((features) => features.includes(feature))).toSorted().join(",");
      assert.notEqual(baseline, "", JSON.stringify(said));
      const templates = JSON.parse((await kube.get<V1ConfigMap>(ref("ConfigMap", componentName("sandbox-templates"), NAMESPACE)))?.data?.["templates.json"] ?? "{}");
      assert.equal(templates.sessions.podTemplate.metadata.annotations["dev.gvisor.internal.cpufeatures"], baseline);
      assert.equal(ran?.metadata?.annotations?.["dev.gvisor.internal.cpufeatures"], baseline);
      const status = await alasioOk("status");
      assert.ok(status.includes(`\n  sessions are held to ${baseline.split(",").length} CPU features, those every node had as alasio up last surveyed them\n`), status);
    });

    test("the session's next turn restores it from its snapshot, which is spent, and its REPL is whole, as bayma tells the next exec", async (t) => {
      const answered = await turn("hello again");
      const pod = await kube.get<V1Pod>(ref("Pod", session, SESSIONS));
      assert.equal(pod?.spec?.runtimeClassName, gvisorRuntimeClass(saved.release));
      const seen = await repl.read(session, sessionId);
      assert.deepEqual([seen.live, seen.checkpointed], [`held by ${session}`, `held by ${session}`]);
      assert.match(seen.notice ?? "", CAME_BACK_WHOLE);
      assert.equal(await snapshotOf(session), null);
      t.diagnostic(`the turn's answer came ${answered} ms after its prompt, the session restored meanwhile on ${pod?.spec?.nodeName}`);
      // As alasio, of its pod's status: its resumed condition, true, alone; not its addresses, nor another condition.
      assert.equal(await podStatusAsAlasio(session, resumedCondition("True")), 200);
      assert.equal(await podStatusAsAlasio(session, resumedCondition("False")), 422);
      assert.equal(await podStatusAsAlasio(session, { conditions: [{ type: "Ready", status: "False" }] }), 422);
      assert.equal(await podStatusAsAlasio(session, { podIPs: [{ ip: "10.42.250.250" }], podIP: "10.42.250.250" }), 422);
    });

    test("the session's pods hold no capability, before its release and after its restore, and the restored one reaches nothing without internet", async () => {
      assertNoCapability(ran);
      assertNoCapability(await kube.get<V1Pod>(ref("Pod", session, SESSIONS)));
      // Those of a process in the restored sandbox, as gVisor gives them: each set empty.
      const held = await inSession(session, 'process.stdout.write(require("fs").readFileSync("/proc/self/status","utf8"))');
      const capabilities = held.split("\n").filter((line) => line.startsWith("Cap"));
      assert.ok(["CapPrm:", "CapEff:", "CapBnd:"].every((set) => capabilities.some((line) => line.startsWith(set))), held);
      assert.deepEqual(capabilities.filter((line) => !/^Cap\w+:\s+0+$/u.test(line)), [], held);
      await assertNoInternet(session);
    });

    test("a session saved on one node is restored on another, from the bucket, its REPL whole", { skip: AGENTS < 2 && "one node" }, async (t) => {
      ({ sessionId } = await repl.set(session, `moved by ${session}`));
      const since = Date.now();
      const node = (await kube.get<V1Pod>(ref("Pod", session, SESSIONS)))?.spec?.nodeName;
      assert.ok(node, `the session ${session} runs on no node`);
      await untilReleased(session, since);
      // Its node takes no pod of the session's, and has no agent to prepare its snapshot,
      // which another node's agent then reads from the bucket.
      const taints = (await kube.get<V1Node>(ref("Node", node)))?.spec?.taints ?? [];
      await kube.patch(ref("Node", node), { spec: { taints: [...taints, { key: "alasio.dev/e2e-away", effect: "NoSchedule" }] } });
      try {
        const agent = (await nodeAgents()).get(node);
        assert.ok(agent, `no node agent runs on ${node}`);
        await kube.remove(ref("Pod", agent, NAMESPACE));
        await until(`the node agent ${agent} gone`, async () => ((await kube.get(ref("Pod", agent, NAMESPACE))) === null ? true : undefined), 300_000);
        const answered = await turn("hello from elsewhere");
        const restoredOn = (await kube.get<V1Pod>(ref("Pod", session, SESSIONS)))?.spec?.nodeName;
        assert.ok(restoredOn && restoredOn !== node, `the session was restored on ${restoredOn}, where it was saved`);
        const seen = await repl.read(session, sessionId);
        assert.deepEqual([seen.live, seen.checkpointed], [`moved by ${session}`, `moved by ${session}`]);
        assert.match(seen.notice ?? "", CAME_BACK_WHOLE);
        t.diagnostic(`the turn's answer came ${answered} ms after its prompt, the session restored meanwhile on ${restoredOn}, saved on ${node}`);
      } finally {
        await kube.patch(ref("Node", node), { spec: { taints } });
        await until(`a node agent ready again on ${node}`, async () => ((await nodeAgents()).has(node) ? true : undefined), 300_000);
      }
    });

    test("a session whose node is drained while it idles is released into a snapshot as its bayma holds its stop, and comes back on another node, its REPL whole", { skip: AGENTS < 2 && "a drain of the cluster's one node stops all of it" }, async (t) => {
      const drained = await newSession("none");
      const { sessionId: held } = await repl.set(drained, `drained with ${drained}`);
      const node = (await kube.get<V1Pod>(ref("Pod", drained, SESSIONS)))?.spec?.nodeName;
      assert.ok(node, `the session ${drained} runs on no node`);
      const began = Date.now();
      try {
        // kubectl drain, of the session's pods alone, as the node's other pods are not this test's.
        assert.match(await onNode(`${CLUSTER}-server-0`, `kubectl drain ${node} --pod-selector=app.kubernetes.io/component=session --delete-emptydir-data --timeout=300s`), /evicted/u);
        const bound = await until("the drained session released into a snapshot", async () => {
          const sandbox = await kube.get<KubernetesObject & { spec?: { operatingMode?: string } }>(ref("Sandbox", drained, SESSIONS));
          const snapshot = sandbox?.metadata?.annotations?.[SNAPSHOT_ANNOTATION];
          return sandbox?.spec?.operatingMode === "Suspended" && snapshot ? snapshot : undefined;
        }, 180_000);
        const manifest = await snapshotOf(drained);
        assert.equal(manifest?.snapshot, bound);
        // A release's snapshot, its sandbox ended by it, not one taken live.
        assert.equal(manifest?.live, undefined);
        t.diagnostic(`released ${Date.now() - began} ms after the drain began: ${manifest?.bytes} bytes, its sandbox paused ${manifest?.pauseMs} ms`);
        await turn("hello after the drain");
        const restoredOn = (await kube.get<V1Pod>(ref("Pod", drained, SESSIONS)))?.spec?.nodeName;
        assert.ok(restoredOn && restoredOn !== node, `the session was restored on ${restoredOn}, the node drained`);
        // The drained node, cordoned, alone held the snapshot: the node it was restored on read it from the bucket.
        const agent = (await nodeAgents()).get(restoredOn);
        assert.ok(agent, `no node agent runs ready on ${restoredOn}`);
        const prepared = (await kube.logs(NAMESPACE, agent, "node-agent")).split("\n").filter((line) => line.includes(`prepared the snapshot ${bound} of ${SESSIONS}/${drained} from the `));
        assert.ok(prepared.length > 0 && prepared.every((line) => line.includes(" from the bucket")), `${restoredOn}'s agent prepared ${bound} so: ${prepared.join(" | ") || "not at all"}`);
        const seen = await repl.read(drained, held);
        assert.deepEqual([seen.live, seen.checkpointed], [`drained with ${drained}`, `drained with ${drained}`]);
        assert.match(seen.notice ?? "", CAME_BACK_WHOLE);
      } finally {
        await onNode(`${CLUSTER}-server-0`, `kubectl uncordon ${node}`);
      }
    });

    test("a session whose pod is deleted outright, as kubectl delete --force or the removal of its Node deletes one, takes no snapshot, and comes back from bayma's checkpoints", async () => {
      const killed = await newSession("none");
      const { sessionId: kept } = await repl.set(killed, `checkpointed by ${killed}`);
      const node = (await kube.get<V1Pod>(ref("Pod", killed, SESSIONS)))?.spec?.nodeName ?? "";
      const deleted = kubectl(["delete", "pod", killed, "--namespace", SESSIONS, "--grace-period=0", "--force"]);
      assert.ok(deleted.ok, deleted.said);
      await turn("hello after the kill");
      const seen = await repl.read(killed, kept);
      assert.deepEqual([seen.live, seen.checkpointed], [null, `checkpointed by ${killed}`]);
      assert.match(seen.notice ?? "", CAME_BACK_FROM_CHECKPOINT);
      assert.equal(await snapshotOf(killed), null, `a snapshot of ${killed} was taken as its pod on ${node} was killed`);
    });

    test("a session whose snapshot's image is corrupted comes back from bayma's checkpoints, as alasio logs and bayma tells the next exec", async () => {
      const corrupt = await newSession("none");
      const { sessionId: kept } = await repl.set(corrupt, `checkpointed by ${corrupt}`);
      await untilReleased(corrupt, Date.now());
      // Restored straight from the bucket wherever it is, where a file of its image is not as it was put.
      const agents = await nodeAgents();
      for (const pod of agents.values()) await kube.execOk(NAMESPACE, pod, ["rm", "-rf", `${SNAPSHOT_CACHE_DIR}/${SESSIONS}/${corrupt}`], { container: "node-agent" });
      const [node = ""] = agents.keys();
      const corrupted = await inNodeAgent<Corrupted>(node, "./snapshot-bucket.ts", "corrupt", SESSIONS, corrupt);
      await turn("hello after the rot");
      const logged = (await kube.logs(NAMESPACE, await kube.runningPod(NAMESPACE, RELEASE), "alasio"))
        .split("\n")
        .find((line) => line.includes(`session ${corrupt} is resumed from bayma's checkpoints, not from its snapshot: `));
      assert.ok(logged?.includes(`is corrupt: its image ${corrupted.image} is not in the bucket as it was put`), `alasio logged no fallback for its corrupted image ${corrupted.image}'s file ${corrupted.path}: ${logged}`);
      const seen = await repl.read(corrupt, kept);
      assert.deepEqual([seen.live, seen.checkpointed], [null, `checkpointed by ${corrupt}`]);
      assert.match(seen.notice ?? "", CAME_BACK_FROM_CHECKPOINT);
      assert.equal(await snapshotOf(corrupt), null);
      // Of a pod not restored, as alasio, nothing of its status.
      assert.equal(await podStatusAsAlasio(corrupt, resumedCondition("True")), 422);
      // A restore that fell back for a failure, as this one did, is what the alert on
      // restores falling back is for: it fires, whenever its message came during the turn.
      const stub = telegram?.base;
      assert.ok(stub, "the Telegram stand-in is not forwarded");
      const deadline = Date.now() + 420_000;
      const alerted = async () => {
        const listed = (await (await fetch(`${stub}/control/calls?since=0`)).json()) as CallsListing;
        return listed.calls.some((call) => call.method === "sendMessage" && /^\*\*Firing\*\*[\s\S]*alertname = Restores are falling back/u.test(call.payload.text ?? ""));
      };
      while (!(await alerted())) {
        assert.ok(Date.now() < deadline, "no alert on restores falling back reached Telegram");
        await sleep(5_000);
      }
    });

    test("a session whose snapshot was taken by a gVisor release no longer kept comes back from bayma's checkpoints, said so, as status says it will", async () => {
      const unkept = await newSession("none");
      const { sessionId: kept } = await repl.set(unkept, `checkpointed by ${unkept}`);
      await untilReleased(unkept, Date.now());
      // Taken, as far as anything says, by a release alasio up no longer keeps.
      const removed = "20990101.0-removed";
      const [node = ""] = (await nodeAgents()).keys();
      const released = await inNodeAgent<Released>(node, "./snapshot-bucket.ts", "release", SESSIONS, unkept, removed);
      await kube.patch(ref("Sandbox", unkept, SESSIONS), { metadata: { annotations: { "alasio.dev/snapshot-release": removed } } });
      const status = await alasioOk("status");
      assert.ok(status.includes(`\n  gVisor ${removed}, not kept: 1 snapshot names it, restored from bayma's checkpoints instead\n`), status);
      // Restored from no snapshot, as alasio logs, counting it a fallback by design, which pages no one.
      const fallbacks = await turn("hello after the release went").then(async () =>
        (await kube.logs(NAMESPACE, await kube.runningPod(NAMESPACE, RELEASE), "alasio"))
          .split("\n")
          .find((line) => line.includes(`session ${unkept} is resumed from bayma's checkpoints, not from its snapshot: gVisor ${removed}, which took it, is no longer kept`))
      );
      assert.ok(fallbacks, `alasio logged no fallback of ${unkept}'s snapshot ${released.snapshot} for its release`);
      const seen = await repl.read(unkept, kept);
      assert.deepEqual([seen.live, seen.checkpointed], [null, `checkpointed by ${unkept}`]);
      assert.match(seen.notice ?? "", CAME_BACK_FROM_CHECKPOINT);
      assert.equal(await snapshotOf(unkept), null);
      const after = await kube.get<Sandbox>(ref("Sandbox", unkept, SESSIONS));
      assert.equal(after?.metadata?.annotations?.["alasio.dev/snapshot-release"], undefined);
    });

    test("a fork of a released session restores its source's REPL", async () => {
      const source = await newSession("none");
      const { sessionId: forked } = await repl.set(source, `forked from ${source}`);
      await untilReleased(source, Date.now());
      await tg.calls();
      await tg.say("/workspace");
      await tg.choose(/^Fork this workspace$/u);
      const panel = await tg.waitFor((call) => call.method === "editMessageText" && /^Forked session workspace /mu.test(call.payload.text ?? ""));
      const fork = new RegExp(`^Forked session workspace ${source} into session workspace (fs-[0-9a-f]+) and switched to it`, "mu").exec(panel.payload.text ?? "")?.[1];
      assert.ok(fork, panel.payload.text);
      await turn("hello from the fork");
      const seen = await repl.read(fork, forked);
      assert.deepEqual([seen.live, seen.checkpointed], [`forked from ${source}`, `forked from ${source}`]);
      assert.match(seen.notice ?? "", CAME_BACK_WHOLE);
    });

    test(`a session holding ${BIG_REPL_MEBIBYTES} MiB live is saved, and restored whole from its node's cache and from the bucket, as the next turn waits`, async (t) => {
      const big = await newSession("none");
      const { sessionId: held, sha256 } = await repl.fill(big, BIG_REPL_MEBIBYTES);
      /** The last line the node agents logged of preparing `snapshot`. */
      const prepared = async (snapshot: string) => {
        for (const pod of (await nodeAgents()).values()) {
          const line = (await kube.logs(NAMESPACE, pod, "node-agent")).split("\n").findLast((each) => each.includes(`prepared the snapshot ${snapshot} `));
          if (line) return line.slice(line.indexOf("prepared"));
        }
        return "not logged";
      };
      for (const from of ["cache", "bucket"] as const) {
        const released = await untilReleased(big, Date.now());
        const snapshot = await snapshotOf(big);
        assert.ok(snapshot, `the session ${big} was released with no snapshot`);
        assert.ok(snapshot.bytes >= BIG_REPL_MEBIBYTES * 1024 * 1024, `its image is ${snapshot.bytes} bytes`);
        t.diagnostic(`saved ${released} ms after its last exec: ${snapshot.bytes} bytes, its sandbox paused ${snapshot.pauseMs} ms, uploaded in ${snapshot.uploadMs} ms`);
        // Restored from the bucket where no node's cache holds it.
        if (from === "bucket") {
          for (const pod of (await nodeAgents()).values()) await kube.execOk(NAMESPACE, pod, ["rm", "-rf", `${SNAPSHOT_CACHE_DIR}/${SESSIONS}/${big}`], { container: "node-agent" });
        }
        // The node agents' memory as they prepare it, said whatever comes of the turn.
        const profile = profileNodeAgents();
        let memory: AgentsMemory | undefined;
        const answered = await turn(`hello with ${BIG_REPL_MEBIBYTES} MiB from the ${from}`).finally(async () => {
          profile.stop();
          memory = await profile.said();
          t.diagnostic(`the node agents' memory as it was restored from the ${from}: ${memory.text}`);
        });
        // Within their budget of what is written and not yet on the disk, beside the filesystem's
        // own, and so never killed for it.
        assert.ok(memory && memory.peaks.size > 0, "the node agents' memory was not read");
        for (const [node, { unsynced, oomKills }] of memory.peaks) {
          assert.ok(
            unsynced <= UNSYNCED_DOWNLOAD_BYTES + FILESYSTEM_DIRTY_BYTES,
            `the node agent on ${node} held ${unsynced} bytes not yet on the disk, past its budget of ${UNSYNCED_DOWNLOAD_BYTES} and the filesystem's ${FILESYSTEM_DIRTY_BYTES}`,
          );
          assert.equal(oomKills, 0, `the node agent on ${node} was OOM-killed`);
        }
        assert.equal(memory.restarts, 0, memory.text);
        const summed = await repl.sum(big, held);
        assert.equal(summed.sha256, sha256);
        assert.match(summed.notice ?? "", CAME_BACK_WHOLE);
        const line = await prepared(snapshot.snapshot);
        assert.ok(line.includes(` from the ${from}: `), line);
        t.diagnostic(`restored from the ${from}: the turn's answer came ${answered} ms after its prompt; ${line}`);
      }
    });

    test("a session released before alasio up changes the sessions' template is restored onto the one it was taken on, which the admission policy pins whole beside the new one, and admits no variant of", async (t) => {
      const taken = await newSession("none");
      const { sessionId: kept } = await repl.set(taken, `taken on the template before by ${taken}`);
      const cpuOf = (sandbox: Sandbox | null) => sandbox?.spec.podTemplate.spec?.containers.find(({ name }) => name === "bayma")?.resources?.requests?.["cpu"];
      const before = cpuOf(await kube.get<Sandbox>(ref("Sandbox", taken, SESSIONS)));
      assert.ok(before, `the session ${taken} asks for no CPU`);
      await untilReleased(taken, Date.now());
      // alasio up, the sessions' pods asking for other CPU: their template changes.
      const { configFile } = paths();
      const config = JSON.parse(readFileSync(configFile, "utf8"));
      const after = before === "37m" ? "41m" : "37m";
      config.install.sessions = { ...config.install.sessions, resources: { ...config.install.sessions?.resources, requests: { ...config.install.sessions?.resources?.requests, cpu: after } } };
      writeFileSync(configFile, JSON.stringify(config));
      await alasioOk("up", "--timeout", "20m");
      const templates = JSON.parse((await kube.get<V1ConfigMap>(ref("ConfigMap", componentName("sandbox-templates"), NAMESPACE)))?.data?.["templates.json"] ?? "{}");
      const cpuIn = (profile: { podTemplate: V1Pod }) => profile.podTemplate.spec?.containers.find(({ name }) => name === "bayma")?.resources?.requests?.["cpu"];
      assert.deepEqual([cpuIn(templates.sessions), cpuIn(templates.previousSessions)], [after, before]);

      // Its next turn restores it onto the template it was taken on, its REPL whole.
      const answered = await turn("hello across the change");
      const restored = await kube.get<Sandbox>(ref("Sandbox", taken, SESSIONS));
      assert.equal(cpuOf(restored), before);
      const seen = await repl.read(taken, kept);
      assert.deepEqual([seen.live, seen.checkpointed], [`taken on the template before by ${taken}`, `taken on the template before by ${taken}`]);
      assert.match(seen.notice ?? "", CAME_BACK_WHOLE);
      t.diagnostic(`the turn's answer came ${answered} ms after its prompt, the session restored onto the template before the current one`);

      // As alasio: a Sandbox as it, on the template before, is made; one of another image on it, or restored from another's image, is not.
      assert.ok(restored);
      const sandboxes = `/apis/agents.x-k8s.io/v1beta1/namespaces/${SESSIONS}/sandboxes`;
      const copied = (name: string, change: (sandbox: Sandbox) => void = () => {}) => {
        const sandbox: Sandbox = JSON.parse(
          JSON.stringify({ apiVersion: restored.apiVersion, kind: restored.kind, metadata: { name, namespace: SESSIONS }, spec: { ...restored.spec, operatingMode: "Suspended" } }).replaceAll(taken, name),
        );
        change(sandbox);
        return sandbox;
      };
      const bayma = (sandbox: Sandbox) => sandbox.spec.podTemplate.spec?.containers.find(({ name }) => name === "bayma");
      // A restore's, of its own image, left paused and ready once resumed, or not.
      const restoringFrom = (workspace: string, { paused = true, gated = true } = {}) => (sandbox: Sandbox) => {
        sandbox.spec.podTemplate.metadata = {
          ...sandbox.spec.podTemplate.metadata,
          annotations: {
            ...sandbox.spec.podTemplate.metadata?.annotations,
            "dev.gvisor.internal.restore.host-image-path": `${SNAPSHOT_CACHE_DIR}/${SESSIONS}/${workspace}/20261013T000000000Z-0123abcd`,
            ...(paused ? { "dev.gvisor.internal.restore.paused": "true" } : {}),
          },
        };
        if (gated && sandbox.spec.podTemplate.spec) sandbox.spec.podTemplate.spec.readinessGates = [{ conditionType: "alasio.dev/resumed" }];
      };
      const pausedAlone = (sandbox: Sandbox) => {
        sandbox.spec.podTemplate.metadata = { ...sandbox.spec.podTemplate.metadata, annotations: { ...sandbox.spec.podTemplate.metadata?.annotations, "dev.gvisor.internal.restore.paused": "true" } };
      };
      for (const [name, change, status] of [
        ["e2e-before", undefined, 201],
        ["e2e-before-image", (sandbox: Sandbox) => {
          const container = bayma(sandbox);
          if (container) container.image = `${container.image?.split("@")[0]}@sha256:${"0".repeat(64)}`;
        }, 422],
        ["e2e-before-own", restoringFrom("e2e-before-own"), 201],
        ["e2e-before-other", restoringFrom(taken), 422],
        ["e2e-before-running", restoringFrom("e2e-before-running", { paused: false }), 422],
        ["e2e-before-ungated", restoringFrom("e2e-before-ungated", { gated: false }), 422],
        ["e2e-before-paused", pausedAlone, 422],
      ] as const) {
        assert.equal(await asAlasio("POST", sandboxes, "application/json", copied(name, change)), status, name);
        if (status === 201) await kube.remove(ref("Sandbox", name, SESSIONS));
      }

      // A session made now is on the new template.
      const made = await newSession("none");
      assert.equal(cpuOf(await kube.get<Sandbox>(ref("Sandbox", made, SESSIONS))), after);
    });
  });
}

if (inShard("workspaces")) {
  describe("diagnostic: the snapshots store from each node's own network, as a checkpoint gofer reaches it", () => {
    test("each node's network reaches the store's Service and pods, and once a policy lets the nodes in", async (t) => {
      const service = await kube.get<V1Service>(ref("Service", componentName("seaweedfs"), NAMESPACE));
      const selector = Object.entries(selectorLabels("seaweedfs")).map(([label, value]) => `${label}=${value}`).join(",");
      const pods = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selector });
      const nodes = await kube.list<V1Node>("Node");
      const targets = [`${service?.spec?.clusterIP}:8333`, ...pods.map((pod) => `${pod.status?.podIP}:8333`)];
      t.diagnostic(`store pods: ${pods.map((pod) => `${pod.metadata?.name} ${pod.status?.podIP} on ${pod.spec?.nodeName}`).join("; ")}`);
      const probe = async (label: string) => {
        for (const node of nodes) {
          const name = node.metadata?.name ?? "";
          const script = [
            "for tool in wget nc curl ip iptables-save timeout; do command -v $tool >/dev/null && printf '%s ' $tool; done; echo",
            ...targets.map((target) => `printf '%s: ' ${target}; (timeout 8 wget -q -T 6 -O /dev/null http://${target}/ 2>&1; echo " rc=$?") | tr '\n' ' '; echo`),
            "ip -4 -o addr 2>/dev/null | awk '{print $2, $4}' | tr '\n' ';'; echo",
            "echo kube-rules: $(iptables-save 2>/dev/null | grep -c -e KUBE-SVC -e KUBE-ROUTER)",
          ].join("\n");
          const said = await onNode(name, script).catch((error: unknown) => String(error));
          t.diagnostic(`${label} ${name} (${node.status?.addresses?.map(({ address }) => address).join(",")}, podCIDR ${node.spec?.podCIDR}):\n${said}`);
        }
      };
      await probe("as installed:");
      // The nodes let in: each node's address and its flannel address, its podCIDR's first.
      const blocks = nodes.flatMap((node) => [
        ...(node.status?.addresses ?? []).filter(({ type }) => type === "InternalIP").map(({ address }) => `${address}/32`),
        ...(node.spec?.podCIDR ? [`${node.spec.podCIDR.split("/")[0]}/32`] : []),
      ]);
      await kube.apply({
        apiVersion: "networking.k8s.io/v1",
        kind: "NetworkPolicy",
        metadata: { name: "diag-nodes-to-store", namespace: NAMESPACE },
        spec: { podSelector: { matchLabels: selectorLabels("seaweedfs") }, policyTypes: ["Ingress"], ingress: [{ from: blocks.map((cidr) => ({ ipBlock: { cidr } })), ports: [{ protocol: "TCP", port: 8333 }] }] },
      } as KubernetesObject);
      t.diagnostic(`let in: ${blocks.join(", ")}`);
      await sleep(10_000);
      await probe("nodes let in:");
      // Left in place: the forks after are restored with it.
    });
  });

  describe("a session workspace forked from Telegram", () => {
    before(async () => {
      telegram = await forward(TELEGRAM);
      tg = operator(telegram.base);
      await tg.waitFor((call) => call.method === "setMyCommands", 300_000).catch(() => {});
    });

    after(() => {
      telegram?.close();
    });

    test("Fork in the workspace panel switches the conversation to a clone of its session workspace, where a turn runs in a session of its own, the source untouched", async () => {
      const source = await newSession("none");
      const inWorkspace = (volumeId: string, script: string) => kube.execOk(SESSIONS, volumeId, ["sh", "-c", script], { container: "bayma" });
      await inWorkspace(source, "echo from the source > /workspace/kept");
      await tg.say("/service codex");
      await tg.waitFor((call) => /^Active: Codex$/mu.test(call.payload.text ?? ""));
      await tg.say("hello from the source");
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER);

      await tg.calls();
      await tg.say("/workspace");
      await tg.choose(/^Fork this workspace$/u);
      // Said, and then on the panel, which shows the fork mounted.
      const panel = await tg.waitFor((call) => call.method === "editMessageText" && /^Forked session workspace /mu.test(call.payload.text ?? ""));
      const fork = new RegExp(`^Forked session workspace ${source} into session workspace (fs-[0-9a-f]+) and switched to it`, "mu").exec(panel.payload.text ?? "")?.[1];
      assert.ok(fork, panel.payload.text);
      assert.match(panel.payload.text ?? "", new RegExp(`^Session workspace: ${fork}, no internet, fork of ${source}$`, "mu"));
      // The fork waits, suspended, for its first turn; the source runs again.
      assert.equal(await kube.get<V1Pod>(ref("Pod", fork, SESSIONS)), null);
      assert.ok(await kube.get<V1Pod>(ref("Pod", source, SESSIONS)));

      await tg.say("hello from the fork");
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER);
      assert.equal(await inWorkspace(fork, "cat /workspace/kept"), "from the source\n");
      await inWorkspace(fork, "echo from the fork > /workspace/kept");
      assert.equal(await inWorkspace(source, "cat /workspace/kept"), "from the source\n");

      // The source is still listed, and switching back to it restores its session.
      await tg.calls();
      await tg.say("/workspace");
      await tg.choose(new RegExp(`^${source}$`, "u"));
      await tg.waitFor((call) => new RegExp(`^Switched to session workspace ${source} `, "mu").test(call.payload.text ?? ""));
    });
  });

  workspaceStorage();
}

/** How many rows of the lake's `otel` metric tables `condition` selects, as SQL. */
const metricRows = (condition: string) =>
  `select sum(n) as n from (${["gauge", "sum", "histogram", "exponential_histogram", "summary"].map((kind) => `select count(*) as n from otel.metrics_${kind} where ${condition}`).join(" union all ")})`;

/**
 * Waits until `sql`, a count as `n`, counts some of the lake's rows, as the collector
 * sends them within seconds, and the loader loads transcripts within its interval. Should
 * none arrive, the failure says what each of `parts` of what `sql` joins answers then as
 * `n`, a count or what it lists, so one failure names the part that is missing.
 */
async function untilInLake(what: string, sql: string, timeoutMs = 240_000, parts: Readonly<Record<string, string>> = {}): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  const answer = (query: string) => lakeQuery(query).then(([row]) => row?.["n"] ?? 0, (error: unknown) => error);
  const said = (answered: unknown) => (answered instanceof Error ? answered.message : typeof answered === "string" && Number.isNaN(Number(answered)) ? answered : `${String(answered)} rows`);
  let last: unknown;
  while (Date.now() < deadline) {
    const answered = await answer(sql);
    last = answered instanceof Error ? answered : Number(answered);
    if (typeof last === "number" && last > 0) return;
    await sleep(5000);
  }
  const counted = await Promise.all(Object.entries(parts).map(async ([part, query]) => `\n  ${part}: ${said(await answer(query))}`));
  assert.fail(`no ${what} arrived in the lake: ${said(last)}${counted.join("")}`);
}

/** Waits until the stand-in backend has received an export of `signal` that contains `needle`. */
async function untilExported(signal: string, needle: string, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    // The sink answers what it received.
    const listed = (await (await fetch(`${sink?.base}/control/exports?contains=${encodeURIComponent(needle)}`)).json()) as ListedExport[];
    if (listed.some((entry) => entry.signal === signal && entry.contains)) return true;
    await sleep(5000);
  }
  return false;
}

/** Grafana's API, as its admin, through alasio grafana's port-forward, until it is closed. */
interface GrafanaApi {
  readonly api: (path: string, init?: RequestInit) => Promise<{ status: number; body: unknown }>;
  readonly close: () => void;
}

/** Grafana's API, as its admin, through `alasio grafana`. */
async function grafanaApi(): Promise<GrafanaApi> {
  const password = (await alasioOk("grafana", "--password")).trim();
  const authorization = `Basic ${Buffer.from(`admin:${password}`).toString("base64")}`;
  const grafana = await alasioRunning("grafana", "--port", "0");
  const base = /http:\/\/127\.0\.0\.1:\d+/u.exec(grafana.said)?.[0] ?? "";
  assert.ok(base, grafana.said);
  return {
    api: async (path, init = {}) => {
      const response = await fetch(`${base}${path}`, { ...init, headers: { authorization, "content-type": "application/json", ...init.headers } });
      const text = await response.text();
      return { status: response.status, body: text.startsWith("{") || text.startsWith("[") ? JSON.parse(text) : text };
    },
    close: () => grafana.child.kill(),
  };
}

/** Sends the stack's collector a turn that failed, as alasio records one, as alasio sends its spans. */
async function sendFailedTurn(): Promise<void> {
  const now = BigInt(Date.now()) * 1_000_000n;
  const attribute = (key: string, value: string) => ({ key, value: { stringValue: value } });
  const turn = {
    resourceSpans: [{
      resource: { attributes: [attribute("service.name", "alasio")] },
      scopeSpans: [{
        scope: { name: "alasio" },
        spans: [{
          traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
          spanId: "00f067aa0ba902b7",
          name: "alasio.turn",
          kind: 1,
          startTimeUnixNano: String(now - 2_000_000_000n),
          endTimeUnixNano: String(now),
          attributes: [attribute("alasio.conversation.id", "telegram:1001"), attribute("alasio.harness", "codex"), attribute("alasio.turn.outcome", "failed")],
          status: { code: 2, message: "the harness failed" },
        }],
      }],
    }],
  };
  await inAlasioContainer(["curl", "-fsS", "-X", "POST", "-H", "content-type: application/json", "--data-binary", "@-", "http://alasio-collector.alasio.svc:4318/v1/traces"], JSON.stringify(turn));
}

/**
 * The alerts that turns are failing alasio sends through the bot until each of its two
 * users has one, and for `more` milliseconds after: their texts, by the chat each went to.
 */
async function untilAlerted(more = 0): Promise<Map<string, string[]>> {
  const alerts = new Map<string, string[]>();
  const collect = async () => {
    for (const call of await tg.calls()) {
      if (call.method !== "sendMessage" || !/Turns are failing/u.test(call.payload.text ?? "")) continue;
      const chat = String(call.payload.chat_id);
      alerts.set(chat, [...(alerts.get(chat) ?? []), call.payload.text ?? ""]);
    }
  };
  for (const deadline = Date.now() + 420_000; alerts.size < 2; await sleep(500)) {
    if (Date.now() > deadline) assert.fail(`not each user was alerted: ${JSON.stringify([...alerts])}`);
    await collect();
  }
  for (const deadline = Date.now() + more; Date.now() < deadline; await sleep(5000)) await collect();
  return alerts;
}

if (inShard("telemetry")) {
  describe("alasio's telemetry", () => {
    before(async () => {
      telegram = await forward(TELEGRAM);
      sink = await forward(OTLP);
      tg = operator(telegram.base);
      await tg.waitFor((call) => call.method === "setMyCommands", 300_000).catch(() => {});
    });

    after(() => {
      telegram?.close();
      sink?.close();
    });

    test("alasio's own traces, logs and metrics are in the lake, queryable through alasio lake", async () => {
      await untilInLake("trace of alasio's", "select count(*) as n from otel.traces where ServiceName = 'alasio'");
      await untilInLake("log record of alasio's", "select count(*) as n from otel.logs where ServiceName = 'alasio'");
      await untilInLake("metric of alasio's", metricRows("ServiceName = 'alasio'"));
    });

    test("a session's bayma's telemetry is in the lake, stamped with its session, and reaches the installation's backend too", async () => {
      const volumeId = await newSession("none");
      await untilInLake("trace of the session's bayma", `select count(*) as n from otel.traces where ServiceName = 'bayma' and ResourceAttributes['alasio.volume.id'] = '${volumeId}'`);
      assert.ok(await untilExported("traces", volumeId, 120_000), "no trace of the session's bayma arrived at the backend stamped with it");
    });

    test("a turn's spans join its transcript in the lake: alasio's turn, Codex's, and the rollout Codex wrote", async () => {
      await tg.calls();
      await tg.say("/service codex");
      await tg.waitFor((call) => /^Active: Codex$/mu.test(call.payload.text ?? ""));
      await tg.say("hello");
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER);
      // alasio's turn names Codex's thread, Codex's span in its trace names Codex's turn, and
      // the thread's rollout, mirrored to Neon and loaded, has that turn, completed.
      await untilInLake(
        "turn joined to its transcript",
        `select count(*) as n from otel.traces turn
          join codex.turns codex on codex.thread_id = turn.SpanAttributes['alasio.session.id']
          join otel.traces app on app.TraceId = turn.TraceId and app.SpanAttributes['turn.id'] = codex.turn_id
          where turn.SpanName = 'alasio.turn' and app.ServiceName = 'codex-app-server' and app.SpanName = 'turn/start' and codex.completed_at is not null`,
        600_000,
        {
          "alasio's turns naming a session": "select count(*) as n from otel.traces where SpanName = 'alasio.turn' and SpanAttributes['alasio.session.id'] is not null",
          "Codex's turn/start spans": "select count(*) as n from otel.traces where ServiceName = 'codex-app-server' and SpanName = 'turn/start'",
          "of them, in the trace of an alasio turn": `select count(*) as n from otel.traces turn join otel.traces app on app.TraceId = turn.TraceId
            where turn.SpanName = 'alasio.turn' and app.ServiceName = 'codex-app-server' and app.SpanName = 'turn/start'`,
          "Codex's turns in the transcripts": "select count(*) as n from codex.turns",
          "of them, completed": "select count(*) as n from codex.turns where completed_at is not null",
          "completed turns of a thread an alasio turn names": `select count(*) as n from otel.traces turn join codex.turns codex on codex.thread_id = turn.SpanAttributes['alasio.session.id']
            where turn.SpanName = 'alasio.turn' and codex.completed_at is not null`,
          "the threads alasio's turns name": "select coalesce(string_agg(distinct SpanAttributes['alasio.session.id'], ', '), 'none') as n from otel.traces where SpanName = 'alasio.turn'",
          "the transcripts' threads": "select coalesce(string_agg(distinct thread_id, ', '), 'none') as n from codex.turns",
        },
      );
    });

    test("the stack's telemetry is in the lake, the compute's and the lake's own among it", async () => {
      // What the collector scrapes is named for its job; the lake exports its events itself.
      await untilInLake("metric of the compute's", metricRows("ServiceName = 'compute'"));
      await untilInLake("log record of the lake's", "select count(*) as n from otel.logs where ServiceName = 'alasio-lake'");
      await untilInLake("metric of JuiceFS's", metricRows("MetricName like 'juicefs_%'"));
      // Each Valkey's, named by its Service.
      for (const valkey of [`${RELEASE}-valkey`, `${RELEASE}-seaweedfs-filer-store`]) {
        await untilInLake(`metric of ${valkey}'s`, metricRows(`MetricName = 'redis.memory.used' and ResourceAttributes['server.address'] = '${valkey}'`));
      }
      // The object store's volume servers', of their disks, labelled with their pods.
      await untilInLake("metric of the object store's volume servers' disks", metricRows("ServiceName = 'seaweedfs' and MetricName = 'SeaweedFS_volumeServer_resource' and Attributes['pod'] like '%-seaweedfs-volume-%'"));
    });

    describe("Grafana", () => {
      let grafana: GrafanaApi | undefined;
      const api: GrafanaApi["api"] = (path, init) => {
        assert.ok(grafana, "Grafana is reached");
        return grafana.api(path, init);
      };

      before(async () => {
        grafana = await grafanaApi();
      });

      after(() => {
        grafana?.close();
      });

      test("alasio grafana reaches Grafana, its data source reads the lake, and its dashboards' queries answer with the lake's rows", async (t) => {
        assert.equal((await api("/api/health")).status, 200);
        assert.deepEqual((await api("/api/datasources/uid/lake/health")).body, { message: "Health check successful", status: "OK" });
        const dashboards = (await api("/api/search?type=dash-db")).body as { title: string; folderTitle: string }[];
        assert.deepEqual(dashboards.map(({ folderTitle, title }) => `${folderTitle}/${title}`).sort(), ["alasio/Agents", "alasio/Conversations", "alasio/Stack health"]);
        // The Stack health dashboard's tables of services and of the object store's volume servers, as their panels query them.
        const stack: { panels: { title: string; targets?: object[] }[] } = JSON.parse(readFileSync(new URL("../../neon/grafana/dashboards/stack-health.json", import.meta.url), "utf8"));
        const queryOf = (panel: string) => stack.panels.find(({ title }) => title === panel)?.targets?.[0] ?? assert.fail(`no panel ${panel}`);
        const services = queryOf("Services now");
        for (const panel of ["Services now", "The object store's volume servers"]) {
          let rows = 0;
          for (const deadline = Date.now() + 180_000; rows === 0 && Date.now() < deadline; await sleep(5000)) {
            const answered = (await api("/api/ds/query", { method: "POST", body: JSON.stringify({ from: "now-1h", to: "now", queries: [queryOf(panel)] }) })).body as {
              results: { A: { frames?: { data: { values: unknown[][] } }[] } };
            };
            rows = answered.results.A.frames?.[0]?.data.values[0]?.length ?? 0;
          }
          assert.ok(rows > 0, `${panel} answered with no row`);
        }
        // A query the lake refuses says why, in Grafana.
        const refused = (await api("/api/ds/query", { method: "POST", body: JSON.stringify({ from: "now-1h", to: "now", queries: [{ ...services, url_options: { ...(services as { url_options: object }).url_options, data: "select nothing" } }] }) })).body as {
          results: { A: { error?: string } };
        };
        assert.match(refused.results.A.error ?? "", /Binder Error: Referenced column "nothing" was not found/u);
        // Started once its database was ready for it, on a fresh install: never restarted for want of it.
        const pod = await kube.get<V1Pod>(ref("Pod", await kube.runningPod(NAMESPACE, "alasio-grafana"), NAMESPACE));
        assert.deepEqual(pod?.status?.containerStatuses?.map(({ name, restartCount }) => [name, restartCount]), [["grafana", 0]]);
        // A restart's back-off, that is: an image's pull's is the registry's, which a pod's first pull can meet.
        const restarts = (await kube.list<CoreV1Event>("Event", { namespace: NAMESPACE }))
          .filter(({ involvedObject, reason, message }) => involvedObject.name?.startsWith("alasio-grafana-") && reason === "BackOff" && message?.startsWith("Back-off restarting"));
        assert.deepEqual(restarts.map(({ message }) => message), []);
        // What Grafana and the lake's query endpoint hold, as their cgroups count it.
        const memory = async (pod: string, container: string) =>
          (await kube.execOk(NAMESPACE, pod, ["cat", "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.peak"], { container })).trim().split("\n").map((bytes) => `${Math.round(Number(bytes) / 1048576)} MiB`).join(", peak ");
        t.diagnostic(`Grafana: ${await memory(await kube.runningPod(NAMESPACE, "alasio-grafana"), "grafana")}`);
        t.diagnostic(`the lake's query endpoint: ${await memory(await kube.runningPod(NAMESPACE, "alasio-lake"), "query")}`);
        t.diagnostic(`the lake service: ${await memory(await kube.runningPod(NAMESPACE, "alasio-lake"), "lake")}`);
      });

      test("a turn that failed alerts each of the bot's users, through the bot", async () => {
        await sendFailedTurn();
        const alerts = await untilAlerted();
        assert.deepEqual([...alerts.keys()].sort(), ["1001", String(OTHER_OPERATOR)]);
        for (const [text] of alerts.values()) assert.match(text ?? "", /conversation = telegram:1001/u);
        const rules = (await api("/api/prometheus/grafana/api/v1/rules")).body as { data: { groups: { rules: { name: string; state: string }[] }[] } };
        assert.equal(rules.data.groups.flatMap((group) => group.rules).find(({ name }) => name === "Turns are failing")?.state, "firing");
      });
    });

    test("the stack's collector sends JuiceFS's metrics and Valkey's to the installation's backend too", async () => {
      // JuiceFS's are its own Prometheus metrics, scraped from its clients and from its
      // driver, whose provisioning errors only its controller counts; Valkey's, the
      // collector's redis receiver's.
      const names = ["juicefs_", "juicefs_provision_errors", "redis.memory.used"];
      const arrived = await Promise.all(names.map((name) => untilExported("metrics", name, 180_000)));
      assert.deepEqual(names.filter((_, index) => !arrived[index]), [], "no metric of these names arrived");
    });
  });
}

if (inShard("neon")) {
  neonStack();

  // On the database as the suite leaves it: on the cluster of three nodes, its tenant split
  // across two pageservers, which every turn reads and writes through main's compute.
  describe("alasio's turns, on its Neon as the neon suite leaves it", () => {
    before(async () => {
      telegram = await forward(TELEGRAM);
      tg = operator(telegram.base);
      await tg.waitFor((call) => call.method === "setMyCommands", 300_000).catch(() => {});
    });

    after(() => {
      telegram?.close();
    });

    test("a message runs a turn on Codex in a session, whose answer reaches the operator, and its rollout, mirrored to Neon, is loaded into the lake", async () => {
      await newSession("none");
      await tg.say("/service codex");
      await tg.waitFor((call) => /^Active: Codex$/mu.test(call.payload.text ?? ""));
      await tg.say("hello");
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER);
      await untilInLake("Codex's turn, mirrored to Neon", "select count(*) as n from codex.turns where completed_at is not null", 600_000);
    });
  });
}

/** The namespace of the scaling suite's own workloads. */
const SCALING = "alasio-e2e-scaling";

/** The pause image, which does nothing, as the suite's own workloads run it. */
const PAUSE = "registry.k8s.io/pause:3.10.1@sha256:278fb9dbcca9518083ad1e11276933a2e96f23de604a3a08cc3c80002767d24c";

/** A pod that does nothing, restricted, labelled `labels`, requesting `requests`, under `runtimeClassName` when given. */
const pausePod = (labels: Record<string, string>, requests: Record<string, string>, runtimeClassName?: string) => ({
  metadata: { labels },
  spec: {
    ...(runtimeClassName ? { runtimeClassName } : {}),
    securityContext: { runAsNonRoot: true, runAsUser: 65535, runAsGroup: 65535, seccompProfile: { type: "RuntimeDefault" } },
    containers: [{
      name: "pause",
      image: PAUSE,
      securityContext: { allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] }, readOnlyRootFilesystem: true },
      resources: { requests },
    }],
  },
});

/** A Deployment of the suite's of `replicas` pods like `pod`, named `name`. */
const deploymentOf = (name: string, replicas: number, pod: ReturnType<typeof pausePod>) => ({
  apiVersion: "apps/v1",
  kind: "Deployment",
  metadata: { name, namespace: SCALING },
  spec: { replicas, selector: { matchLabels: { app: name } }, template: { ...pod, metadata: { labels: { ...pod.metadata.labels, app: name } } } },
});

/** Waits until `check` gives something, every two seconds for `timeoutMs`: what it gave. */
async function until<A>(what: string, check: () => Promise<A | undefined>, timeoutMs: number): Promise<A> {
  for (const deadline = Date.now() + timeoutMs; Date.now() < deadline; await sleep(2000)) {
    const found = await check();
    if (found !== undefined) return found;
  }
  return assert.fail(`${what}, not within ${timeoutMs / 1000}s`);
}

/** Runs kubectl on the run's cluster with `args`, given `input`: whether it succeeded, what it printed, and all it said. */
function kubectl(args: readonly string[], input?: string): { readonly ok: boolean; readonly printed: string; readonly said: string } {
  const run = spawnSync("kubectl", ["--kubeconfig", paths().kubeconfig, ...args], { encoding: "utf8", ...(input === undefined ? {} : { input }) });
  return { ok: run.status === 0, printed: run.stdout, said: `${run.stdout}${run.stderr}` };
}

/** The HorizontalPodAutoscaler KEDA moves Grafana's replicas by, as its ScaledObject says, or none. */
function kedaHpa(): V2HorizontalPodAutoscaler | null {
  const got = kubectl(["get", "horizontalpodautoscaler", "keda-hpa-alasio-grafana", "--namespace", NAMESPACE, "--output", "json"]);
  if (!got.ok && /NotFound/u.test(got.said)) return null;
  assert.ok(got.ok, got.said);
  return JSON.parse(got.printed);
}

/** Asks the API server `method` of `path`, with `body` as `contentType`, as alasio's ServiceAccount, from its pod: the status it answered. */
async function asAlasio(method: string, path: string, contentType: string, body: unknown): Promise<number> {
  const said = await inAlasioContainer([
    "sh",
    "-c",
    'cd /var/run/secrets/kubernetes.io/serviceaccount && curl -sS -o /dev/null -w "%{http_code}" --cacert ca.crt -H "Authorization: Bearer $(cat token)" ' +
    '-H "Content-Type: $2" -X "$0" --data-binary @- "https://kubernetes.default.svc$1"',
    method,
    path,
    contentType,
  ], JSON.stringify(body));
  return Number(said);
}

/** Whether the pod's label says it runs a turn. */
const labelledBusy = async (namespace: string, name: string) => (await kube.get<V1Pod>(ref("Pod", name, namespace)))?.metadata?.labels?.["alasio.dev/busy"] === "true";

if (inShard("scaling")) {
  describe("how alasio is scaled, sized and drained", () => {
    before(async () => {
      telegram = await forward(TELEGRAM);
      tg = operator(telegram.base);
      await tg.waitFor((call) => call.method === "setMyCommands", 300_000).catch(() => {});
      await kube.apply({ apiVersion: "v1", kind: "Namespace", metadata: { name: SCALING, labels: { "pod-security.kubernetes.io/enforce": "restricted" } } });
    });

    after(() => {
      telegram?.close();
    });

    test("KEDA and VPA run restricted, on pinned images, the external metrics served, within the floor's budget for them", async (t) => {
      const controllers = ["keda-operator", "keda-metrics-apiserver", "keda-admission-webhooks", "vpa-recommender", "vpa-updater", "vpa-admission-controller"];
      for (const controller of controllers) {
        const pod = await kube.get<V1Pod>(ref("Pod", await kube.runningPod(NAMESPACE, componentName(controller)), NAMESPACE));
        assert.equal(pod?.spec?.securityContext?.runAsNonRoot, true, controller);
        assert.equal(pod?.spec?.securityContext?.seccompProfile?.type, "RuntimeDefault", controller);
        assert.deepEqual(
          pod?.spec?.containers.map(({ image, securityContext }) => ({
            pinned: /@sha256:[0-9a-f]{64}$/u.test(image ?? ""),
            drop: securityContext?.capabilities?.drop,
            allowPrivilegeEscalation: securityContext?.allowPrivilegeEscalation,
            readOnlyRootFilesystem: securityContext?.readOnlyRootFilesystem,
          })),
          [{ pinned: true, drop: ["ALL"], allowPrivilegeEscalation: false, readOnlyRootFilesystem: true }],
          controller,
        );
      }
      await until("the external metrics' APIService available", async () => {
        const service = await kube.get<KubernetesObject & { status?: { conditions?: { type: string; status: string }[] } }>(ref("APIService", "v1beta1.external.metrics.k8s.io"));
        return service?.status?.conditions?.some(({ type, status }) => type === "Available" && status === "True") || undefined;
      }, 300_000);
      // What each uses, as the metrics server measures it, against the floor's budget of 0.5 GiB for both.
      const used = await until("every controller measured", async () => {
        const measured = await Promise.all(controllers.map(async (controller) => {
          const [pod] = await kube.list<KubernetesObject & { containers?: { usage?: { memory?: string } }[] }>("PodMetrics", {
            namespace: NAMESPACE,
            labelSelector: `app.kubernetes.io/component=${controller}`,
          });
          return pod?.containers?.reduce((sum, { usage }) => sum + memoryBytes(usage?.memory ?? "0"), 0);
        }));
        return measured.every((bytes) => bytes !== undefined) ? measured as number[] : undefined;
      }, 300_000);
      controllers.forEach((controller, index) => t.diagnostic(`${controller}: ${Math.round((used[index] ?? 0) / 2 ** 20)} MiB`));
      assert.ok(used.reduce((sum, bytes) => sum + bytes, 0) < 0.5 * 2 ** 30, "KEDA and VPA within half a GiB");
    });

    test("KEDA scales a workload from none to one on what it waits on, and back to none once it has waited a while", async () => {
      await kube.apply(deploymentOf("e2e-scaled", 0, pausePod({}, { cpu: "1m", memory: "4Mi" })));
      const scaledObject = {
        apiVersion: "keda.sh/v1alpha1",
        kind: "ScaledObject",
        metadata: { name: "e2e-scaled", namespace: SCALING },
        spec: {
          scaleTargetRef: { name: "e2e-scaled" },
          minReplicaCount: 0,
          maxReplicaCount: 1,
          pollingInterval: 5,
          cooldownPeriod: 15,
          // A pod of this label is what the workload waits on.
          triggers: [{ type: "kubernetes-workload", metadata: { podSelector: "e2e-demand=yes", value: "1" } }],
        },
      };
      await kube.apply(scaledObject);
      const replicas = async () => (await kube.get<V1Deployment>(ref("Deployment", "e2e-scaled", SCALING)))?.status?.readyReplicas ?? 0;
      const demand = pausePod({ "e2e-demand": "yes" }, { cpu: "1m", memory: "4Mi" });
      await kube.apply({ apiVersion: "v1", kind: "Pod", ...demand, metadata: { ...demand.metadata, name: "e2e-demand", namespace: SCALING } });
      await until("the workload woken", async () => ((await replicas()) === 1 ? true : undefined), 180_000);
      await kube.remove(ref("Pod", "e2e-demand", SCALING));
      await until("the workload asleep again", async () => ((await kube.get<V1Deployment>(ref("Deployment", "e2e-scaled", SCALING)))?.spec?.replicas === 0 ? true : undefined), 180_000);
    });

    test("VPA resizes a workload's pod in place, without restarting it, under runc and under gVisor", async (t) => {
      // Requests far under what VPA recommends at the least, so it resizes them at once: it may
      // have by the time the pod is first read, so a resize is told from these, not from that read.
      const asked = { cpu: "1m", memory: "4Mi" };
      const sized = (name: string, runtimeClassName?: string) => [
        deploymentOf(name, 1, pausePod({ "app.kubernetes.io/part-of": "alasio" }, asked, runtimeClassName)),
        {
          apiVersion: "autoscaling.k8s.io/v1",
          kind: "VerticalPodAutoscaler",
          metadata: { name, namespace: SCALING, labels: { "app.kubernetes.io/part-of": "alasio" } },
          spec: {
            targetRef: { apiVersion: "apps/v1", kind: "Deployment", name },
            updatePolicy: { updateMode: "InPlace", minReplicas: 1 },
            resourcePolicy: { containerPolicies: [{ containerName: "pause", minAllowed: { cpu: "5m", memory: "16Mi" }, controlledValues: "RequestsOnly" }] },
          },
        },
      ];
      for (const object of [...sized("e2e-sized-runc"), ...sized("e2e-sized-gvisor", "gvisor")]) await kube.apply(object);
      await kube.awaitReady([ref("Deployment", "e2e-sized-runc", SCALING), ref("Deployment", "e2e-sized-gvisor", SCALING)], "5 minutes");
      /** The pod of `name`'s Deployment: its uid, its container's restarts, and its memory request. */
      const podOf = async (name: string) => {
        const [pod] = await kube.list<V1Pod>("Pod", { namespace: SCALING, labelSelector: `app=${name}` });
        return {
          uid: pod?.metadata?.uid,
          restarts: pod?.status?.containerStatuses?.[0]?.restartCount,
          memory: pod?.spec?.containers[0]?.resources?.requests?.["memory"],
          resize: JSON.stringify(pod?.status?.conditions?.filter(({ type }) => type.startsWith("PodResize")) ?? []),
        };
      };
      const names = ["e2e-sized-runc", "e2e-sized-gvisor"];
      const befores = await Promise.all(names.map(podOf));
      for (const [index, name] of names.entries()) {
        const before = befores[index] ?? assert.fail(name);
        const resized = await until(`${name}'s pod resized`, async () => {
          const now = await podOf(name);
          return now.uid !== before.uid || now.memory !== asked.memory ? now : undefined;
        }, 600_000);
        t.diagnostic(`${name}: requests ${asked.memory} to ${resized.memory}, restarts ${resized.restarts}, ${resized.resize}`);
        assert.deepEqual([resized.uid, resized.restarts], [before.uid, 0], `${name}'s pod resized in place, not recreated or restarted`);
        assert.ok(memoryBytes(resized.memory ?? "0") >= 16 * 2 ** 20, resized.memory);
      }
    });

    test("a turn holds alasio's pod and its session's busy until it ends", async () => {
      const volumeId = await newSession("none");
      await tg.say("/service codex");
      await tg.waitFor((call) => /^Active: Codex$/mu.test(call.payload.text ?? ""));
      const alasioPod = await kube.runningPod(NAMESPACE, RELEASE);
      await tg.say("wait 30");
      await until("both pods busy", async () => ((await labelledBusy(NAMESPACE, alasioPod)) && (await labelledBusy(SESSIONS, volumeId)) ? true : undefined), 60_000);
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER, 120_000);
      await until("neither pod busy", async () => (!(await labelledBusy(NAMESPACE, alasioPod)) && !(await labelledBusy(SESSIONS, volumeId)) ? true : undefined), 60_000);
    });

    test("alasio may change no more of a pod than its busy label and annotations, of its own namespace's its own alone, and make no Job, Sandbox or Secret but as it renders them", async () => {
      const volumeId = await newSession("none");
      const grafana = await kube.runningPod(NAMESPACE, componentName("grafana"));
      const imageOf = async (namespace: string, name: string) => (await kube.get<V1Pod>(ref("Pod", name, namespace)))?.spec?.containers[0]?.image;
      const images = [await imageOf(SESSIONS, volumeId), await imageOf(NAMESPACE, grafana)];
      const merged = "application/merge-patch+json";
      const patched = "application/json-patch+json";
      const pod = (namespace: string, name: string) => `/api/v1/namespaces/${namespace}/pods/${name}`;
      // An image the API server lets a pod's container be changed to, but for the policy.
      const otherImage = [{ op: "replace", path: "/spec/containers/0/image", value: "registry.k8s.io/pause:3.10.1" }];
      assert.equal(await asAlasio("PATCH", pod(SESSIONS, volumeId), merged, { metadata: { labels: { "alasio.dev/busy": "true" }, annotations: { "karpenter.sh/do-not-disrupt": "true" } } }), 200);
      assert.equal(await asAlasio("PATCH", pod(SESSIONS, volumeId), merged, { metadata: { labels: { "alasio.dev/busy": null }, annotations: { "karpenter.sh/do-not-disrupt": null } } }), 200);
      assert.equal(await asAlasio("PATCH", pod(SESSIONS, volumeId), patched, otherImage), 422);
      assert.equal(await asAlasio("PATCH", pod(NAMESPACE, grafana), patched, otherImage), 422);
      assert.equal(await asAlasio("PATCH", pod(SESSIONS, volumeId), merged, { metadata: { labels: { "app.kubernetes.io/component": "other" } } }), 422);
      assert.equal(await asAlasio("PATCH", pod(SESSIONS, volumeId), merged, { metadata: { finalizers: ["e2e.alasio.dev/held"] } }), 422);
      assert.equal(await asAlasio("PATCH", pod(SESSIONS, volumeId), merged, { metadata: { ownerReferences: null } }), 422);
      assert.deepEqual([await imageOf(SESSIONS, volumeId), await imageOf(NAMESPACE, grafana)], images);
      // Of its own namespace's pods, its own alone: not the floor's beside it.
      const busyLabel = (busy: boolean) => ({ metadata: { labels: { "alasio.dev/busy": busy ? "true" : null } } });
      const own = await kube.runningPod(NAMESPACE, RELEASE);
      assert.equal(await asAlasio("PATCH", pod(NAMESPACE, grafana), merged, busyLabel(true)), 422);
      assert.equal(await asAlasio("PATCH", pod(NAMESPACE, own), merged, busyLabel(false)), 200);
      // A Sandbox as its session's, but under runc rather than the sessions' runtime; and as it, which is made.
      const session = await kube.get<Sandbox>(ref("Sandbox", volumeId, SESSIONS));
      assert.ok(session);
      const copied = (name: string, runtime: boolean) => {
        const text = JSON.stringify({ apiVersion: session.apiVersion, kind: session.kind, metadata: { name, namespace: SESSIONS }, spec: { ...session.spec, operatingMode: "Suspended" } })
          .replaceAll(volumeId, name);
        const sandbox: Sandbox = JSON.parse(text);
        if (!runtime) delete sandbox.spec.podTemplate.spec?.runtimeClassName;
        return sandbox;
      };
      const sandboxes = `/apis/agents.x-k8s.io/v1beta1/namespaces/${SESSIONS}/sandboxes`;
      assert.equal(await asAlasio("POST", sandboxes, "application/json", copied("e2e-runc", false)), 422);
      assert.equal(await asAlasio("POST", sandboxes, "application/json", copied("e2e-made", true)), 201);
      await kube.remove(ref("Sandbox", "e2e-made", SESSIONS));
      // A Secret that would be a ServiceAccount's token.
      const token = { apiVersion: "v1", kind: "Secret", metadata: { name: "e2e-bayma-token", annotations: { "kubernetes.io/service-account.name": "default" } }, type: "kubernetes.io/service-account-token" };
      assert.equal(await asAlasio("POST", `/api/v1/namespaces/${SESSIONS}/secrets`, "application/json", token), 422);
      // The clone's Job, as the installation gives it alasio, but as KEDA's operator, whose rights reach the whole cluster.
      const templates = await kube.get<V1ConfigMap>(ref("ConfigMap", componentName("sandbox-templates"), NAMESPACE));
      const job: V1Job = JSON.parse(templates?.data?.["templates.json"] ?? "{}").sessions.clone.job;
      const borrowed: V1Job = { ...job, metadata: { ...job.metadata, generateName: "e2e-borrowed-" } };
      if (borrowed.spec?.template.spec) borrowed.spec.template.spec.serviceAccountName = componentName("keda-operator");
      assert.equal(await asAlasio("POST", `/apis/batch/v1/namespaces/${NAMESPACE}/jobs`, "application/json", borrowed), 422);
      const jobs = await kube.list<V1Job>("Job", { namespace: NAMESPACE });
      assert.deepEqual(jobs.filter(({ metadata }) => metadata?.name?.startsWith("e2e-borrowed-")), []);
    });

    test("a session's pod is not evicted while a turn runs in it, and is once the turn ends", async () => {
      const volumeId = await newSession("none");
      await tg.say("/service codex");
      await tg.waitFor((call) => /^Active: Codex$/mu.test(call.payload.text ?? ""));
      await tg.say("wait 30");
      await until("the session busy", async () => ((await labelledBusy(SESSIONS, volumeId)) ? true : undefined), 60_000);
      // An eviction, as a drain or a node autoscaler asks one: what the API server said.
      const evict = () => {
        const eviction = { apiVersion: "policy/v1", kind: "Eviction", metadata: { name: volumeId, namespace: SESSIONS } };
        return kubectl(["create", "--raw", `/api/v1/namespaces/${SESSIONS}/pods/${volumeId}/eviction`, "-f", "-"], JSON.stringify(eviction)).said;
      };
      assert.match(evict(), /disruption budget/u);
      const uid = (await kube.get<V1Pod>(ref("Pod", volumeId, SESSIONS)))?.metadata?.uid;
      assert.ok(uid, "the session's pod is not evicted");
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER, 120_000);
      await until("the session no longer busy", async () => (!(await labelledBusy(SESSIONS, volumeId)) ? true : undefined), 60_000);
      assert.doesNotMatch(evict(), /disruption budget|Error/u);
      // Gone, or going, or made anew by its Sandbox.
      await until("the session's pod evicted", async () => {
        const pod = await kube.get<V1Pod>(ref("Pod", volumeId, SESSIONS));
        return pod === null || pod.metadata?.deletionTimestamp !== undefined || pod.metadata?.uid !== uid ? true : undefined;
      }, 60_000);
    });

    test("a node's drain waits until the turn running in a session there ends, then evicts it", { skip: AGENTS < 2 && "a drain of the cluster's one node stops all of it" }, async () => {
      const volumeId = await newSession("none");
      const node = (await kube.get<V1Pod>(ref("Pod", volumeId, SESSIONS)))?.spec?.nodeName ?? "";
      await tg.say("/service codex");
      await tg.waitFor((call) => /^Active: Codex$/mu.test(call.payload.text ?? ""));
      await tg.say("wait 60");
      await until("the session busy", async () => ((await labelledBusy(SESSIONS, volumeId)) ? true : undefined), 60_000);
      // kubectl drain, of the session's pods alone, as the node's other pods are not this test's.
      let drained = 0;
      const draining = onNode(`${CLUSTER}-server-0`, `kubectl drain ${node} --pod-selector=app.kubernetes.io/component=session --delete-emptydir-data --timeout=300s`).then((said) => {
        drained = Date.now();
        return said;
      });
      try {
        await sleep(20_000);
        assert.equal(drained, 0, "the drain waits while the turn runs");
        assert.ok(await kube.get(ref("Pod", volumeId, SESSIONS)), "the session's pod is not evicted");
        await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER, 120_000);
        const answered = Date.now();
        assert.match(await draining, /evicted/u);
        assert.ok(drained >= answered, "the session was evicted once its turn had answered");
      } finally {
        await draining.catch(() => undefined);
        await onNode(`${CLUSTER}-server-0`, `kubectl uncordon ${node}`);
      }
    });

    test("two Grafanas, one cluster of them, send each alert once", async () => {
      const grafana = await grafanaApi();
      try {
        // Its Alertmanagers' cluster, as its metrics say it, until both are of it.
        let cluster = "";
        await until("both Grafanas in one cluster", async () => {
          const metrics = String((await grafana.api("/metrics")).body);
          cluster = metrics.split("\n").filter((line) => /alertmanager_cluster_|alertmanager_peer_/u.test(line) && !line.startsWith("#")).join("\n");
          return /alertmanager_cluster_members(\{[^}]*\})? 2$/mu.test(cluster) ? true : undefined;
        }, 300_000).catch(async (error: unknown) => {
          console.error(`# Grafana's Alertmanagers' cluster:\n${cluster}`);
          for (const pod of await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: "app.kubernetes.io/component=grafana" })) {
            const logged = (await kube.logs(NAMESPACE, pod.metadata?.name ?? "", "grafana")).split("\n").filter((line) => /cluster|memberlist|gossip|peer/iu.test(line));
            console.error(`# ${pod.metadata?.name}:\n${logged.slice(-40).join("\n")}`);
          }
          throw error;
        });
        await sendFailedTurn();
        // Each user's, and two minutes more, in which a second Grafana's would have come.
        const alerts = await untilAlerted(120_000);
        assert.deepEqual([...alerts.entries()].map(([chat, texts]) => [chat, texts.length]).sort(), [["1001", 1], [String(OTHER_OPERATOR), 1]]);
      } finally {
        grafana.close();
      }
    });

    describe("Grafana, its scale made a range and fixed again", () => {
      const replicas = async () => (await kube.get<V1Deployment>(ref("Deployment", "alasio-grafana", NAMESPACE)))?.spec?.replicas;
      /** Brings alasio up with Grafana's scale `scaling`: the fewest replicas Grafana was given meanwhile. */
      const upScaled = async (scaling: { min: number; max: number }) => {
        const { configFile } = paths();
        const config = JSON.parse(readFileSync(configFile, "utf8"));
        config.install.grafana.scaling = scaling;
        writeFileSync(configFile, JSON.stringify(config));
        let fewest = Number.POSITIVE_INFINITY;
        let applying = true;
        const watching = (async () => {
          while (applying) {
            fewest = Math.min(fewest, (await replicas()) ?? 0);
            await sleep(200);
          }
        })();
        try {
          await alasioOk("up", "--timeout", "20m");
        } finally {
          applying = false;
          await watching;
        }
        return fewest;
      };

      test("keeps its two replicas as KEDA takes them over, and KEDA scales it on its CPU", async () => {
        assert.equal(await upScaled({ min: 2, max: 4 }), 2);
        assert.equal(await replicas(), 2);
        const scaledObject = await until("Grafana's ScaledObject ready", async () => {
          const found = await kube.get<KubernetesObject & { status?: { conditions?: { type: string; status: string }[] } }>(ref("ScaledObject", "alasio-grafana", NAMESPACE));
          return found?.status?.conditions?.some(({ type, status }) => type === "Ready" && status === "True") ? found : undefined;
        }, 180_000);
        assert.ok(scaledObject);
        const hpa = kedaHpa();
        assert.deepEqual(
          [hpa?.spec?.minReplicas, hpa?.spec?.maxReplicas, hpa?.spec?.metrics?.map(({ type, resource }) => [type, resource?.name, resource?.target.averageUtilization])],
          [2, 4, [["Resource", "cpu", 80]]],
        );
      });

      test("ends at its fixed count, which KEDA's scaler, gone first, does not set back", async () => {
        // Above the range's least, toward which KEDA's scaler, were it still there, would take an idle Grafana.
        await upScaled({ min: 3, max: 3 });
        assert.equal(await kube.get(ref("ScaledObject", "alasio-grafana", NAMESPACE)), null);
        assert.equal(kedaHpa(), null);
        await sleep(30_000);
        assert.equal(await replicas(), 3);
      });
    });
  });
}

/** The branch environment the run makes, and its namespaces. */
const BRANCH = "e2e";
const BRANCH_NAMESPACE = `alasio-branch-${BRANCH}`;
const BRANCH_SESSIONS = `${BRANCH_NAMESPACE}-sessions`;
/** The chat, another user's, that main mounts on a folder. */
const FOLDER_CHAT = OTHER_OPERATOR;

/**
 * Main at work, as its operator has it: a session workspace without internet, a file
 * written in it, and a turn on Codex there, in the operator's chat; and, where folder
 * workspaces are on, a folder mounted in another chat, on Codex. The session's volume id.
 */
async function workInMain(): Promise<string> {
  const volumeId = await newSession("none");
  await inSession(volumeId, 'require("fs").writeFileSync("/workspace/proof", "written by main")');
  await tg.say("/service codex");
  await tg.waitFor((call) => /^Active: Codex$/mu.test(call.payload.text ?? ""));
  await tg.say("hello");
  await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER);
  if (HOST_PROFILE) {
    await tg.say("/workspace new e2e-folder", FOLDER_CHAT);
    await tg.waitFor((call) => Number(call.payload.chat_id) === FOLDER_CHAT && /e2e-folder/u.test(call.payload.text ?? ""));
    await tg.say("/service codex", FOLDER_CHAT);
    await tg.waitFor((call) => Number(call.payload.chat_id) === FOLDER_CHAT && /^Active: Codex$/mu.test(call.payload.text ?? ""));
  }
  return volumeId;
}

/**
 * Makes the run's branch environment by `alasio branch create`, with `overrides` over its
 * installation. Its alasio talks to its own bot's stand-in, which `base` forwards to here,
 * as the command line checks its token with it.
 */
async function createBranch(base: string | undefined, overrides: Record<string, unknown> = {}): Promise<void> {
  const work = mkdtempSync(join(tmpdir(), "alasio-e2e-branch-"));
  writeFileSync(join(work, "bot-token"), "456:branch", { mode: 0o600 });
  writeFileSync(join(work, "overrides.json"), JSON.stringify({ ...overrides, alasio: { env: { TELEGRAM_API_ROOT: urlOf(TELEGRAM_BRANCH) } } }));
  await alasioOkWith({ TELEGRAM_API_ROOT: base }, "branch", "create", BRANCH, "--bot-token-file", join(work, "bot-token"), "--overrides", join(work, "overrides.json"));
}

/** Runs `sql` on alasio's database in `namespace` (main's, or a branch's), as the compute's own superuser, in its pod: what psql printed. */
async function onDatabase(namespace: string, sql: string): Promise<string> {
  const pod = await kube.runningPod(namespace, "alasio-neon-compute");
  return (await kube.execOk(namespace, pod, ["psql", "-h", "127.0.0.1", "-p", "55433", "-U", "cloud_admin", "-d", "alasio", "-Atc", sql], { container: "compute" })).trim();
}

/** Calls neon-control's API in its pod with the stack's admin token: what it answered. */
async function neonControl(path: string): Promise<string> {
  const pod = await kube.runningPod(NAMESPACE, "alasio-neon-control");
  const token = await kube.secret(NAMESPACE, "alasio-branches", "control-token");
  return kube.execOk(NAMESPACE, pod, ["curl", "-sS", "-H", "@-", `http://127.0.0.1:8080${path}`], { container: "neon-control", stdin: `authorization: Bearer ${token}\n` });
}

/** Waits until main's lake logs a maintenance pass that `passes` holds for, after those it logged before. */
async function untilMaintained(what: string, passes: (line: string) => boolean): Promise<void> {
  const pod = await kube.runningPod(NAMESPACE, "alasio-lake");
  const seen = (await kube.logs(NAMESPACE, pod, "lake")).split("\n").length;
  const deadline = Date.now() + 240_000;
  while (Date.now() < deadline) {
    const lines = (await kube.logs(NAMESPACE, pod, "lake")).split("\n").slice(seen);
    if (lines.some((line) => line.includes('"message":"maintained"') && passes(line))) return;
    await sleep(5000);
  }
  assert.fail(`main's lake logged no maintenance that ${what}`);
}

if (inShard("branches")) {
  describe("branch environments", () => {
    let branchTelegram: Forward | undefined;
    let branch: ReturnType<typeof operator>;
    let volumeId: string;

    before(async () => {
      telegram = await forward(TELEGRAM);
      branchTelegram = await forward(TELEGRAM_BRANCH);
      tg = operator(telegram.base);
      branch = operator(branchTelegram.base);
      await tg.waitFor((call) => call.method === "setMyCommands", 300_000).catch(() => {});
      // Main's lake loaded and maintained every few seconds, so the run sees its passes.
      const lake = await kube.get<V1Deployment>(ref("Deployment", "alasio-lake", NAMESPACE));
      const containers = lake?.spec?.template.spec?.containers ?? [];
      assert.ok(containers.some(({ name }) => name === "lake"));
      const often = [{ name: "LAKE_INTERVAL_SECONDS", value: "10" }, { name: "LAKE_MAINTENANCE_HOURS", value: "0.002" }];
      // A merge patch replaces the list whole: the query endpoint's container is given back as it was.
      const patched = containers.map((container) => (container.name === "lake" ? { ...container, env: [...(container.env ?? []), ...often] } : container));
      await kube.patch(ref("Deployment", "alasio-lake", NAMESPACE), { spec: { template: { spec: { containers: patched } } } });
      await kube.awaitReady([ref("Deployment", "alasio-lake", NAMESPACE)], "5 minutes");
    });

    after(() => {
      telegram?.close();
      branchTelegram?.close();
    });

    test("main works in a session workspace on Codex, and in a folder in another chat, which the branch inherits", async () => {
      volumeId = await workInMain();
    });

    test("alasio branch create makes the branch, serving its own bot, and main's lake keeps its files while it is", async () => {
      const started = Date.now();
      // Its compute sleeps after a minute idle.
      await createBranch(branchTelegram?.base, { neon: { compute: { suspendAfter: "1m" } } });
      console.error(`# the branch ran ${((Date.now() - started) / 1000).toFixed(1)}s after alasio branch create was run`);
      await branch.waitFor((call) => call.method === "setMyCommands", 300_000);
      assert.match(await alasioOk("branch", "list"), new RegExp(`^${BRANCH}: a branch of main, \\d+m old, made by alasio \\S+, its alasio ready, `, "mu"));
      await untilMaintained("kept the files for the branch", (line) => line.includes(`"keptFilesFor":["${BRANCH}"]`));
    });

    test("a turn on the branch goes on in main's conversation in a fork of its workspace, which main made, and nothing of main's changes", async () => {
      const messages = await onDatabase(NAMESPACE, "select count(*) from state.messages");
      const mounted = await onDatabase(NAMESPACE, "select working_directory from state.conversations where id = 'telegram:1001'");
      assert.equal(mounted, `sessionfs:${volumeId}`);
      await tg.calls();
      await branch.say("again");
      await branch.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER);
      // Forked into the branch's own sessions, under the same id, with main's files.
      assert.ok(await kube.get(ref("Sandbox", volumeId, BRANCH_SESSIONS)));
      const inBranch = (code: string) => kube.execOk(BRANCH_SESSIONS, volumeId, ["node", "-e", code], { container: "bayma" });
      assert.equal(await inBranch('process.stdout.write(require("fs").readFileSync("/workspace/proof", "utf8"))'), "written by main");
      await inBranch('require("fs").writeFileSync("/workspace/proof", "written by the branch")');
      assert.equal(await inSession(volumeId, 'process.stdout.write(require("fs").readFileSync("/workspace/proof", "utf8"))'), "written by main");
      // Main's database, and its bot, as they were.
      assert.equal(await onDatabase(NAMESPACE, "select count(*) from state.messages"), messages);
      assert.equal(await onDatabase(NAMESPACE, "select working_directory from state.conversations where id = 'telegram:1001'"), mounted);
      assert.deepEqual((await tg.calls()).filter((call) => call.method !== "getUpdates" && call.method !== "answerCallbackQuery"), []);
      // The branch's database has the turn main's does not.
      assert.ok(Number(await onDatabase(BRANCH_NAMESPACE, "select count(*) from state.messages")) > Number(messages));
    });

    test("the branch's credentials are its own: main's compute takes neither its password nor its compute's token", async () => {
      const passwordOf = async (namespace: string) => decodeURIComponent(new URL(await kube.secret(namespace, "alasio-database", "url")).password);
      const compute = await kube.runningPod(NAMESPACE, "alasio-neon-compute");
      // By the pod's address, as alasio connects, not the loopback compute_ctl trusts.
      const loginWith = async (password: string) => (await kube.execOk(NAMESPACE, compute, [
        "sh",
        "-c",
        'PGPASSWORD="$1" psql -h "$(hostname -i | cut -d" " -f1)" -p 55433 -U alasio -d alasio -Atc "select current_user" 2>/dev/null || echo refused',
        "sh",
        password,
      ])).trim();
      assert.deepEqual([await loginWith(await passwordOf(NAMESPACE)), await loginWith(await passwordOf(BRANCH_NAMESPACE))], ["alasio", "refused"]);
      const control = await kube.runningPod(NAMESPACE, "alasio-neon-control");
      const specStatus = async (id: string, token: string) => (await kube.execOk(NAMESPACE, control, [
        "node",
        "-e",
        'fetch("http://127.0.0.1:8080/compute/api/v2/computes/" + process.argv[1] + "/spec", { headers: { authorization: "Bearer " + process.argv[2] } }).then((r) => process.stdout.write(String(r.status)))',
        "--",
        id,
        token,
      ])).trim();
      const branchToken = await kube.secret(BRANCH_NAMESPACE, "alasio-neon-compute", "NEON_CONTROL_PLANE_TOKEN");
      assert.deepEqual([await specStatus(`branch-${BRANCH}`, branchToken), await specStatus("alasio", branchToken)], ["200", "403"]);
    });

    test("the branch's telemetry is in main's lake, tagged with the branch", async () => {
      await untilInLake("trace of the branch's alasio", `select count(*) as n from otel.traces where ServiceName = 'alasio' and ResourceAttributes['alasio.branch'] = '${BRANCH}'`);
    });

    test("a folder workspace main mounted is refused on the branch, saying why, and so is mounting one", { skip: !HOST_PROFILE && "folder workspaces are for a single node" }, async () => {
      await branch.say("hi", FOLDER_CHAT);
      await branch.waitFor((call) => Number(call.payload.chat_id) === FOLDER_CHAT && /hit an error: This is the branch environment e2e, .* A folder is this machine's own files/u.test(call.payload.text ?? ""));
      await branch.say("/workspace e2e-folder", FOLDER_CHAT);
      await branch.waitFor((call) => Number(call.payload.chat_id) === FOLDER_CHAT && /^This is the branch environment e2e, .* A folder is this machine's own files/mu.test(call.payload.text ?? ""));
    });

    test("the branch's alasio, compute, read replica and lake fit the run's cluster, as the metrics server measures them", async () => {
      const deadline = Date.now() + 300_000;
      let measured: readonly (KubernetesObject & { containers?: { usage?: { memory?: string } }[] })[] = [];
      while (Date.now() < deadline && measured.length < 4) {
        measured = await kube.list("PodMetrics", { namespace: BRANCH_NAMESPACE }).catch(() => []);
        if (measured.length < 4) await sleep(10_000);
      }
      const pods = await kube.list<V1Pod>("Pod", { namespace: BRANCH_NAMESPACE });
      for (const { metadata, containers } of measured) {
        const node = pods.find((pod) => pod.metadata?.name === metadata?.name)?.spec?.nodeName;
        console.error(`# measured: ${metadata?.name} on ${node} uses ${containers?.map(({ usage }) => usage?.memory).join(" + ")}`);
      }
      assert.equal(measured.length, 4, "the metrics server measured the branch's four pods");
      if (AGENTS === 0) assert.equal(new Set(pods.map((pod) => pod.spec?.nodeName)).size, 1);
    });

    test("the branch's compute sleeps once nothing has had a session on it for its minute, and its first connection, through main's proxy, wakes it", async () => {
      // Its alasio and lake stopped, which hold sessions on it, its lake's loader always.
      for (const name of ["alasio", "alasio-lake"]) await kube.patch(ref("Deployment", name, BRANCH_NAMESPACE), { spec: { replicas: 0 } });
      const compute = ref("Deployment", "alasio-neon-compute", BRANCH_NAMESPACE);
      const deadline = Date.now() + 300_000;
      while ((await kube.get<V1Deployment>(compute))?.spec?.replicas !== 0) {
        assert.ok(Date.now() < deadline, "the branch's compute did not sleep");
        await sleep(5000);
      }
      // With the branch's own credentials, as its alasio connects.
      const proxy = await kube.forward(NAMESPACE, "alasio-neon-proxy", 5432);
      try {
        const url = new URL(await kube.secret(BRANCH_NAMESPACE, "alasio-database", "url"));
        Object.assign(url, { hostname: "127.0.0.1", port: String(proxy.port) });
        const started = Date.now();
        const client = new pg.Client({ connectionString: url.toString(), connectionTimeoutMillis: 60_000 });
        await client.connect();
        try {
          const { rows } = await client.query<{ messages: number }>("select count(*)::int as messages from state.messages");
          console.error(`# the branch's compute woke in ${Date.now() - started} ms`);
          assert.ok(rows[0] && rows[0].messages > 0);
        } finally {
          await client.end();
        }
      } finally {
        proxy.close();
      }
      assert.equal((await kube.get<V1Deployment>(compute))?.spec?.replicas, 1);
    });

    test("alasio branch delete leaves nothing of the branch, and main's lake maintenance deletes files again", async () => {
      const { branches } = JSON.parse(await neonControl("/branches")) as { branches: { name: string; timelineId: string }[] };
      const timeline = branches.find(({ name }) => name === BRANCH)?.timelineId;
      assert.ok(timeline);
      await alasioOk("branch", "delete", BRANCH);
      assert.equal(await alasioOk("branch", "list"), "alasio has no branch environments.\n");
      for (const namespace of [BRANCH_NAMESPACE, BRANCH_SESSIONS]) assert.equal(await kube.get(ref("Namespace", namespace)), null);
      const volumes = await kube.list<V1PersistentVolume>("PersistentVolume");
      assert.deepEqual(volumes.filter(({ spec }) => spec?.claimRef?.namespace === BRANCH_SESSIONS).map(({ metadata }) => metadata?.name), []);
      const { tenantId } = JSON.parse(await kube.secret(NAMESPACE, "alasio-neon-root", "secrets.json")) as { tenantId: string };
      const controller = await kube.runningPod(NAMESPACE, "alasio-neon-storage-controller");
      const found = await kube.execOk(NAMESPACE, controller, ["sh", "-c", 'curl -sS -o /dev/null -w "%{http_code}" -H "authorization: Bearer $CONTROL_PLANE_JWT_TOKEN" "$0"', `http://127.0.0.1:1234/v1/tenant/${tenantId}/timeline/${timeline}`]);
      assert.equal(found, "404");
      await untilMaintained("deleted files again", (line) => !line.includes("keptFilesFor"));
      // Main goes on as it was.
      await tg.say("and main?");
      await tg.waitFor((call) => call.method === "sendRichMessage" && call.payload.rich_message?.markdown === ANSWER);
    });
  });
}

/** What alasio installs on this machine as the host target, and what its cluster leaves there: none of it is left once it is removed. */
const INSTALLED_HERE = [
  "/usr/local/bin/k3s",
  "/usr/local/bin/k3s-uninstall.sh",
  "/usr/local/lib/alasio",
  "/etc/systemd/system/k3s.service",
  "/etc/rancher",
  "/var/lib/rancher",
  "/var/lib/kubelet",
  "/var/lib/juicefs",
  "/etc/sysctl.d/60-alasio-inotify.conf",
];

describe("alasio uninstall --purge", { skip: KEEP && "the run keeps the cluster" }, () => {
  test("removes alasio and the cluster on this machine, with all they keep, and keeps the config", async () => {
    const { configFile, kubeconfig, storage } = paths();
    assert.equal(
      await alasioOk("uninstall", "--purge", "--yes"),
      TARGET === "host"
        ? `alasio and k3s on this machine are removed, with gVisor and all their data; the config at ${configFile} is kept.\n`
        : `alasio and the cluster ${CLUSTER} are removed, with all their data; the config at ${configFile} is kept.\n`,
    );
    assert.ok(!existsSync(storage), `${storage} is left`);
    assert.ok(!existsSync(kubeconfig), `${kubeconfig} is left`);
    assert.ok(existsSync(configFile));
    const status = await alasio("status");
    assert.equal(status.code, 1);
    if (TARGET === "host") {
      assert.equal(status.stdout, "k3s on this machine:\n  not installed\n");
      assert.equal(status.stderr, "alasio: there is no k3s on this machine yet: alasio up installs it\n");
      const left = INSTALLED_HERE.filter((path) => existsSync(path));
      assert.deepEqual(left, [], left.length > 0 ? await onNode("", `ls -laR ${left.join(" ")} 2>&1 | head -50`) : "");
      // Nothing of k3s's, its containerd's or gVisor's runs on, by the executables of what runs.
      const running = "for process in /proc/[0-9]*; do readlink \"$process/exe\" 2>/dev/null; done | grep -E '^(/usr/local/bin/k3s|/usr/local/lib/alasio/gvisor/|/var/lib/rancher/k3s/)' || true";
      assert.equal(await onNode("", running), "");
    } else {
      assert.match(status.stdout, new RegExp(`^cluster ${CLUSTER}, in Docker .*:\n {2}not made\n$`, "u"));
      assert.equal(status.stderr, `alasio: there is no cluster ${CLUSTER} yet: alasio up makes it\n`);
    }
  });
});
