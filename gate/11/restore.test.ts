/**
 * 11's gate (program/11-snapshots.md, gates 1 to 4): alasio up on k3s on the runner, the
 * host target (test/e2e/harness.ts), and a real session pod, its two init containers,
 * its emptyDir /tmp, its restart policy and its JuiceFS volume as alasio makes them,
 * saved by runsc beside gVisor's containerd shim, as a node agent would save it, and
 * restored into a new pod of its Sandbox through the fork's restore annotation, as
 * tools/fork/pod_restore_test.sh restores a one-container pod; then the same for a
 * session whose pod has no init containers and is never restarted, the shape the first
 * shows a restore needs. What it records, it writes to $OUT as evidence.json:
 *
 *   - race: the pod, second by second, after its sandbox is saved and ended, while
 *     kubelet may restart it (restartPolicy Always) before the Sandbox is suspended;
 *   - restore: the new pod, second by second, and its events, until bayma answers;
 *   - read: the REPL's state through a new client, its file in /tmp, and a file in /tmp
 *     it held open;
 *   - egress: whether the restored session, which has no internet, reaches the internet
 *     or the API server.
 *
 * alasio's containerd template passes no restore annotation to runsc yet (11's change set
 * B does), so the gate adds it and restarts k3s, whose pods keep running.
 *
 *   ALASIO_E2E_TARGET=host ALASIO_E2E_REGISTRY=... OUT=... node --test gate/11/restore.test.ts
 */
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { after, before, test } from "node:test";

import type { CoreV1Event, V1ContainerStatus, V1Pod } from "@kubernetes/client-node";

import { NAMESPACE, RELEASE } from "../../cli/src/manifests/common.ts";
import { dumpClusterState, inAlasio, inAlasioContainer, inSession, kube, onNode, ref, SESSIONS, setUp, TARGET, tcp, tearDown } from "../../test/e2e/harness.ts";
import type { Created, Read } from "./session.ts";

const OUT = process.env["OUT"] ?? "/tmp/gate-11";
/** Where k3s's containerd has runsc keep its sandboxes' state, as gVisor's shim runs it. */
const RUNSC_ROOT = "/run/containerd/runsc/k8s.io";
/** The containerd template alasio writes on the host target. */
const TEMPLATE = "/var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.tmpl";
const RESTORE_ANNOTATION = "dev.gvisor.internal.restore.host-image-path";
const IMAGES = "/var/lib/alasio-gate-11";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
/** On the node, which on the host target is this machine. */
const node = (script: string) => onNode("", script);

/** A container's state, in a word, with its restarts. */
function stateOf(status: V1ContainerStatus): string {
  const { state, restartCount } = status;
  const now = state?.running ? "running" : state?.waiting ? `waiting ${state.waiting.reason}` : state?.terminated ? `terminated ${state.terminated.reason} ${state.terminated.exitCode}` : "unknown";
  return `${now}${status.ready ? ", ready" : ""}, ${restartCount} restarts`;
}

/** The session's pod, as the gate records it: gone, or its uid, phase, and each container's state. */
async function podState(volumeId: string): Promise<Record<string, string> | null> {
  const pod = await kube.get<V1Pod>(ref("Pod", volumeId, SESSIONS));
  if (!pod) return null;
  return Object.fromEntries([
    ["uid", pod.metadata?.uid ?? ""],
    ["phase", pod.status?.phase ?? ""],
    ...[...(pod.status?.initContainerStatuses ?? []), ...(pod.status?.containerStatuses ?? [])].map((status) => [status.name, stateOf(status)]),
  ]);
}

/** The pod's state each second for `seconds`, or until `done` says it is done with it. */
async function timeline(volumeId: string, seconds: number, done: (state: Record<string, string> | null) => boolean = () => false) {
  const startedAt = Date.now();
  const seen: { at: number; pod: Record<string, string> | null }[] = [];
  while (Date.now() - startedAt < seconds * 1000) {
    const pod = await podState(volumeId);
    // Each change, and when it was seen.
    if (JSON.stringify(seen.at(-1)?.pod) !== JSON.stringify(pod)) seen.push({ at: (Date.now() - startedAt) / 1000, pod });
    if (done(pod)) break;
    await sleep(1000);
  }
  return seen;
}

/** The events of the session's pods, oldest first. */
async function events(volumeId: string): Promise<string[]> {
  const listed = await kube.list<CoreV1Event>("Event", { namespace: SESSIONS, fieldSelector: `involvedObject.name=${volumeId}` });
  return [...listed]
    .sort((a, b) => new Date(a.lastTimestamp ?? a.eventTime ?? 0).getTime() - new Date(b.lastTimestamp ?? b.eventTime ?? 0).getTime())
    .map(({ type, reason, message, involvedObject }) => `${involvedObject.kind} ${type} ${reason}: ${message?.trim()}`);
}

const evidence: Record<string, unknown> = {};

before(async () => {
  assert.equal(TARGET, "host", "11's gate runs on the host target");
  try {
    await setUp();
  } catch (error) {
    await dumpClusterState();
    throw error;
  }
}, { timeout: 120 * 60_000 });

after(async () => {
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "evidence.json"), `${JSON.stringify(evidence, null, 2)}\n`);
  console.error(JSON.stringify(evidence, null, 2));
  await tearDown();
}, { timeout: 30 * 60_000 });

/**
 * Adds the fork's restore annotation, and gVisor's mount hints, to what k3s's containerd
 * passes runsc's specs, and waits for k3s to run on.
 */
async function passRestoreAnnotations(): Promise<void> {
  await node([
    `grep -q 'pod_annotations' ${TEMPLATE} || sed -i '/runtime_type = "io.containerd.runsc.v1"/a\\  pod_annotations = ["dev.gvisor.internal.restore.*", "dev.gvisor.spec.mount.*"]' ${TEMPLATE}`,
    "systemctl restart k3s",
  ].join(" && "));
  await kube.awaitReady([ref("Deployment", RELEASE, NAMESPACE)], "10 minutes");
  // Its kubelet runs a command in a pod once it has found its pods again.
  for (let attempt = 1; ; attempt++) {
    try {
      await inAlasioContainer(["true"]);
      return;
    } catch (error) {
      if (attempt === 60) throw error;
      await sleep(3_000);
    }
  }
}

/**
 * Makes a session (`bare`: its pod without init containers, never restarted), saves
 * its sandbox, suspends and resumes its Sandbox with the restore annotation, and records
 * all it sees in `seen`, reading the REPL back once its pod is ready. Returns whether
 * it was.
 */
async function saveAndRestore(seen: Record<string, unknown>, ...create: string[]): Promise<boolean> {
  const created = await inAlasio<Created>("../../gate/11/session.ts", "create", ...create);
  seen["created"] = created;
  assert.equal(created.said, '"ready"');
  const { volumeId, replId } = created;
  const sandboxId = (await node(`k3s crictl pods --namespace ${SESSIONS} --name '^${volumeId}$' --state ready --quiet`)).trim();
  assert.ok(sandboxId, `the session ${volumeId} has no ready sandbox`);
  seen["before"] = await podState(volumeId);

  // Saved as a node agent saves it: runsc, with the shim's flags, on the sandbox.
  const image = `${IMAGES}/${volumeId}`;
  const savingAt = Date.now();
  await node(`mkdir -p ${IMAGES} && runsc --root ${RUNSC_ROOT} --systemd-cgroup checkpoint --image-path ${image} ${sandboxId}`);
  seen["saveMs"] = Date.now() - savingAt;
  seen["imageBytes"] = Number((await node(`du -sb ${image} | cut -f1`)).trim());
  seen["race"] = await timeline(volumeId, 30);

  // Suspended, its pod goes; then running again, its new pod restored from the image.
  await kube.patch(ref("Sandbox", volumeId, SESSIONS), { spec: { operatingMode: "Suspended" } });
  seen["suspend"] = await timeline(volumeId, 120, (pod) => pod === null);
  const restoringAt = Date.now();
  await kube.patch(ref("Sandbox", volumeId, SESSIONS), {
    spec: { operatingMode: "Running", podTemplate: { metadata: { annotations: { [RESTORE_ANNOTATION]: image } } } },
  });
  const restore = await timeline(volumeId, 180, (pod) => pod?.["bayma"]?.includes("ready") === true || JSON.stringify(pod).includes("CrashLoopBackOff"));
  seen["restore"] = restore;
  seen["restoreMs"] = Date.now() - restoringAt;
  seen["events"] = await events(volumeId);
  if (restore.at(-1)?.pod?.["bayma"]?.includes("ready") !== true) return false;

  seen["read"] = await inAlasio<Read>("../../gate/11/session.ts", "read", volumeId, replId);
  seen["egress"] = {
    internet: (await inSession(volumeId, tcp('"1.1.1.1"', 53))).trim(),
    apiServer: (await inSession(volumeId, tcp("process.env.KUBERNETES_SERVICE_HOST", 443))).trim(),
  };
  return true;
}

test("a session pod saved beside the shim, as alasio makes it, and restored through the annotation", { timeout: 30 * 60_000 }, async () => {
  try {
    await passRestoreAnnotations();
    const seen: Record<string, unknown> = {};
    evidence["asAlasioMakesIt"] = seen;
    // What this records is the evidence: whether it restores, and what kubelet did.
    seen["restored"] = await saveAndRestore(seen);
  } catch (error) {
    evidence["error"] = error instanceof Error ? error.stack : String(error);
    await dumpClusterState();
    throw error;
  }
});

test("a session pod without init containers, never restarted, its /tmp gVisor's tmpfs, comes back whole in a new pod of its Sandbox", { timeout: 30 * 60_000 }, async () => {
  try {
    const seen: Record<string, unknown> = {};
    evidence["bare"] = seen;
    assert.equal(await saveAndRestore(seen, "bare"), true);
    const read = seen["read"] as Read;
    assert.equal(read.marker, "42");
    assert.equal(read.tmpFile, '"written to /tmp"');
    assert.equal(read.heldFile, '"held open in /tmp"');
    assert.deepEqual(seen["egress"], { internet: "blocked", apiServer: "blocked" });
  } catch (error) {
    evidence["error"] = error instanceof Error ? error.stack : String(error);
    await dumpClusterState();
    throw error;
  }
});
