/**
 * Spike (gate G7 of the sandbox service), not to land: the node's JuiceFS mount held by a
 * pod of its own, a workspace attached from it into a running gVisor pod writing all the
 * while, then that pod's mount taken over by a second pod's, as JuiceFS's smooth upgrade
 * takes a mount over (its FUSE fd passed through its comm socket, the old process told by
 * SIGHUP), the two sharing the host's PID namespace and a /tmp; the first then gone; and the
 * second gone with none after it. Each finding is printed as `SPIKE G7 <finding>: <value>`.
 */
import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { V1ConfigMap, V1CronJob, V1Node, V1Pod } from "@kubernetes/client-node";

import { componentName, NAMESPACE } from "../../cli/src/manifests/common.ts";
import { JUICEFS_ADMIN, WORKSPACES_CREDENTIALS } from "../../cli/src/manifests/workspace-storage.ts";
import { kube, onNode, ref } from "./harness.ts";
import { inPool, isReady, poolPod, ROOT, said } from "./spike-attach.ts";

const NS = "alasio-spike-mount";
import { until } from "./workspace-storage.ts";

/** A pod holding the node's JuiceFS mount at ROOT/jfs, in the foreground, as a mount DaemonSet's would. */
function mounter(name: string, node: string, image: string): V1Pod {
  const credential = (variable: string, key: string) => ({ name: variable, valueFrom: { secretKeyRef: { name: WORKSPACES_CREDENTIALS, key } } });
  return {
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name, namespace: NAMESPACE, labels: JUICEFS_ADMIN },
    spec: {
      nodeName: node,
      hostPID: true,
      restartPolicy: "Never",
      terminationGracePeriodSeconds: 30,
      containers: [{
        name: "juicefs",
        image,
        command: ["sh", "-c", 'mkdir -p /spike/jfs /spike/pods && exec juicefs mount --no-bgjob --cache-size 0 "$META_URL" /spike/jfs'],
        env: [credential("META_URL", "metaurl"), credential("ACCESS_KEY", "access-key"), credential("SECRET_KEY", "secret-key")],
        securityContext: { privileged: true },
        volumeMounts: [{ name: "spike", mountPath: "/spike", mountPropagation: "Bidirectional" }, { name: "tmp", mountPath: "/tmp" }],
      }],
      volumes: [{ name: "spike", hostPath: { path: ROOT, type: "DirectoryOrCreate" } }, { name: "tmp", hostPath: { path: `${ROOT}-tmp`, type: "DirectoryOrCreate" } }],
    },
  };
}

const inMounter = async (pod: string, script: string) => {
  const ran = await kube.exec(NAMESPACE, pod, ["sh", "-c", `${script} 2>&1`]);
  return { code: ran.code, out: ran.stdout.trim() };
};

/** Writes a file a fifth of a second for `seconds`, each fsynced, saying each that fails. */
const WRITER = (seconds: number) => `mkdir -p /attach/ws/workspace/g7 && i=0; end=$(( $(date +%s) + ${seconds} )); while [ $(date +%s) -lt $end ]; do if ! { echo $i > /attach/ws/workspace/g7/$i && sync /attach/ws/workspace/g7/$i; } 2>/tmp/e; then echo "fail $i $(date +%s) $(cat /tmp/e)"; fi; i=$((i+1)); sleep 0.2; done; echo "wrote $i"`;

export function spikeMount(): void {
  describe("spike: the node's JuiceFS mount outliving the pod that holds it (G7)", () => {
    test("taken over by a second pod's as a gVisor pod writes through it, then gone with none after it", async () => {
      const node = (await kube.list<V1Node>("Node"))[0]?.metadata?.name;
      assert.ok(node);
      const templates = await kube.get<V1ConfigMap>(ref("ConfigMap", componentName("sandbox-templates"), NAMESPACE));
      const template = (JSON.parse(templates?.data?.["templates.json"] ?? "{}") as { sessions?: { podTemplate: V1Pod } }).sessions?.podTemplate.spec;
      const image = template?.containers[0]?.image;
      const juicefs = (await kube.get<V1CronJob>(ref("CronJob", componentName("juicefs-quota-check"), NAMESPACE)))?.spec?.jobTemplate.spec?.template.spec?.containers[0]?.image;
      assert.ok(template && image && juicefs);
      await kube.apply({ apiVersion: "v1", kind: "Namespace", metadata: { name: NS } });
      const pod = "pool-g7";
      try {
        await kube.apply(mounter("spike-mounter-a", node, juicefs));
        await until("the first mounter's mount", async () => ((await inMounter("spike-mounter-a", "stat -f -c %T /spike/jfs").catch(() => ({ out: "" }))).out.includes("fuse") ? true : undefined), 120_000);
        said("G7", "juicefs version", (await inMounter("spike-mounter-a", "juicefs version")).out);
        await inMounter("spike-mounter-a", `mkdir -p /spike/jfs/spike/g7/files/workspace /spike/pods/${pod}/ws && chown -R 1000:1000 /spike/jfs/spike/g7 /spike/pods/${pod}`);
        await kube.apply(poolPod(pod, node, template, image, NS));
        await until("the pool pod running", async () => (isReady(await kube.get<V1Pod>(ref("Pod", pod, NS))) ? true : undefined));
        // Attached once it runs, as a claim attaches.
        await inMounter("spike-mounter-a", `mount --bind /spike/jfs/spike/g7/files /spike/pods/${pod}/ws`);
        await until("the workspace in the pool pod", async () => ((await inPool(pod, "test -d /attach/ws/workspace && echo yes", NS)).out === "yes" ? true : undefined), 60_000);
        const writing = inPool(pod, WRITER(100), NS);
        await new Promise((resolve) => setTimeout(resolve, 15_000));

        // A second pod's mount on the same mount point: JuiceFS's smooth upgrade.
        const tookOverAt = Date.now();
        await kube.apply(mounter("spike-mounter-b", node, juicefs));
        await until("the first mounter ended, its mount taken over", async () => {
          const first = await kube.get<V1Pod>(ref("Pod", "spike-mounter-a", NAMESPACE));
          return first?.status?.phase === "Succeeded" || first?.status?.phase === "Failed" ? true : undefined;
        }, 90_000).catch(() => undefined);
        said("G7", "the first mounter after the takeover", (await kube.get<V1Pod>(ref("Pod", "spike-mounter-a", NAMESPACE)))?.status?.phase);
        said("G7", "takeover took ms", Date.now() - tookOverAt);
        said("G7", "the first mounter's log", (await kube.logs(NAMESPACE, "spike-mounter-a", "juicefs").catch(() => "")).split("\n").slice(-12));
        said("G7", "the second mounter's log", (await kube.logs(NAMESPACE, "spike-mounter-b", "juicefs").catch(() => "")).split("\n").slice(-12));
        await kube.remove(ref("Pod", "spike-mounter-a", NAMESPACE)).catch(() => undefined);
        said("G7", "the mount in the pool pod after the takeover", (await inPool(pod, "stat -f -c %T /attach/ws; ls /attach/ws/workspace/g7 | wc -l", NS)).out);
        await new Promise((resolve) => setTimeout(resolve, 20_000));

        // The second gone too, none after it.
        const goneAt = Date.now();
        await kube.remove(ref("Pod", "spike-mounter-b", NAMESPACE));
        await until("the second mounter gone", async () => ((await kube.get<V1Pod>(ref("Pod", "spike-mounter-b", NAMESPACE))) === null ? true : undefined), 120_000);
        said("G7", "the mount in the pool pod with no mounter", { afterMs: Date.now() - goneAt, ...(await inPool(pod, "stat -f -c %T /attach/ws; ls /attach/ws/workspace/g7 | wc -l", NS)) });
        const wrote = await writing;
        said("G7", "the writer", wrote.out.split("\n").slice(-40));
      } finally {
        // Detached first, as a pod whose directory holds a mount is never deleted whole.
        await onNode(node, `umount -l ${ROOT}/pods/${pod}/ws; umount -l ${ROOT}/jfs; true`).catch(() => undefined);
        for (const name of ["spike-mounter-a", "spike-mounter-b"]) await kube.remove(ref("Pod", name, NAMESPACE)).catch(() => undefined);
        await kube.remove(ref("Pod", pod, NS)).catch(() => undefined);
        await kube.remove(ref("Namespace", NS)).catch(() => undefined);
      }
    });
  });
}
