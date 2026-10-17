/**
 * Spike (gates G1 and G6 of the sandbox service), not to land: a workspace's directory on
 * JuiceFS bind-mounted, after a gVisor pod started, into a subdirectory of that pod's own
 * hostPath (`subPathExpr` of its name), as the node agent is to attach it; its files seen,
 * worked on and replaced; how it is released; a bind over the pod's root, not seen; and a
 * directory quota. Each finding is printed as `SPIKE <gate> <finding>: <value>`.
 */
import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { V1ConfigMap, V1CronJob, V1Node, V1Pod } from "@kubernetes/client-node";

import { componentName, NAMESPACE } from "../../cli/src/manifests/common.ts";
import { JUICEFS_ADMIN, WORKSPACES_CREDENTIALS } from "../../cli/src/manifests/workspace-storage.ts";
import { kube, onNode, ref } from "./harness.ts";
import { FILE_OPERATIONS, until, WORKDIR } from "./workspace-storage.ts";

export const SPIKE_NAMESPACE = "alasio-spike";
/** The node's directory of the spike: JuiceFS's mount at jfs/, each pod's attach directory under pods/. */
export const ROOT = "/var/lib/alasio-spike";
const ATTACHER = "spike-attacher";

export const said = (gate: string, finding: string, value: unknown) => console.error(`SPIKE ${gate} ${finding}: ${typeof value === "string" ? value : JSON.stringify(value)}`);

export const isReady = (pod: V1Pod | null | undefined) => pod?.status?.conditions?.some(({ type, status }) => type === "Ready" && status === "True") ?? false;

/** Runs `script` in the attacher, as root on the node's mounts: what it printed and its exit status, never throwing. */
async function inAttacher(script: string): Promise<{ code: number; out: string }> {
  const ran = await kube.exec(NAMESPACE, ATTACHER, ["sh", "-c", `${script} 2>&1`]);
  return { code: ran.code, out: ran.stdout.trim() };
}

/** Runs `script` in the pool pod `pod`, as its agent user: what it printed and its exit status. */
export async function inPool(pod: string, script: string): Promise<{ code: number; out: string }> {
  const ran = await kube.exec(SPIKE_NAMESPACE, pod, ["sh", "-c", `${script} 2>&1`]);
  return { code: ran.code, out: ran.stdout.trim() };
}

/** A pod of the sessions' template, on `node`, its /attach the node's ROOT/pods/<its name>, mounted HostToContainer, and nothing else of a session's. */
export function poolPod(name: string, node: string, template: V1Pod["spec"] & object, image: string): V1Pod {
  return {
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name, namespace: SPIKE_NAMESPACE, labels: { "alasio.dev/spike": "pool" } },
    spec: {
      ...(template.runtimeClassName ? { runtimeClassName: template.runtimeClassName } : {}),
      ...(template.securityContext ? { securityContext: template.securityContext } : {}),
      nodeName: node,
      terminationGracePeriodSeconds: 5,
      restartPolicy: "Never",
      automountServiceAccountToken: false,
      containers: [{
        name: "bayma",
        image,
        command: ["/usr/bin/tini", "-s", "--", "sleep", "infinity"],
        env: [{ name: "POD_NAME", valueFrom: { fieldRef: { fieldPath: "metadata.name" } } }],
        securityContext: template.containers[0]?.securityContext ?? {},
        volumeMounts: [{ name: "attach", mountPath: "/attach", subPathExpr: "$(POD_NAME)", mountPropagation: "HostToContainer" }, { name: "tmp", mountPath: "/tmp" }],
      }],
      volumes: [{ name: "attach", hostPath: { path: `${ROOT}/pods`, type: "DirectoryOrCreate" } }, { name: "tmp", emptyDir: {} }],
    },
  };
}

export function spikeAttach(): void {
  describe("spike: a workspace attached into a running gVisor pod (G1), and its quota (G6)", () => {
    test("attach, work, replace, release, the root not seen, and a quota", async () => {
      const node = (await kube.list<V1Node>("Node"))[0]?.metadata?.name;
      assert.ok(node);
      const templates = await kube.get<V1ConfigMap>(ref("ConfigMap", componentName("sandbox-templates"), NAMESPACE));
      const profile = JSON.parse(templates?.data?.["templates.json"] ?? "{}") as { sessions?: { podTemplate: V1Pod } };
      const template = profile.sessions?.podTemplate.spec;
      const image = template?.containers[0]?.image;
      assert.ok(template && image, "the sessions' template");
      said("G1", "runtime class", template.runtimeClassName ?? "none");
      said("G1", "runsc options", await onNode(node, "cat /var/lib/rancher/k3s/agent/etc/containerd/runsc*.toml 2>/dev/null | grep -v '^#' | tr '\\n' ' ' || true"));

      const quotaCheck = await kube.get<V1CronJob>(ref("CronJob", componentName("juicefs-quota-check"), NAMESPACE));
      const juicefsImage = quotaCheck?.spec?.jobTemplate.spec?.template.spec?.containers[0]?.image;
      assert.ok(juicefsImage, "JuiceFS's image");
      const credential = (name: string, key: string) => ({ name, valueFrom: { secretKeyRef: { name: WORKSPACES_CREDENTIALS, key } } });
      await kube.apply({ apiVersion: "v1", kind: "Namespace", metadata: { name: SPIKE_NAMESPACE } });
      await kube.apply({
        apiVersion: "v1",
        kind: "Pod",
        metadata: { name: ATTACHER, namespace: NAMESPACE, labels: JUICEFS_ADMIN },
        spec: {
          nodeName: node,
          terminationGracePeriodSeconds: 0,
          containers: [{
            name: "juicefs",
            image: juicefsImage,
            command: ["sleep", "infinity"],
            env: [credential("META_URL", "metaurl"), credential("ACCESS_KEY", "access-key"), credential("SECRET_KEY", "secret-key")],
            securityContext: { privileged: true },
            volumeMounts: [{ name: "spike", mountPath: "/spike", mountPropagation: "Bidirectional" }],
          }],
          volumes: [{ name: "spike", hostPath: { path: ROOT, type: "DirectoryOrCreate" } }],
        },
      } as V1Pod);
      try {
        await until("the attacher running", async () => (isReady(await kube.get<V1Pod>(ref("Pod", ATTACHER, NAMESPACE))) ? true : undefined));
        const mounted = await inAttacher([
          "mkdir -p /spike/jfs /spike/pods",
          'juicefs mount --background --no-bgjob --cache-size 0 "$META_URL" /spike/jfs >/dev/null',
          "for w in a b; do mkdir -p /spike/jfs/spike/$w/files/workspace /spike/jfs/spike/$w/files/home && echo $w > /spike/jfs/spike/$w/files/workspace/marker; done",
          "chown -R 1000:1000 /spike/jfs/spike",
          "stat -f -c %T /spike/jfs",
        ].join(" && "));
        said("G1", "JuiceFS mounted in the attacher", mounted);
        assert.equal(mounted.code, 0);

        // The attach directory made before the pod, as the gateway is to have it made.
        const pod = "pool-0";
        assert.equal((await inAttacher(`mkdir -p /spike/pods/${pod}/ws && chown 1000:1000 /spike/pods/${pod} /spike/pods/${pod}/ws`)).code, 0);
        await kube.apply(poolPod(pod, node, template, image));
        await until("the pool pod running", async () => (isReady(await kube.get<V1Pod>(ref("Pod", pod, SPIKE_NAMESPACE))) ? true : undefined));
        said("G1", "kernel", (await inPool(pod, "cat /proc/version")).out);
        said("G1", "before attaching", await inPool(pod, "ls -la /attach/ws; stat -f -c %T /attach/ws"));

        // Attached after the sandbox started.
        const startedAt = Date.now();
        said("G1", "bind", await inAttacher(`mount --bind /spike/jfs/spike/a/files /spike/pods/${pod}/ws && grep spike /proc/self/mountinfo`));
        const seen = await until("the attached workspace seen", async () => ((await inPool(pod, "cat /attach/ws/workspace/marker")).out === "a" ? true : undefined), 60_000).catch(() => false);
        said("G1", "attached workspace seen", { seen, afterMs: Date.now() - startedAt });
        said("G1", "file system seen", (await inPool(pod, "stat -f -c %T /attach/ws; ls -la /attach/ws/workspace")).out);

        if (seen) {
          const operations = FILE_OPERATIONS.split(WORKDIR).join("/attach/ws/workspace/juicefs-e2e");
          const ran = await inPool(pod, operations);
          said("G1", "file operations", ran.out.split("\n"));
          // Written through the pod, seen on JuiceFS from outside it.
          said("G1", "written seen outside", (await inAttacher("ls /spike/jfs/spike/a/files/workspace/juicefs-e2e/operations | head -20")).out);
        }

        // Replaced: another workspace attached in its place.
        said("G1", "detach for replacement", await inAttacher(`umount /spike/pods/${pod}/ws`));
        said("G1", "bind the replacement", await inAttacher(`mount --bind /spike/jfs/spike/b/files /spike/pods/${pod}/ws`));
        const replaced = await until("the replacement seen", async () => ((await inPool(pod, "cat /attach/ws/workspace/marker")).out === "b" ? true : undefined), 60_000).catch(() => false);
        said("G1", "replacement seen", { replaced, marker: (await inPool(pod, "cat /attach/ws/workspace/marker")).out });

        // Released while the sandbox holds a file open in it.
        said("G1", "hold open", await inPool(pod, "nohup sh -c 'exec 3</attach/ws/workspace/marker; sleep 600' >/dev/null 2>&1 & sleep 1; echo held"));
        said("G1", "plain unmount while held", await inAttacher(`umount /spike/pods/${pod}/ws`));
        said("G1", "mounts after it", (await inAttacher("grep spike /proc/self/mountinfo | grep pods || echo none")).out);
        said("G1", "lazy unmount while held", await inAttacher(`umount -l /spike/pods/${pod}/ws`));
        said("G1", "seen in the pod after the lazy unmount", (await inPool(pod, "cat /attach/ws/workspace/marker; ls /attach/ws")).out);
        said("G1", "lazy unmount of the one beneath, held", await inAttacher(`umount -l /spike/pods/${pod}/ws`));
        said("G1", "seen in the pod after both", (await inPool(pod, "cat /attach/ws/workspace/marker; ls -la /attach/ws")).out);
        said("G1", "node mounts before the pod's deletion", (await inAttacher("grep spike /proc/self/mountinfo | grep pods || echo none")).out);
        const deleting = Date.now();
        await kube.remove(ref("Pod", pod, SPIKE_NAMESPACE));
        const gone = await until("the pool pod gone", async () => ((await kube.get<V1Pod>(ref("Pod", pod, SPIKE_NAMESPACE))) === null ? true : undefined), 120_000).catch(() => false);
        said("G1", "pod deleted, its workspace detached first", { gone, afterMs: Date.now() - deleting });

        // A second pod: attached, then released only after its deletion.
        const after = "pool-1";
        await inAttacher(`mkdir -p /spike/pods/${after}/ws && chown 1000:1000 /spike/pods/${after} /spike/pods/${after}/ws`);
        await kube.apply(poolPod(after, node, template, image));
        await until("the second pool pod running", async () => (isReady(await kube.get<V1Pod>(ref("Pod", after, SPIKE_NAMESPACE))) ? true : undefined));
        await inAttacher(`mount --bind /spike/jfs/spike/a/files /spike/pods/${after}/ws`);
        await until("the second attach seen", async () => ((await inPool(after, "cat /attach/ws/workspace/marker")).out === "a" ? true : undefined), 60_000).catch(() => false);
        await inPool(after, "nohup sh -c 'exec 3</attach/ws/workspace/marker; sleep 600' >/dev/null 2>&1 & sleep 1");
        const deletingAfter = Date.now();
        await kube.remove(ref("Pod", after, SPIKE_NAMESPACE));
        const stopped = await until("the second pool pod's container stopped", async () => {
          const ended = await kube.get<V1Pod>(ref("Pod", after, SPIKE_NAMESPACE));
          return ended === null || ended.status?.containerStatuses?.every(({ state }) => state?.terminated) ? true : undefined;
        }, 120_000).catch(() => false);
        said("G1", "the second pod's container stopped, its workspace attached", { stopped, afterMs: Date.now() - deletingAfter });
        said("G1", "plain unmount once its container stopped", await inAttacher(`umount /spike/pods/${after}/ws`));
        const goneAfter = await until("the second pool pod gone", async () => ((await kube.get<V1Pod>(ref("Pod", after, SPIKE_NAMESPACE))) === null ? true : undefined), 120_000).catch(() => false);
        said("G1", "the second pod gone once detached", { gone: goneAfter, afterMs: Date.now() - deletingAfter });

        // Over the root: a bind on the pod's attach directory itself, after it started.
        const root = "pool-2";
        await inAttacher(`mkdir -p /spike/pods/${root} && chown 1000:1000 /spike/pods/${root}`);
        await kube.apply(poolPod(root, node, template, image));
        await until("the third pool pod running", async () => (isReady(await kube.get<V1Pod>(ref("Pod", root, SPIKE_NAMESPACE))) ? true : undefined));
        await inAttacher(`mount --bind /spike/jfs/spike/a/files /spike/pods/${root}`);
        const overRoot = await until("the bind over the root seen", async () => ((await inPool(root, "cat /attach/workspace/marker")).out === "a" ? true : undefined), 20_000).catch(() => false);
        said("G1", "bind over the root seen", overRoot);
        await inAttacher(`umount -l /spike/pods/${root}`);
        await kube.remove(ref("Pod", root, SPIKE_NAMESPACE));
        await until("the third pool pod gone", async () => ((await kube.get<V1Pod>(ref("Pod", root, SPIKE_NAMESPACE))) === null ? true : undefined), 120_000).catch(() => false);

        // G6: a directory quota, its usage reported, and a write past it refused, in a pod.
        said("G6", "quota set", await inAttacher('juicefs quota set "$META_URL" --path /spike/a --capacity 1 2>&1 | tail -3'));
        const quota = "pool-3";
        // Attached before its pod starts, as a restore would: is it seen?
        await inAttacher(`mkdir -p /spike/pods/${quota}/ws && chown 1000:1000 /spike/pods/${quota} /spike/pods/${quota}/ws && mount --bind /spike/jfs/spike/a/files /spike/pods/${quota}/ws`);
        await kube.apply(poolPod(quota, node, template, image));
        await until("the quota pod running", async () => (isReady(await kube.get<V1Pod>(ref("Pod", quota, SPIKE_NAMESPACE))) ? true : undefined));
        said("G1", "attached before its pod started, seen", (await inPool(quota, "cat /attach/ws/workspace/marker; ls -la /attach/ws")).out);
        // Attached again once it runs, over nothing it sees: as after a start.
        said("G1", "node mounts of the pod's directory", (await inAttacher(`grep pods/${quota} /proc/self/mountinfo || echo none`)).out);
        await inAttacher(`umount -l /spike/pods/${quota}/ws; mount --bind /spike/jfs/spike/a/files /spike/pods/${quota}/ws`);
        await until("the quota pod's workspace", async () => ((await inPool(quota, "cat /attach/ws/workspace/marker")).out === "a" ? true : undefined), 60_000).catch(() => undefined);
        const filled = Date.now();
        said("G6", "write of 1.2 GiB", await inPool(quota, "dd if=/dev/zero of=/attach/ws/workspace/fill bs=1M count=1229 conv=fsync 2>&1 | tail -2; ls -la /attach/ws/workspace/fill"));
        said("G6", "write took ms", Date.now() - filled);
        said("G6", "df in the pod", (await inPool(quota, "df -h /attach/ws")).out);
        said("G6", "quota get", (await inAttacher('juicefs quota get "$META_URL" --path /spike/a 2>&1 | tail -6')).out);
        await inPool(quota, "rm -f /attach/ws/workspace/fill");
        await new Promise((resolve) => setTimeout(resolve, 5000));
        said("G6", "quota get after removing it", (await inAttacher('juicefs quota get "$META_URL" --path /spike/a 2>&1 | tail -6')).out);
        await inAttacher(`umount -l /spike/pods/${quota}/ws`);
        await kube.remove(ref("Pod", quota, SPIKE_NAMESPACE));
        await until("the quota pod gone", async () => ((await kube.get<V1Pod>(ref("Pod", quota, SPIKE_NAMESPACE))) === null ? true : undefined), 120_000).catch(() => false);
      } finally {
        await inAttacher("for m in $(grep ' /spike/pods/' /proc/self/mountinfo | cut -d' ' -f5 | sort -r); do umount -l $m; done; umount -l /spike/jfs; rm -rf /spike/pods").catch(() => undefined);
        await kube.remove(ref("Pod", ATTACHER, NAMESPACE)).catch(() => undefined);
        await kube.remove(ref("Namespace", SPIKE_NAMESPACE)).catch(() => undefined);
      }
    });
  });
}
