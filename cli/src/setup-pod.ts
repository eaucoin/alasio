/**
 * The pod of this release's alasio that makes its state alone, `alasio setup`
 * (src/setup/main.ts), as `alasio upgrade --setup-pod` prints it, for a cut-over to run before
 * the upgrade (upgrades/from-9.0.0.sh): alasio's own container of the release's Deployment, its
 * image, its environment and its Secrets, so what it makes is what this release's alasio would
 * make as it starts, run once to its end. It is labelled as alasio's pods are, which alone may
 * reach Neon's proxy (./manifests/network-policies.ts), but by no ReplicaSet's hash, so no
 * Deployment adopts it; it serves nothing, and mounts none of the machine's folders, which
 * making the state needs none of.
 *
 * And the pod of this release's stack setup (neon/control/kube-setup.ts), as
 * `alasio upgrade --setup-job` prints it, for a cut-over to run before that setup pod: its
 * neon-setup Job's pod, which makes what the stack's secrets lack, those this release adds among
 * them, and renders each service's Secret from them, idempotent and never rotating, as applying
 * alasio runs it before anything else starts; run once to its end, as the Job's is.
 */
import type { KubernetesObject, V1Deployment, V1Job, V1Pod } from "@kubernetes/client-node";

import { NAMESPACE, neonName, RELEASE } from "./manifests/common.ts";

/** The setup pod's name. */
export const SETUP_POD = `${RELEASE}-setup`;

/** The label the setup pod has beside alasio's, which tells it from alasio's own. */
export const SETUP_LABEL = "alasio.dev/setup";

/** The setup pod of the installation `objects` are, as the module says; failing where they have no alasio. */
export function setupPod(objects: readonly KubernetesObject[]): V1Pod {
  const deployment = objects.find((object): object is V1Deployment => object.kind === "Deployment" && object.metadata?.name === RELEASE && object.metadata.namespace === NAMESPACE);
  const template = deployment?.spec?.template;
  const container = template?.spec?.containers.find(({ name }) => name === "alasio");
  if (!template?.spec || !container) throw new Error(`the installation has no ${NAMESPACE}/${RELEASE} Deployment of an alasio container to make its setup pod of`);
  const { affinity: _affinity, terminationGracePeriodSeconds: _grace, volumes = [], ...spec } = template.spec;
  const machine = new Set(volumes.filter(({ hostPath }) => hostPath !== undefined).map(({ name }) => name));
  const { ports: _ports, readinessProbe: _readiness, livenessProbe: _liveness, startupProbe: _startup, lifecycle: _lifecycle, volumeMounts = [], ...served } = container;
  return {
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name: SETUP_POD, namespace: NAMESPACE, labels: { ...template.metadata?.labels, [SETUP_LABEL]: "true" } },
    spec: {
      ...spec,
      restartPolicy: "Never",
      volumes: volumes.filter(({ name }) => !machine.has(name)),
      containers: [{ ...served, args: ["node", "/opt/alasio/src/setup/main.ts"], volumeMounts: volumeMounts.filter(({ name }) => !machine.has(name)) }],
    },
  };
}

/** The stack setup's pod's name. */
export const STACK_SETUP_POD = `${RELEASE}-stack-setup`;

/** The stack setup's pod of the installation `objects` are, as the module says; failing where they have no neon-setup Job. */
export function stackSetupPod(objects: readonly KubernetesObject[]): V1Pod {
  const name = neonName("setup");
  const job = objects.find((object): object is V1Job => object.kind === "Job" && object.metadata?.name === name && object.metadata.namespace === NAMESPACE);
  const template = job?.spec?.template;
  if (!template?.spec) throw new Error(`the installation has no ${NAMESPACE}/${name} Job to make its stack setup's pod of`);
  return {
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name: STACK_SETUP_POD, namespace: NAMESPACE, labels: { ...template.metadata?.labels, [SETUP_LABEL]: "true" } },
    spec: { ...template.spec, restartPolicy: "Never" },
  };
}
