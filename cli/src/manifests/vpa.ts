/**
 * The Vertical Pod Autoscaler (kubernetes/autoscaler vertical-pod-autoscaler-1.8.0), which
 * sizes alasio's workloads' requests from what they use: its recommender, which
 * recommends each workload's requests; its updater, which applies them to running pods
 * in place, or recreates them, as each workload's VerticalPodAutoscaler allows (alasio's,
 * in place alone: ./sizing.ts); and its
 * admission controller, which gives a new pod its recommendation.
 *
 * Its CRDs are as upstream's deploy/vpa-v1-crd-gen.yaml at that tag (sha256
 * 2916e9b8e0699375ed8eca8af81e00c5a8cf9d68a70b4796e24d43794838d623) has them, unchanged; its
 * controllers and what they may do as deploy/ installs them, with what alasio's other
 * controllers have over upstream's (./controllers.ts), and:
 *
 * - its admission controller's certificate made, and its webhook's CA patched in, by
 *   kube-webhook-certgen, as its chart (vertical-pod-autoscaler-chart-0.13.0) does, and
 *   its webhook registered by alasio rather than by itself, so it may change no webhook;
 *   the webhook sees alasio's pods alone, and is ignored while it does not answer;
 * - the recommender tracks the pods of VerticalPodAutoscalers alone, and recommends as
 *   little as alasio's smallest components use rather than 250 MB a pod;
 * - the updater resizes a workload of one replica, as most of alasio's are, and it and the
 *   admission controller take the InPlace update mode, alpha in this release, which
 *   resizes a pod in place and never recreates it;
 * - requests a tenth of upstream's 1.2 GiB.
 *
 * A cluster has one VPA: where the cluster has one already, autoscaling.vpa.enabled false
 * uses it (../kube/autoscalers.ts).
 */
import type {
  KubernetesObject,
  V1ClusterRole,
  V1ClusterRoleBinding,
  V1Job,
  V1MutatingWebhookConfiguration,
  V1PolicyRule,
  V1Role,
  V1RoleBinding,
  V1Service,
  V1ServiceAccount,
} from "@kubernetes/client-node";

import { imageReference } from "../images.ts";
import { componentName, helperResources, imagePullSecrets, labels, NAMESPACE, restrictedContainer, restrictedPod, selectorLabels } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { controllerDeployment, rule } from "./controllers.ts";
import { priority } from "./priorities.ts";
import checkpoints from "./verticalpodautoscalercheckpoints.autoscaling.k8s.io.json" with { type: "json" };
import verticalPodAutoscalers from "./verticalpodautoscalers.autoscaling.k8s.io.json" with { type: "json" };

/** VPA's CRDs, as the API server takes them. */
export const VPA_CRDS: readonly KubernetesObject[] = [verticalPodAutoscalers, checkpoints];

const RECOMMENDER = componentName("vpa-recommender");
const UPDATER = componentName("vpa-updater");
const ADMISSION_CONTROLLER = componentName("vpa-admission-controller");
const CERTGEN = componentName("vpa-certgen");
/** The admission controller's Service, which its webhook and certificate name. */
const WEBHOOK_SERVICE = componentName("vpa-webhook");
const WEBHOOK_CONFIGURATION = componentName("vpa-admission");
/** The Secret of the admission controller's certificate, with the CA the webhook trusts. */
const TLS = componentName("vpa-tls");
/** The lease the admission controller says it runs by, which the updater reads before it recreates a pod. */
const STATUS_LEASE = "vpa-admission-controller";

/** The feature gate of the InPlace update mode, which the updater and the admission controller take. */
const IN_PLACE = "--feature-gates=InPlace=true";

/** Who `name` is, and what the cluster lets it do: `rules`. */
function identity(name: string, component: string, rules: V1PolicyRule[]): KubernetesObject[] {
  const metadata = { name, labels: labels(component) };
  const serviceAccount: V1ServiceAccount = { apiVersion: "v1", kind: "ServiceAccount", metadata: { ...metadata, namespace: NAMESPACE } };
  const clusterRole: V1ClusterRole = { apiVersion: "rbac.authorization.k8s.io/v1", kind: "ClusterRole", metadata, rules };
  const binding: V1ClusterRoleBinding = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "ClusterRoleBinding",
    metadata,
    subjects: [{ kind: "ServiceAccount", name, namespace: NAMESPACE }],
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "ClusterRole", name },
  };
  return [serviceAccount, clusterRole, binding];
}

/** What `name` may do in alasio's namespace alone: `rules`. */
function namespaced(name: string, component: string, rules: V1PolicyRule[]): KubernetesObject[] {
  const metadata = { name, namespace: NAMESPACE, labels: labels(component) };
  const role: V1Role = { apiVersion: "rbac.authorization.k8s.io/v1", kind: "Role", metadata, rules };
  const binding: V1RoleBinding = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "RoleBinding",
    metadata,
    subjects: [{ kind: "ServiceAccount", name, namespace: NAMESPACE }],
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name },
  };
  return [role, binding];
}

/** What all three read of the workloads VerticalPodAutoscalers name: their scale and their controllers. */
const TARGET_READER: V1PolicyRule[] = [
  rule(["*"], ["*/scale"], ["get", "watch"]),
  rule([""], ["replicationcontrollers"], ["get", "list", "watch"]),
  rule(["apps"], ["daemonsets", "deployments", "replicasets", "statefulsets"], ["get", "list", "watch"]),
  rule(["batch"], ["jobs", "cronjobs"], ["get", "list", "watch"]),
];

/** What the recommender and the updater read and say of pods and VerticalPodAutoscalers, as upstream's vpa-actor. */
const ACTOR: V1PolicyRule[] = [
  rule([""], ["pods", "nodes", "limitranges"], ["get", "list", "watch"]),
  rule(["", "events.k8s.io"], ["events"], ["create", "get", "list", "watch", "patch", "update"]),
  rule(["autoscaling.k8s.io"], ["verticalpodautoscalers"], ["get", "list", "watch"]),
];

/** The recommender: each workload's recommended requests, from its pods' use, which the metrics server measures, kept in checkpoints. */
function recommender(config: InstallConfig): KubernetesObject[] {
  const { recommender } = config.autoscaling.vpa;
  return [
    ...identity(RECOMMENDER, "vpa-recommender", [
      ...ACTOR,
      ...TARGET_READER,
      rule(["metrics.k8s.io"], ["pods"], ["get", "list"]),
      rule(["autoscaling.k8s.io"], ["verticalpodautoscalers/status"], ["get", "patch"]),
      rule(["autoscaling.k8s.io"], ["verticalpodautoscalercheckpoints"], ["get", "list", "watch", "create", "patch", "delete"]),
    ]),
    controllerDeployment(config, "vpa-recommender", RECOMMENDER, {
      name: "recommender",
      image: imageReference(recommender.image),
      args: ["--memory-saver", "--pod-recommendation-min-cpu-millicores=10", "--pod-recommendation-min-memory-mb=16"],
      ports: [{ name: "prometheus", containerPort: 8942 }],
      livenessProbe: { httpGet: { path: "/health-check", port: "prometheus" }, initialDelaySeconds: 5, periodSeconds: 10, failureThreshold: 3 },
      readinessProbe: { httpGet: { path: "/health-check", port: "prometheus" }, periodSeconds: 10, failureThreshold: 3 },
      resources: recommender.resources,
    }),
  ];
}

/** The updater: a running pod whose requests are off its recommendation resized in place, or recreated, as its VerticalPodAutoscaler allows. */
function updater(config: InstallConfig): KubernetesObject[] {
  const { updater } = config.autoscaling.vpa;
  return [
    ...identity(UPDATER, "vpa-updater", [
      ...ACTOR,
      ...TARGET_READER,
      rule(["apps"], ["replicasets"], ["get"]),
      rule([""], ["pods/eviction"], ["create"]),
      // A resize in place, and the annotation that says one was made.
      rule([""], ["pods/resize", "pods"], ["patch"]),
    ]),
    ...namespaced(UPDATER, "vpa-updater", [rule(["coordination.k8s.io"], ["leases"], ["get", "list", "watch"])]),
    controllerDeployment(config, "vpa-updater", UPDATER, {
      name: "updater",
      image: imageReference(updater.image),
      args: ["--min-replicas=1", IN_PLACE],
      env: [{ name: "NAMESPACE", valueFrom: { fieldRef: { fieldPath: "metadata.namespace" } } }],
      ports: [{ name: "prometheus", containerPort: 8943 }],
      livenessProbe: { httpGet: { path: "/health-check", port: "prometheus" }, initialDelaySeconds: 5, periodSeconds: 10, failureThreshold: 3 },
      readinessProbe: { httpGet: { path: "/health-check", port: "prometheus" }, periodSeconds: 10, failureThreshold: 3 },
      resources: updater.resources,
    }),
  ];
}

/**
 * The admission controller, which gives a pod its recommendation as it is made: its
 * Service, its webhook, whose CA the certgen Job patches in, and its Deployment, which
 * serves the certificate that Job makes.
 */
function admissionController(config: InstallConfig): KubernetesObject[] {
  const { admissionController } = config.autoscaling.vpa;
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name: WEBHOOK_SERVICE, namespace: NAMESPACE, labels: labels("vpa-admission-controller") },
    spec: { selector: selectorLabels("vpa-admission-controller"), ports: [{ port: 443, targetPort: 8000 }] },
  };
  const webhook: V1MutatingWebhookConfiguration = {
    apiVersion: "admissionregistration.k8s.io/v1",
    kind: "MutatingWebhookConfiguration",
    metadata: { name: WEBHOOK_CONFIGURATION, labels: labels("vpa-admission-controller") },
    webhooks: [{
      name: "vpa.k8s.io",
      admissionReviewVersions: ["v1"],
      clientConfig: { service: { name: WEBHOOK_SERVICE, namespace: NAMESPACE, port: 443 } },
      failurePolicy: "Ignore",
      matchPolicy: "Equivalent",
      namespaceSelector: {},
      // alasio's pods, and its VerticalPodAutoscalers, alone.
      objectSelector: { matchLabels: { "app.kubernetes.io/part-of": "alasio" } },
      reinvocationPolicy: "Never",
      rules: [
        { apiGroups: [""], apiVersions: ["v1"], operations: ["CREATE"], resources: ["pods"], scope: "*" },
        { apiGroups: ["autoscaling.k8s.io"], apiVersions: ["*"], operations: ["CREATE", "UPDATE"], resources: ["verticalpodautoscalers"], scope: "*" },
      ],
      sideEffects: "None",
      timeoutSeconds: 5,
    }],
  };
  return [
    ...identity(ADMISSION_CONTROLLER, "vpa-admission-controller", [
      rule([""], ["pods", "configmaps", "nodes", "limitranges"], ["get", "list", "watch"]),
      rule(["autoscaling.k8s.io"], ["verticalpodautoscalers"], ["get", "list", "watch"]),
      ...TARGET_READER,
    ]),
    ...namespaced(ADMISSION_CONTROLLER, "vpa-admission-controller", [
      rule(["coordination.k8s.io"], ["leases"], ["create", "list", "watch"]),
      rule(["coordination.k8s.io"], ["leases"], ["get", "update"], [STATUS_LEASE]),
    ]),
    service,
    webhook,
    controllerDeployment(
      config,
      "vpa-admission-controller",
      ADMISSION_CONTROLLER,
      {
        name: "admission-controller",
        image: imageReference(admissionController.image),
        args: ["--register-webhook=false", `--webhook-service=${WEBHOOK_SERVICE}`, IN_PLACE],
        env: [{ name: "NAMESPACE", valueFrom: { fieldRef: { fieldPath: "metadata.namespace" } } }],
        ports: [{ name: "webhook", containerPort: 8000 }, { name: "prometheus", containerPort: 8944 }],
        livenessProbe: { httpGet: { path: "/health-check", port: "prometheus" }, initialDelaySeconds: 5, periodSeconds: 10, failureThreshold: 3 },
        readinessProbe: { httpGet: { path: "/health-check", port: "prometheus" }, periodSeconds: 10, failureThreshold: 3 },
        resources: admissionController.resources,
        volumeMounts: [{ name: "tls-certs", mountPath: "/etc/tls-certs", readOnly: true }],
      },
      [{
        name: "tls-certs",
        secret: { secretName: TLS, items: [{ key: "ca", path: "caCert.pem" }, { key: "cert", path: "serverCert.pem" }, { key: "key", path: "serverKey.pem" }] },
      }],
    ),
  ];
}

/**
 * The admission controller's certificate, made once and kept, for its Service's names, and
 * its webhook given the certificate's CA, by kube-webhook-certgen: a Job, which applying
 * alasio runs to completion before the controller starts (../kube/apply.ts).
 */
function certgen(config: InstallConfig): KubernetesObject[] {
  const image = imageReference(config.autoscaling.vpa.certgenImage);
  const step = (name: string, args: string[]) => ({
    name,
    image,
    imagePullPolicy: "IfNotPresent",
    args,
    securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
    ...helperResources(),
  });
  const job: V1Job = {
    apiVersion: "batch/v1",
    kind: "Job",
    metadata: { name: CERTGEN, namespace: NAMESPACE, labels: labels("vpa-certgen") },
    spec: {
      backoffLimit: 3,
      activeDeadlineSeconds: 300,
      template: {
        metadata: { labels: labels("vpa-certgen") },
        spec: {
          serviceAccountName: CERTGEN,
          restartPolicy: "Never",
          ...priority("component"),
          securityContext: restrictedPod(65532, 65532),
          ...imagePullSecrets(config),
          initContainers: [step("create", ["create", `--host=${WEBHOOK_SERVICE},${WEBHOOK_SERVICE}.${NAMESPACE}.svc`, `--namespace=${NAMESPACE}`, `--secret-name=${TLS}`])],
          containers: [step("patch", ["patch", `--webhook-name=${WEBHOOK_CONFIGURATION}`, `--namespace=${NAMESPACE}`, `--secret-name=${TLS}`, "--patch-validating=false"])],
        },
      },
    },
  };
  return [
    ...identity(CERTGEN, "vpa-certgen", [rule(["admissionregistration.k8s.io"], ["mutatingwebhookconfigurations"], ["get", "update", "patch"], [WEBHOOK_CONFIGURATION])]),
    ...namespaced(CERTGEN, "vpa-certgen", [rule([""], ["secrets"], ["create"]), rule([""], ["secrets"], ["get"], [TLS])]),
    job,
  ];
}

/** VPA's objects, unless the cluster has it already. */
export function vpaObjects(config: InstallConfig): KubernetesObject[] {
  if (!config.autoscaling.vpa.enabled) return [];
  return [...VPA_CRDS, ...recommender(config), ...updater(config), ...admissionController(config), ...certgen(config)];
}
