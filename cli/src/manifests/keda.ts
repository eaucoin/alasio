/**
 * KEDA (kedacore/keda v2.21.0), the one scaler of alasio's components that scale: what a
 * component's ScaledObject, beside its workload, declares it scales on, KEDA reads and
 * moves its replicas by, between its scale's min and max, to zero where it may sleep.
 *
 * Its CRDs are as upstream's release manifest, keda-2.21.0.yaml (sha256
 * b43c89ffeef81722d7e2dd2c079d74789767a0f89cae1336cff784994814f6d7), has them, unchanged;
 * and its operator, its metrics API server and its admission webhooks as that manifest
 * installs them, with what alasio's other controllers have over upstream's:
 *
 * - in alasio's namespace, under alasio's names, which the operator is told, as it makes
 *   their certificate and puts its CA in the APIService and the webhooks; what it may
 *   write there is its certificate's Secret alone, and it reads no Secret elsewhere;
 * - restricted, on pinned images, under the floor's priority, with small requests;
 * - neither its HTTP add-on, as nothing of alasio's scales on HTTP requests, nor the
 *   aggregated roles that give the cluster's users its objects.
 *
 * A cluster has one KEDA, as the API server takes external metrics from one provider:
 * where the cluster has one already, autoscaling.keda.enabled false uses it (../kube/
 * autoscalers.ts).
 */
import type {
  KubernetesObject,
  V1ClusterRole,
  V1ClusterRoleBinding,
  V1Deployment,
  V1Role,
  V1RoleBinding,
  V1Service,
  V1ServiceAccount,
  V1ServicePort,
  V1ValidatingWebhookConfiguration,
  V1Volume,
  V1VolumeMount,
} from "@kubernetes/client-node";

import { imageReference } from "../images.ts";
import cloudEventSources from "./cloudeventsources.eventing.keda.sh.json" with { type: "json" };
import clusterCloudEventSources from "./clustercloudeventsources.eventing.keda.sh.json" with { type: "json" };
import clusterTriggerAuthentications from "./clustertriggerauthentications.keda.sh.json" with { type: "json" };
import { componentName, labels, NAMESPACE, selectorLabels } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { controllerDeployment, rule } from "./controllers.ts";
import scaledJobs from "./scaledjobs.keda.sh.json" with { type: "json" };
import scaledObjects from "./scaledobjects.keda.sh.json" with { type: "json" };
import triggerAuthentications from "./triggerauthentications.keda.sh.json" with { type: "json" };

/**
 * KEDA's CRDs, as the API server takes them, whose schemas' keys are not client-node's
 * model's. The operator watches every one of them, so all are installed, though alasio
 * makes ScaledObjects alone.
 */
export const KEDA_CRDS: readonly KubernetesObject[] = [scaledObjects, scaledJobs, triggerAuthentications, clusterTriggerAuthentications, cloudEventSources, clusterCloudEventSources];

/** The external metrics' APIService, which one provider serves for the whole cluster: KEDA's metrics API server, here. */
export const EXTERNAL_METRICS = "v1beta1.external.metrics.k8s.io";

const OPERATOR = componentName("keda-operator");
const METRICS_API_SERVER = componentName("keda-metrics-apiserver");
const WEBHOOKS = componentName("keda-admission-webhooks");
const WEBHOOK_CONFIGURATION = componentName("keda-admission");
/** The Secret of the certificate the operator makes for all three, and keeps renewed. */
const CERTIFICATES = componentName("keda-certs");
/** The identity all three run as, as upstream's do. */
const ACCOUNT = OPERATOR;

/** The volume of the certificates' Secret, mounted at /certs, as each of the three reads it; the operator starts before it exists. */
function certificates(optional: boolean): { readonly volume: V1Volume; readonly mount: V1VolumeMount } {
  return {
    volume: { name: "certificates", secret: { secretName: CERTIFICATES, ...(optional ? { optional: true } : {}) } },
    mount: { name: "certificates", mountPath: "/certs", readOnly: true },
  };
}

/** The variables each of the three is given, as upstream gives them: its namespace, every namespace watched. */
const KEDA_ENV = [
  { name: "POD_NAMESPACE", valueFrom: { fieldRef: { fieldPath: "metadata.namespace" } } },
  { name: "WATCH_NAMESPACE", value: "" },
];

/** The probes of the operator and the webhooks, on their health port. */
const HEALTH_PROBES = {
  livenessProbe: { httpGet: { path: "/healthz", port: 8081 }, initialDelaySeconds: 25 },
  readinessProbe: { httpGet: { path: "/readyz", port: 8081 }, initialDelaySeconds: 20 },
};

/** The operator, which scales what ScaledObjects name, and makes the three's certificate. */
function operator(config: InstallConfig): V1Deployment {
  const { operator } = config.autoscaling.keda;
  const certs = certificates(true);
  return controllerDeployment(config, "keda-operator", ACCOUNT, {
    name: "keda-operator",
    image: imageReference(operator.image),
    command: ["/keda"],
    args: [
      "--leader-elect",
      "--zap-log-level=info",
      "--zap-encoder=console",
      "--zap-time-encoding=rfc3339",
      "--enable-cert-rotation=true",
      `--cert-secret-name=${CERTIFICATES}`,
      `--operator-service-name=${OPERATOR}`,
      `--metrics-server-service-name=${METRICS_API_SERVER}`,
      `--webhooks-service-name=${WEBHOOKS}`,
      `--validating-webhook-name=${WEBHOOK_CONFIGURATION}`,
    ],
    env: [
      ...KEDA_ENV,
      { name: "WATCH_LABEL_SELECTOR", value: "" },
      { name: "WATCH_LABEL_SELECTOR_FOR_TRIGGERAUTH", value: "" },
      // The Secrets it reads are of its own namespace alone: alasio's ScaledObjects
      // authenticate to nothing, and another's that does reads its Secret from there,
      // through a ClusterTriggerAuthentication.
      { name: "KEDA_RESTRICT_SECRET_ACCESS", value: "true" },
    ],
    ports: [{ name: "http", containerPort: 8080 }, { name: "metricsservice", containerPort: 9666 }],
    ...HEALTH_PROBES,
    resources: operator.resources,
    volumeMounts: [certs.mount],
  }, [certs.volume]);
}

/** The metrics API server, which serves the API server the metrics of ScaledObjects' triggers, from the operator. */
function metricsApiServer(config: InstallConfig): V1Deployment {
  const { metricsApiServer } = config.autoscaling.keda;
  const certs = certificates(false);
  return controllerDeployment(config, "keda-metrics-apiserver", ACCOUNT, {
    name: "keda-metrics-apiserver",
    image: imageReference(metricsApiServer.image),
    command: ["/keda-adapter"],
    args: [
      "--secure-port=6443",
      "--zap-log-level=error",
      "--client-ca-file=/certs/ca.crt",
      "--tls-cert-file=/certs/tls.crt",
      "--tls-private-key-file=/certs/tls.key",
      "--cert-dir=/certs",
      `--metrics-service-address=${OPERATOR}.${NAMESPACE}.svc.cluster.local:9666`,
    ],
    env: KEDA_ENV,
    ports: [{ name: "https", containerPort: 6443 }, { name: "http", containerPort: 8080 }],
    livenessProbe: { httpGet: { path: "/healthz", port: 6443, scheme: "HTTPS" }, initialDelaySeconds: 5 },
    readinessProbe: { httpGet: { path: "/readyz", port: 6443, scheme: "HTTPS" }, initialDelaySeconds: 5 },
    resources: metricsApiServer.resources,
    volumeMounts: [{ name: "tmp", mountPath: "/tmp" }, certs.mount],
  }, [{ name: "tmp", emptyDir: {} }, certs.volume]);
}

/** The admission webhooks, which refuse a ScaledObject that cannot work, such as two on one workload. */
function admissionWebhooks(config: InstallConfig): V1Deployment {
  const { admissionWebhooks } = config.autoscaling.keda;
  const certs = certificates(false);
  return controllerDeployment(config, "keda-admission-webhooks", ACCOUNT, {
    name: "keda-admission-webhooks",
    image: imageReference(admissionWebhooks.image),
    command: ["/keda-admission-webhooks"],
    args: ["--zap-log-level=info", "--zap-encoder=console", "--zap-time-encoding=rfc3339", "--cert-dir=/certs"],
    env: KEDA_ENV,
    ports: [{ name: "https", containerPort: 9443 }, { name: "metrics", containerPort: 8080 }],
    ...HEALTH_PROBES,
    resources: admissionWebhooks.resources,
    volumeMounts: [certs.mount],
  }, [certs.volume]);
}

/** A Service of KEDA's, `name`, to `component`'s pods on `ports`. */
function service(name: string, component: string, ports: V1ServicePort[]): V1Service {
  return { apiVersion: "v1", kind: "Service", metadata: { name, namespace: NAMESPACE, labels: labels(component) }, spec: { selector: selectorLabels(component), ports } };
}

/**
 * Who the three are and what they may do: upstream's grants, but that in alasio's
 * namespace the operator writes its certificate's Secret and its leader's lease alone,
 * of the Secrets and leases there, and of the cluster's webhooks and APIServices its
 * own; that it writes no Job, as alasio makes no ScaledJob; and that of the objects
 * ScaledObjects scale it reads Deployments and StatefulSets alone, rather than any object
 * of the cluster's, Secrets among them: a cluster whose ScaledObjects scale another kind
 * (a custom resource), or whose ScaledJobs run, needs a KEDA of its own
 * (autoscaling.keda.enabled false).
 */
function identity(): KubernetesObject[] {
  const metadata = { name: OPERATOR, labels: labels("keda-operator") };
  const subjects = [{ kind: "ServiceAccount", name: ACCOUNT, namespace: NAMESPACE }];
  const serviceAccount: V1ServiceAccount = { apiVersion: "v1", kind: "ServiceAccount", metadata: { ...metadata, namespace: NAMESPACE } };
  const role: V1Role = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "Role",
    metadata: { ...metadata, namespace: NAMESPACE },
    rules: [
      rule([""], ["secrets"], ["create", "list", "watch"]),
      rule([""], ["secrets"], ["get", "patch", "update", "delete"], [CERTIFICATES]),
      rule(["coordination.k8s.io"], ["leases"], ["create", "list", "watch"]),
      rule(["coordination.k8s.io"], ["leases"], ["get", "patch", "update", "delete"], ["operator.keda.sh"]),
    ],
  };
  const clusterRole: V1ClusterRole = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "ClusterRole",
    metadata,
    rules: [
      rule([""], ["configmaps", "configmaps/status", "external", "pods", "services"], ["get", "list", "watch"]),
      rule([""], ["limitranges", "serviceaccounts"], ["list", "watch"]),
      rule(["", "events.k8s.io"], ["events"], ["create", "patch"]),
      rule(["*"], ["*/scale"], ["get", "list", "patch", "update", "watch"]),
      // Of the webhooks and APIServices it reads, it writes its own alone, the CA they trust.
      rule(["admissionregistration.k8s.io"], ["validatingwebhookconfigurations"], ["get", "list", "watch"]),
      rule(["admissionregistration.k8s.io"], ["validatingwebhookconfigurations"], ["patch", "update"], [WEBHOOK_CONFIGURATION]),
      rule(["apiregistration.k8s.io"], ["apiservices"], ["get", "list", "watch"]),
      rule(["apiregistration.k8s.io"], ["apiservices"], ["patch", "update"], [EXTERNAL_METRICS]),
      rule(["apps"], ["deployments", "statefulsets"], ["get", "list", "watch"]),
      rule(["apps"], ["replicasets"], ["list", "watch"]),
      rule(["autoscaling"], ["horizontalpodautoscalers"], ["create", "delete", "get", "list", "patch", "update", "watch"]),
      // ScaledJobs' Jobs, which alasio makes none of: read, as the operator watches them, never written.
      rule(["batch"], ["jobs"], ["get", "list", "watch"]),
      rule(["discovery.k8s.io"], ["endpointslices"], ["get", "list", "watch"]),
      rule(
        ["eventing.keda.sh"],
        ["cloudeventsources", "cloudeventsources/status", "clustercloudeventsources", "clustercloudeventsources/status"],
        ["get", "list", "patch", "update", "watch"],
      ),
      rule(
        ["keda.sh"],
        [
          "clustertriggerauthentications",
          "clustertriggerauthentications/status",
          "scaledjobs",
          "scaledjobs/finalizers",
          "scaledjobs/status",
          "scaledobjects",
          "scaledobjects/finalizers",
          "scaledobjects/status",
          "triggerauthentications",
          "triggerauthentications/status",
        ],
        ["get", "list", "patch", "update", "watch"],
      ),
    ],
  };
  const externalMetricsReader: V1ClusterRole = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "ClusterRole",
    metadata: { name: componentName("keda-external-metrics-reader"), labels: labels("keda-operator") },
    rules: [rule(["external.metrics.k8s.io"], ["*"], ["*"])],
  };
  const clusterBinding = (name: string, roleName: string, bound = subjects): V1ClusterRoleBinding => ({
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "ClusterRoleBinding",
    metadata: { name, labels: labels("keda-operator") },
    subjects: bound,
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "ClusterRole", name: roleName },
  });
  const roleBinding = (name: string, namespace: string, roleName: string): V1RoleBinding => ({
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "RoleBinding",
    metadata: { name, namespace, labels: labels("keda-operator") },
    subjects,
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: roleName },
  });
  return [
    serviceAccount,
    role,
    roleBinding(OPERATOR, NAMESPACE, OPERATOR),
    clusterRole,
    clusterBinding(OPERATOR, OPERATOR),
    externalMetricsReader,
    // The cluster's HPAs, which KEDA's are, read external metrics.
    clusterBinding(componentName("keda-hpa-controller-external-metrics"), externalMetricsReader.metadata?.name ?? "", [
      { kind: "ServiceAccount", name: "horizontal-pod-autoscaler", namespace: "kube-system" },
    ]),
    // The metrics API server authenticates the API server's calls as an extension API server does.
    clusterBinding(componentName("keda-system-auth-delegator"), "system:auth-delegator"),
    roleBinding(componentName("keda-auth-reader"), "kube-system", "extension-apiserver-authentication-reader"),
  ];
}

/** Where the API server reaches the metrics API server and the webhooks, which the operator gives the CA of. */
function registrations(): KubernetesObject[] {
  const apiService = {
    apiVersion: "apiregistration.k8s.io/v1",
    kind: "APIService",
    metadata: { name: EXTERNAL_METRICS, labels: labels("keda-metrics-apiserver") },
    spec: { group: "external.metrics.k8s.io", version: "v1beta1", groupPriorityMinimum: 100, versionPriority: 100, service: { name: METRICS_API_SERVER, namespace: NAMESPACE } },
  };
  // Its webhook of each kind as upstream's, which KEDA answers at its path; ignored while it does not answer.
  const webhook = (name: string, group: string, resource: string, path: string) => ({
    name,
    admissionReviewVersions: ["v1"],
    clientConfig: { service: { name: WEBHOOKS, namespace: NAMESPACE, path } },
    failurePolicy: "Ignore",
    matchPolicy: "Equivalent",
    namespaceSelector: {},
    objectSelector: {},
    rules: [{ apiGroups: [group], apiVersions: ["v1alpha1"], operations: ["CREATE", "UPDATE"], resources: [resource] }],
    sideEffects: "None",
    timeoutSeconds: 10,
  });
  const webhooks: V1ValidatingWebhookConfiguration = {
    apiVersion: "admissionregistration.k8s.io/v1",
    kind: "ValidatingWebhookConfiguration",
    metadata: { name: WEBHOOK_CONFIGURATION, labels: labels("keda-admission-webhooks") },
    webhooks: [
      webhook("vscaledobject.kb.io", "keda.sh", "scaledobjects", "/validate-keda-sh-v1alpha1-scaledobject"),
      webhook("vscaledjob.kb.io", "keda.sh", "scaledjobs", "/validate-keda-sh-v1alpha1-scaledjob"),
      webhook("vstriggerauthentication.kb.io", "keda.sh", "triggerauthentications", "/validate-keda-sh-v1alpha1-triggerauthentication"),
      webhook("vsclustertriggerauthentication.kb.io", "keda.sh", "clustertriggerauthentications", "/validate-keda-sh-v1alpha1-clustertriggerauthentication"),
      webhook("vcloudeventsource.kb.io", "eventing.keda.sh", "cloudeventsources", "/validate-eventing-keda-sh-v1alpha1-cloudeventsource"),
      webhook("vclustercloudeventsource.kb.io", "eventing.keda.sh", "clustercloudeventsources", "/validate-eventing-keda-sh-v1alpha1-clustercloudeventsource"),
    ],
  };
  return [apiService, webhooks];
}

/** KEDA's objects, unless the cluster has it already. */
export function kedaObjects(config: InstallConfig): KubernetesObject[] {
  if (!config.autoscaling.keda.enabled) return [];
  return [
    ...KEDA_CRDS,
    ...identity(),
    service(OPERATOR, "keda-operator", [{ name: "metricsservice", port: 9666, targetPort: 9666 }, { name: "metrics", port: 8080, targetPort: 8080 }]),
    service(METRICS_API_SERVER, "keda-metrics-apiserver", [{ name: "https", port: 443, targetPort: 6443 }, { name: "metrics", port: 8080, targetPort: 8080 }]),
    service(WEBHOOKS, "keda-admission-webhooks", [{ name: "https", port: 443, targetPort: 9443 }, { name: "metrics", port: 8080, targetPort: 8080 }]),
    ...registrations(),
    operator(config),
    metricsApiServer(config),
    admissionWebhooks(config),
  ];
}
