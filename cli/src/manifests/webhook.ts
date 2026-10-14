/**
 * The webhook intake (webhook/src/server.ts), the one thing of alasio's the internet
 * reaches: Telegram delivers each environment's bot's updates to it, over HTTPS on
 * webhook.port, and it writes those the environment's alasio would act on to the
 * environment's topic in the log (./automq.ts), as the principal `webhook`, which may only
 * write the Telegram topics. It holds no bot token and reaches nothing but the log.
 *
 * How it is reached its target says (webhook.exposure): on this machine's public address
 * alone, a host port bound to it, from k3s on the machine; on its node's addresses, from
 * the cluster in Docker; or through a LoadBalancer Service that takes Telegram's ranges
 * alone. Its port inside its pod is the public one, which an unprivileged process binds
 * as its pod lets it, so a firewall's rules say one port either side of the pod.
 *
 * Its routes, one an environment, are in the Secret alasio's command line writes as it
 * registers each bot (../webhook.ts), read as the kubelet updates it; its certificate is
 * the stack's setup's (neon/control/certificate.ts). Its health, its routes' environments
 * and its metrics are served inside the cluster on a port of their own.
 */
import type { KubernetesObject, V1ContainerPort, V1Deployment, V1Service } from "@kubernetes/client-node";

import { imageReference } from "../images.ts";
import { LOG_BROKERS, logPrincipalSecret } from "./automq.ts";
import { componentName, imagePullSecrets, labels, NAMESPACE, replicasOf, restrictedContainer, restrictedPod, selectorLabels } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { priority } from "./priorities.ts";
import { verticalPodAutoscaler } from "./sizing.ts";

/** Its name, its pods' component, and its Service's. */
export const WEBHOOK = componentName("webhook");
export const WEBHOOK_COMPONENT = "webhook";
/** The Secret of its routes, which alasio's command line writes, a key an environment. */
export const ROUTES_SECRET = componentName("webhook-routes");
/** The Secret of its certificate and key, which the stack's setup writes. */
export const TLS_SECRET = componentName("webhook-tls");
/** The port its health, its environments and its metrics are served on, inside the cluster. */
export const WEBHOOK_HEALTH_PORT = 8080;

/**
 * The addresses Telegram delivers webhooks from, as its Bot API's documentation lists
 * them; a change to them shows in each bot's getWebhookInfo as a last error, which alasio
 * status says.
 */
export const TELEGRAM_RANGES = ["149.154.160.0/20", "91.108.4.0/22"] as const;

/** The intake's user, distroless's nonroot. */
const NONROOT = 65532;

/** The intake's objects. */
export function webhookObjects(config: InstallConfig): KubernetesObject[] {
  const { webhook } = config;
  const metadata = { name: WEBHOOK, namespace: NAMESPACE, labels: labels(WEBHOOK_COMPONENT) };
  const https: V1ContainerPort = {
    name: "https",
    containerPort: webhook.port,
    ...(webhook.exposure === "hostAddress" ? { hostPort: webhook.port, hostIP: webhook.publicAddress } : {}),
    ...(webhook.exposure === "node" ? { hostPort: webhook.port } : {}),
  };
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata,
    spec: {
      ...replicasOf(webhook.scaling),
      // A host port is held by one pod at a time: the old one goes before the new one starts.
      ...(webhook.exposure === "loadBalancer" ? {} : { strategy: { type: "Recreate" } }),
      selector: { matchLabels: selectorLabels(WEBHOOK_COMPONENT) },
      template: {
        metadata: { labels: labels(WEBHOOK_COMPONENT) },
        spec: {
          ...priority("floor"),
          automountServiceAccountToken: false,
          ...imagePullSecrets(config),
          securityContext: {
            ...restrictedPod(NONROOT, NONROOT),
            // Its own port, which may be under 1024, bound as its user.
            sysctls: [{ name: "net.ipv4.ip_unprivileged_port_start", value: "0" }],
          },
          ...(Object.keys(config.neon.nodeSelector).length > 0 ? { nodeSelector: { ...config.neon.nodeSelector } } : {}),
          ...(config.neon.tolerations.length > 0 ? { tolerations: [...config.neon.tolerations] } : {}),
          terminationGracePeriodSeconds: 30,
          containers: [{
            name: "webhook",
            image: imageReference(config.images.webhook),
            imagePullPolicy: config.images.pullPolicy,
            env: [
              { name: "WEBHOOK_PORT", value: String(webhook.port) },
              { name: "HEALTH_PORT", value: String(WEBHOOK_HEALTH_PORT) },
              { name: "WEBHOOK_TLS", value: "/etc/webhook/tls" },
              { name: "WEBHOOK_ROUTES", value: "/etc/webhook/routes" },
              { name: "LOG_BROKERS", value: LOG_BROKERS },
              { name: "LOG_USERNAME", valueFrom: { secretKeyRef: { name: logPrincipalSecret("webhook"), key: "username" } } },
              { name: "LOG_PASSWORD", valueFrom: { secretKeyRef: { name: logPrincipalSecret("webhook"), key: "password" } } },
            ],
            ports: [https, { name: "health", containerPort: WEBHOOK_HEALTH_PORT }],
            readinessProbe: { httpGet: { path: "/readyz", port: "health" }, periodSeconds: 5 },
            livenessProbe: { httpGet: { path: "/healthz", port: "health" }, periodSeconds: 20, failureThreshold: 3 },
            securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
            resources: webhook.resources,
            volumeMounts: [{ name: "tls", mountPath: "/etc/webhook/tls", readOnly: true }, { name: "routes", mountPath: "/etc/webhook/routes", readOnly: true }],
          }],
          volumes: [
            { name: "tls", secret: { secretName: TLS_SECRET } },
            // None until alasio's command line registers a bot: the intake serves no route until then.
            { name: "routes", secret: { secretName: ROUTES_SECRET, optional: true } },
          ],
        },
      },
    },
  };
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata,
    spec: { selector: selectorLabels(WEBHOOK_COMPONENT), ports: [{ name: "health", port: WEBHOOK_HEALTH_PORT, targetPort: "health" }] },
  };
  const exposed: V1Service[] = webhook.exposure === "loadBalancer"
    ? [{
      apiVersion: "v1",
      kind: "Service",
      metadata: { ...metadata, name: componentName("webhook-public") },
      spec: {
        type: "LoadBalancer",
        selector: selectorLabels(WEBHOOK_COMPONENT),
        ports: [{ name: "https", port: webhook.port, targetPort: "https" }],
        loadBalancerSourceRanges: [...TELEGRAM_RANGES],
        // The source addresses kept to the pod, so the ranges hold at the node too.
        externalTrafficPolicy: "Local",
      },
    }]
    : [];
  return [service, ...exposed, deployment, verticalPodAutoscaler(deployment, "InPlace")];
}
