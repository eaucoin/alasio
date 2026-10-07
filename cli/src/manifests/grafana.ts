/**
 * Grafana (neon/grafana): dashboards and alert rules on the lake, which it reads through
 * the lake's query endpoint, alerting through alasio's Telegram bot. It runs beside the
 * lake, holds nothing but in its database in Neon, which alasio makes (src/neon/
 * grafana.ts), and is reached by nothing but `alasio grafana`'s port-forward
 * (./network-policies.ts).
 *
 * One replica, replaced rather than rolled: its migrations take a session's advisory
 * lock, which is why it connects to the compute directly. Its settings and what it is
 * provisioned with are its image's; what is the installation's is given here, from the
 * Secret the stack's setup makes (`alasio-grafana`) and the bot's.
 */
import type { KubernetesObject, V1Deployment, V1Service } from "@kubernetes/client-node";

import { imageReference } from "../images.ts";
import { componentName, grafanaRuns, imagePullSecrets, labels, NAMESPACE, neonName, restrictedPod, restrictedContainer, selectorLabels } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { LAKE_QUERY_PORT } from "./lake.ts";

/** Grafana's name, its Secret's, and its pods' component. */
export const GRAFANA = componentName("grafana");
/** The port it serves on. */
export const GRAFANA_PORT = 3000;
/** The user its image runs as. */
const GRAFANA_USER = 472;

/** Grafana's objects, when it runs. */
export function grafanaObjects(config: InstallConfig): KubernetesObject[] {
  if (!grafanaRuns(config)) return [];
  const metadata = { name: GRAFANA, namespace: NAMESPACE, labels: labels("grafana") };
  const telegram = config.alasio.telegram.existingSecret;
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata,
    spec: { selector: selectorLabels("grafana"), ports: [{ name: "http", port: GRAFANA_PORT, targetPort: "http" }] },
  };
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata,
    spec: {
      replicas: 1,
      strategy: { type: "Recreate" },
      selector: { matchLabels: selectorLabels("grafana") },
      template: {
        metadata: { labels: labels("grafana") },
        spec: {
          automountServiceAccountToken: false,
          enableServiceLinks: false,
          securityContext: restrictedPod(GRAFANA_USER, GRAFANA_USER),
          ...imagePullSecrets(config),
          containers: [{
            name: "grafana",
            image: imageReference(config.images.grafana),
            imagePullPolicy: config.images.pullPolicy,
            env: [
              { name: "GF_DATABASE_HOST", value: `${neonName("compute")}:55433` },
              { name: "LAKE_QUERY_URL", value: `http://${componentName("lake")}:${LAKE_QUERY_PORT}` },
              { name: "TELEGRAM_BOT_TOKEN", valueFrom: { secretKeyRef: { name: telegram, key: "token" } } },
              { name: "TELEGRAM_ALLOWED_USER_IDS", valueFrom: { secretKeyRef: { name: telegram, key: "allowedUserIds" } } },
            ],
            // Its database's password, its admin's, its secret key, and the lake's query token.
            envFrom: [{ secretRef: { name: GRAFANA } }],
            ports: [{ name: "http", containerPort: GRAFANA_PORT }],
            readinessProbe: { httpGet: { path: "/api/health", port: "http" }, periodSeconds: 10 },
            // Its first start migrates its database, which takes a minute or so.
            livenessProbe: { httpGet: { path: "/api/health", port: "http" }, initialDelaySeconds: 180, periodSeconds: 30, failureThreshold: 3 },
            securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
            resources: config.grafana.resources,
            volumeMounts: [{ name: "data", mountPath: "/var/lib/grafana" }, { name: "tmp", mountPath: "/tmp" }],
          }],
          // Its plugins' sockets and caches, and its provisioning as start.sh completes it: nothing it keeps.
          volumes: [{ name: "data", emptyDir: { sizeLimit: "1Gi" } }, { name: "tmp", emptyDir: { sizeLimit: "1Gi" } }],
        },
      },
    },
  };
  return [service, deployment];
}
