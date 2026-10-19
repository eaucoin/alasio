/**
 * Grafana (neon/grafana): dashboards and alert rules on the lake, which it reads through
 * the lake's query endpoint, alerting through alasio's Telegram bot. It runs beside the
 * lake, holds nothing but in its database in Neon, which alasio makes (src/neon/
 * grafana.ts), and is reached by nothing but `alasio grafana`'s port-forward
 * (./network-policies.ts).
 *
 * Of one replica, rolled out by stopping it before its successor starts, so no two, not
 * of one cluster, both send an alert; of more, a pod at a time, the new ready before the
 * old stops. Replicas starting together take turns at its migrations, which hold a
 * session's advisory lock, which Neon's proxy, which it connects to main's compute
 * through, keeps, as it passes the session through whole. Of more
 * than one replica (grafana.scaling),
 * unified alerting is highly available: they find each other through a headless Service
 * and gossip on GOSSIP_PORT, one of them evaluates the rules and shares what it finds, and
 * their Alertmanagers, gossiping, send each notification once. A scale of min and max
 * apart is KEDA's to move on its CPU. It starts once it can log in to its database, which
 * alasio lets it once the database is made, rather than failing and being restarted until
 * then.
 * Its settings and what it is provisioned with are its image's; what is the
 * installation's is given here, from the Secret the stack's setup makes (`alasio-grafana`)
 * and the bot's.
 */
import type { KubernetesObject, V1Deployment, V1EnvVar, V1Service } from "@kubernetes/client-node";

import { computeId, MAIN } from "../../../neon/control/branches.ts";
import { GRAFANA_ROLE } from "../../../neon/control/secrets.ts";
import { imageReference } from "../images.ts";
import {
  componentName,
  grafanaRuns,
  helperResources,
  imagePullSecrets,
  labels,
  NAMESPACE,
  replicasOf,
  restrictedContainer,
  restrictedPod,
  scaled,
  script,
  selectorLabels,
} from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { replicaBudget, replicated, spread } from "./disruption.ts";
import { LAKE_QUERY_PORT } from "./lake.ts";
import { PROXY_HOST, PROXY_PORT } from "./neon.ts";
import { priority } from "./priorities.ts";
import { cpuScaledObject } from "./scalers.ts";
import { verticalPodAutoscaler } from "./sizing.ts";

/** Grafana's name, its Secret's, and its pods' component. */
export const GRAFANA = componentName("grafana");
/** The port it serves on. */
export const GRAFANA_PORT = 3000;
/** The user its image runs as. */
const GRAFANA_USER = 472;

/**
 * What its database's URL asks of the connection, beside the server: main's compute, named
 * to Neon's proxy; no TLS, which the proxy is not given; and the patience a wake takes.
 */
const DATABASE_QUERY = new URLSearchParams({ options: `endpoint=${computeId(MAIN)}`, sslmode: "disable", connect_timeout: "60" }).toString();
/** The port its replicas gossip on, over TCP and UDP, as unified alerting's peers. */
export const GOSSIP_PORT = 9094;

/** What a replica is told of its peers, where there may be several: where to find them, how to be found, and that one evaluates. */
function haEnv(): V1EnvVar[] {
  return [
    { name: "POD_IP", valueFrom: { fieldRef: { fieldPath: "status.podIP" } } },
    { name: "GF_UNIFIED_ALERTING_HA_PEERS", value: `${GRAFANA}-peers.${NAMESPACE}.svc.cluster.local:${GOSSIP_PORT}` },
    { name: "GF_UNIFIED_ALERTING_HA_ADVERTISE_ADDRESS", value: `$(POD_IP):${GOSSIP_PORT}` },
    { name: "GF_UNIFIED_ALERTING_HA_SINGLE_NODE_EVALUATION", value: "true" },
    // Short, as a replica gone is not coming back at its address.
    { name: "GF_UNIFIED_ALERTING_HA_RECONNECT_TIMEOUT", value: "2m" },
  ];
}

/** Grafana's objects, when it runs. */
export function grafanaObjects(config: InstallConfig): KubernetesObject[] {
  if (!grafanaRuns(config)) return [];
  const metadata = { name: GRAFANA, namespace: NAMESPACE, labels: labels("grafana") };
  const telegram = config.alasio.telegram.existingSecret;
  const { scaling } = config.grafana;
  const ha = replicated(scaling);
  // Where its replicas find each other: every one of them, ready or not.
  const peers: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata: { ...metadata, name: `${GRAFANA}-peers` },
    spec: {
      clusterIP: "None",
      publishNotReadyAddresses: true,
      selector: selectorLabels("grafana"),
      ports: [{ name: "gossip-tcp", port: GOSSIP_PORT, protocol: "TCP" }, { name: "gossip-udp", port: GOSSIP_PORT, protocol: "UDP" }],
    },
  };
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
      ...replicasOf(scaling),
      strategy: replicated(scaling) ? { type: "RollingUpdate", rollingUpdate: { maxSurge: 1, maxUnavailable: 0 } } : { type: "Recreate" },
      selector: { matchLabels: selectorLabels("grafana") },
      template: {
        metadata: { labels: labels("grafana") },
        spec: {
          automountServiceAccountToken: false,
          enableServiceLinks: false,
          ...priority("floor"),
          ...spread("grafana", scaling),
          securityContext: restrictedPod(GRAFANA_USER, GRAFANA_USER),
          ...imagePullSecrets(config),
          initContainers: [{
            name: "wait-for-database",
            image: imageReference(config.neon.computeImage),
            imagePullPolicy: "IfNotPresent",
            command: ["/bin/sh", "-c", script('until psql --no-psqlrc --quiet --command="select 1" >/dev/null 2>&1; do sleep 2; done')],
            env: [
              { name: "PGHOST", value: PROXY_HOST },
              { name: "PGPORT", value: String(PROXY_PORT) },
              { name: "PGOPTIONS", value: `endpoint=${computeId(MAIN)}` },
              { name: "PGDATABASE", value: "grafana" },
              { name: "PGUSER", value: GRAFANA_ROLE },
              { name: "PGPASSWORD", valueFrom: { secretKeyRef: { name: GRAFANA, key: "GF_DATABASE_PASSWORD" } } },
              // Long enough for the proxy to wake the compute.
              { name: "PGCONNECT_TIMEOUT", value: "60" },
            ],
            securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
            ...helperResources(),
          }],
          containers: [{
            name: "grafana",
            image: imageReference(config.images.grafana),
            imagePullPolicy: config.images.pullPolicy,
            env: [
              // Its database, on main's compute through Neon's proxy, its password the Secret's
              // (envFrom, which this may name), which the stack's setup makes URL-safe.
              { name: "GF_DATABASE_URL", value: `postgres://${GRAFANA_ROLE}:$(GF_DATABASE_PASSWORD)@${PROXY_HOST}:${PROXY_PORT}/grafana?${DATABASE_QUERY}` },
              { name: "LAKE_QUERY_URL", value: `http://${componentName("lake")}:${LAKE_QUERY_PORT}` },
              { name: "TELEGRAM_BOT_TOKEN", valueFrom: { secretKeyRef: { name: telegram, key: "token" } } },
              { name: "TELEGRAM_ALLOWED_USER_IDS", valueFrom: { secretKeyRef: { name: telegram, key: "allowedUserIds" } } },
              ...(ha ? haEnv() : []),
            ],
            // Its database's password, its admin's, its secret key, and the lake's query token.
            envFrom: [{ secretRef: { name: GRAFANA } }],
            ports: [
              { name: "http", containerPort: GRAFANA_PORT },
              ...(ha ? [{ name: "gossip-tcp", containerPort: GOSSIP_PORT, protocol: "TCP" }, { name: "gossip-udp", containerPort: GOSSIP_PORT, protocol: "UDP" }] : []),
            ],
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
  return [
    service,
    ...(ha ? [peers] : []),
    deployment,
    // Its start (its plugins loaded, its database migrated, its alert rules first evaluated)
    // keeps its CPU above 80 % of its request for up to two minutes or so.
    ...cpuScaledObject(deployment, scaling, { percent: 80, startUpSeconds: 180 }),
    ...replicaBudget("grafana", NAMESPACE, scaling),
    verticalPodAutoscaler(deployment, "InPlace", scaled(scaling) ? ["memory"] : undefined),
  ];
}
