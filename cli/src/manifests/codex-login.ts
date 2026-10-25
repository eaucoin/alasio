/**
 * An environment's relay of its Codex login to its harness pools' pods (src/codex/relay.ts), a
 * component of its own, from alasio's image, so a bound Codex harness goes on calling the model
 * while its environment's alasio sleeps:
 *
 * - its Deployment, of alasio.codexLogin.scaling's replicas, as a role of its own on the
 *   environment's database, which reaches the login there alone, with the key alasio signs its
 *   Codex turns' tokens for the relay by, and none of the caller key it is made of, which
 *   signs the environment's sessions' tokens. Its Codex home is its own,
 *   written from the store and kept there, unless the host profile gives alasio the operator's
 *   home, whose `~/.codex` is then the relay's too, as the operator;
 * - KEDA's scaling of it on the gateway's count of the environment's Codex harnesses bound, of
 *   its session conversations' pool and, main's, of its folder conversations': up from none at
 *   the first, back to its min once none has been bound for alasio.cooldownSeconds;
 * - its Service, which the pools' pods' Codex reaches (ALASIO_CODEX_LOGIN_URL).
 */
import type { KubernetesObject, V1Deployment, V1Service, V1Volume, V1VolumeMount } from "@kubernetes/client-node";
import { posix } from "node:path";

import { CODEX_LOGIN_DRAIN_SECONDS, CODEX_LOGIN_KEY_VARIABLE, CODEX_LOGIN_PORT } from "../../../src/harness/names.ts";
import { imageReference } from "../images.ts";
import {
  CODEX_LOGIN_DATABASE_SECRET,
  CODEX_LOGIN_KEY_SECRET,
  componentName,
  type Environment,
  envOf,
  given,
  imagePullSecrets,
  labels,
  NAMESPACE,
  otelEnv,
  replicasOf,
  restrictedContainer,
  restrictedPod,
  selectorLabels,
} from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { foldersRun } from "./folders.ts";
import { FOLDER_HARNESS_POOLS, HARNESS_POOLS } from "./harness-pools.ts";
import { priority } from "./priorities.ts";
import { verticalPodAutoscaler } from "./sizing.ts";
import { GATEWAY, GATEWAY_PORT } from "./sandbox-gateway.ts";
import { scaledObject, type Trigger } from "./scalers.ts";

/** The relay's name, its Service's, which the pools' pods reach it by, and its pods' component. */
export const CODEX_LOGIN = componentName("codex-login");
export const CODEX_LOGIN_COMPONENT = "codex-login";

/** Where `environment`'s harness pools' pods' Codex reaches its relay of its Codex login: the Service's model API. */
export const codexLoginUrl = (environment: Environment): string => `http://${CODEX_LOGIN}.${environment.namespace}.svc.cluster.local:${CODEX_LOGIN_PORT}/v1`;

/** What a bound Codex harness asks of the relay's scale: so much that any number of them ask for one relay, and no more. */
const BOUND_TARGET = 1_000_000;

/** The Codex harness pools of `environment`, its session conversations' and, main's, its folder conversations', where the host profile renders them. */
const codexPools = (config: InstallConfig, environment: Environment): string[] =>
  [HARNESS_POOLS.codex, ...(environment.branch === null && foldersRun(config) ? [FOLDER_HARNESS_POOLS.codex] : [])];

/** The relay's Deployment in `environment`, as the module says. */
function deployment(config: InstallConfig, environment: Environment): V1Deployment {
  const { alasio, host } = config;
  // The operator's home where the host profile gives it alasio, as alasio's Codex login is theirs then; else one of its own.
  const operatorHome = host.enabled && host.alasioHome;
  const home = operatorHome ? host.home : "/var/lib/alasio/home";
  const codexHome = posix.join(home, ".codex");
  const [uid, gid] = operatorHome ? [host.uid, host.gid] : [alasio.runAsUser, alasio.runAsGroup];
  const homeVolume: V1Volume = operatorHome
    ? { name: "codex-home", hostPath: { path: codexHome, type: "Directory" } }
    : { name: "codex-home", emptyDir: {} };
  const mounts: V1VolumeMount[] = [
    { name: "codex-home", mountPath: operatorHome ? codexHome : home },
    { name: "database", mountPath: "/run/alasio/database", readOnly: true },
  ];
  return {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name: CODEX_LOGIN, namespace: environment.namespace, labels: labels(CODEX_LOGIN_COMPONENT) },
    spec: {
      ...replicasOf(alasio.codexLogin.scaling),
      selector: { matchLabels: selectorLabels(CODEX_LOGIN_COMPONENT) },
      template: {
        metadata: { labels: labels(CODEX_LOGIN_COMPONENT) },
        spec: {
          automountServiceAccountToken: false,
          // Long enough for the answers streaming through it as it stops to end (CODEX_LOGIN_DRAIN_SECONDS), and its telemetry to be sent.
          terminationGracePeriodSeconds: CODEX_LOGIN_DRAIN_SECONDS + 10,
          // As a running turn's Codex calls the model through it.
          ...priority("turn"),
          securityContext: restrictedPod(uid, gid),
          ...imagePullSecrets(config),
          containers: [{
            name: CODEX_LOGIN_COMPONENT,
            image: imageReference(config.images.alasio),
            imagePullPolicy: config.images.pullPolicy,
            args: ["node", "/opt/alasio/src/codex/relay-main.ts"],
            workingDir: home,
            // alasio's variables first, as its Codex's binary among them, then its own, which win.
            env: [
              ...envOf(alasio.env),
              { name: "HOME", value: home },
              { name: "CODEX_HOME", value: codexHome },
              // A home of its own does not outlast its pod: the store keeps its login.
              ...(operatorHome ? [] : [{ name: "ALASIO_KEEP_CODEX_LOGIN", value: "1" }]),
              { name: "ALASIO_DATABASE_URL_FILE", value: "/run/alasio/database/url" },
              { name: CODEX_LOGIN_KEY_VARIABLE, valueFrom: { secretKeyRef: { name: CODEX_LOGIN_KEY_SECRET, key: "key" } } },
              ...otelEnv(config, environment),
            ],
            ports: [{ name: "codex-login", containerPort: CODEX_LOGIN_PORT }],
            readinessProbe: { tcpSocket: { port: "codex-login" }, periodSeconds: 2 },
            securityContext: restrictedContainer(),
            resources: alasio.codexLogin.resources,
            volumeMounts: mounts,
          }],
          volumes: [homeVolume, { name: "database", secret: { secretName: CODEX_LOGIN_DATABASE_SECRET, items: [{ key: "url", path: "url" }] } }],
          ...given("nodeSelector", { ...alasio.nodeSelector }),
          ...given("tolerations", [...alasio.tolerations]),
          ...given("affinity", alasio.affinity),
        },
      },
    },
  };
}

/** The objects of `environment`'s relay of its Codex login, as the module says. */
export function codexLoginObjects(config: InstallConfig, environment: Environment): KubernetesObject[] {
  const relay = deployment(config, environment);
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name: CODEX_LOGIN, namespace: environment.namespace, labels: labels(CODEX_LOGIN_COMPONENT) },
    spec: { selector: selectorLabels(CODEX_LOGIN_COMPONENT), ports: [{ name: "codex-login", port: CODEX_LOGIN_PORT, targetPort: "codex-login" }] },
  };
  // Its Codex harnesses bound, of each pool, as the gateway counts them: KEDA scales on the most of them, so on whether any is.
  const triggers: Trigger[] = codexPools(config, environment).map((pool) => ({
    type: "metrics-api",
    metadata: { url: `http://${GATEWAY}.${NAMESPACE}.svc.cluster.local:${GATEWAY_PORT}/counts/${environment.namespace}/${pool}`, valueLocation: "bound", targetValue: String(BOUND_TARGET), activationTargetValue: "0" },
  }));
  // Sized in place, of its memory alone, as what wakes from none is (./sizing.ts).
  return [relay, service, verticalPodAutoscaler(relay, "InPlace", ["memory"]), ...scaledObject(relay, config.alasio.codexLogin.scaling, triggers, { pollingSeconds: config.alasio.pollingSeconds, cooldownSeconds: config.alasio.cooldownSeconds })];
}
