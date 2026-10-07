/**
 * What alasio talks to in the end-to-end run instead of Telegram and a telemetry backend:
 * the stand-ins ./telegram-stub.ts and ./otlp-sink.ts, each run by Node in a Deployment
 * of its own, from a ConfigMap that holds its script, behind a Service, in a namespace of
 * their own. The run applies them to the cluster before it starts alasio. Telegram's
 * serves HTTPS too, as api.telegram.org, for Grafana (./telegram-tls.ts).
 */
import { readFileSync } from "node:fs";

import type { KubernetesObject, V1ConfigMap, V1Deployment, V1Namespace, V1Service } from "@kubernetes/client-node";

import { CERTIFICATE, KEY } from "./telegram-tls.ts";

/** The stand-ins' namespace. */
export const STAND_INS = "alasio-test";

/**
 * A stand-in: its Deployment, the container in it, its Service and port, its script, and
 * the path that answers once it serves; and the port it serves HTTPS on, which its
 * Service serves as 443, if it does.
 */
export interface StandIn {
  readonly name: string;
  readonly container: string;
  readonly service: string;
  readonly port: number;
  readonly script: string;
  readonly ready: string;
  readonly tlsPort?: number;
}

/** The port the Telegram stand-in serves HTTPS on. */
export const TELEGRAM_TLS_PORT = 8443;

export const TELEGRAM: StandIn = { name: "telegram-stub", container: "stub", service: "telegram", port: 8081, script: "telegram-stub.ts", ready: "/control/calls", tlsPort: TELEGRAM_TLS_PORT };
export const OTLP: StandIn = { name: "otlp-sink", container: "sink", service: "otlp", port: 4318, script: "otlp-sink.ts", ready: "/control/exports" };

/** Where a stand-in is reached from the cluster. */
export const urlOf = ({ service, port }: StandIn): string => `http://${service}.${STAND_INS}.svc:${port}`;

/** Node, which the scripts need and nothing else. */
const NODE_IMAGE = "node:24.14.0-bookworm-slim@sha256:d8e448a56fc63242f70026718378bd4b00f8c82e78d20eefb199224a4d8e33d8";

/** `standIn`'s objects: its script's ConfigMap, its Service, and its Deployment, run unprivileged. */
function objectsOf({ name, container, service, port, script, ready, tlsPort }: StandIn): KubernetesObject[] {
  const labels = { app: name };
  const configMap: V1ConfigMap = {
    apiVersion: "v1",
    kind: "ConfigMap",
    metadata: { name, namespace: STAND_INS },
    data: { [script]: readFileSync(new URL(script, import.meta.url), "utf8"), ...(tlsPort ? { "tls.pem": CERTIFICATE, "tls.key": KEY } : {}) },
  };
  const serviceObject: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name: service, namespace: STAND_INS },
    spec: { selector: labels, ports: [{ name: "http", port }, ...(tlsPort ? [{ name: "https", port: 443, targetPort: tlsPort }] : [])] },
  };
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name, namespace: STAND_INS },
    spec: {
      replicas: 1,
      selector: { matchLabels: labels },
      template: {
        metadata: { labels },
        spec: {
          securityContext: { runAsNonRoot: true, runAsUser: 1000, seccompProfile: { type: "RuntimeDefault" } },
          containers: [{
            name: container,
            image: NODE_IMAGE,
            command: ["node", `/stand-in/${script}`, String(port), ...(tlsPort ? [String(tlsPort)] : [])],
            ports: [{ containerPort: port }, ...(tlsPort ? [{ containerPort: tlsPort }] : [])],
            readinessProbe: { httpGet: { path: ready, port } },
            securityContext: { allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] } },
            volumeMounts: [{ name: "stand-in", mountPath: "/stand-in" }],
          }],
          volumes: [{ name: "stand-in", configMap: { name } }],
        },
      },
    },
  };
  return [configMap, serviceObject, deployment];
}

/** The stand-ins' objects, their namespace first. */
export function standInObjects(): KubernetesObject[] {
  const namespace: V1Namespace = { apiVersion: "v1", kind: "Namespace", metadata: { name: STAND_INS } };
  return [namespace, ...objectsOf(TELEGRAM), ...objectsOf(OTLP)];
}
