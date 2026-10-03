/**
 * The templates the deployment renders for alasio's workspaces: `ALASIO_KUBE_TEMPLATES` is
 * the path of a JSON file, which the Helm chart mounts from a ConfigMap, of the form
 *
 *     {
 *       "sessions": { "namespace", "port", "workspaceDir", "egressGate", "fullModeNameservers",
 *                     "podTemplate", "volumeClaimTemplates" },
 *       "host":     { "namespace", "port", "stateRoot", "podTemplate" }
 *     }
 *
 * either of which may be absent: no `sessions`, no session filesystems; no `host`, no
 * folder workspaces. Each `podTemplate` has a container named `bayma` whose arguments
 * serve MCP over HTTP on `port`; alasio adds the rest per Sandbox (./sandboxes.ts).
 */
import { readFileSync } from "node:fs";

import type { V1PersistentVolumeClaim, V1PodSpec, V1PodTemplateSpec } from "@kubernetes/client-node";

/** A pod template with a spec, whose containers include one named `bayma`. */
export interface BaymaPodTemplate extends V1PodTemplateSpec {
  spec: V1PodSpec;
}

/** What both profiles hold: where their Sandboxes are made, and from what. */
export interface SandboxProfile {
  readonly namespace: string;
  /** The port bayma serves MCP over HTTP on. */
  readonly port: number;
  readonly podTemplate: BaymaPodTemplate;
}

/** The template of session filesystems' Sandboxes (charts/alasio/templates/alasio/sandbox-templates.yaml). */
export interface SessionsProfile extends SandboxProfile {
  /** Where the session's workspace is in bayma's container. */
  readonly workspaceDir: string;
  /** Whether a session waits for its egress to be confined before it starts; only false turns it off. */
  readonly egressGate?: boolean | null;
  /** The resolvers sessions with internet access use, or none for alasio's defaults. */
  readonly fullModeNameservers?: readonly string[] | null;
  readonly volumeClaimTemplates?: readonly V1PersistentVolumeClaim[];
}

/** The template of folder workspaces' bayma (charts/alasio/templates/alasio/sandbox-templates.yaml). */
export interface HostProfile extends SandboxProfile {
  /** The directory on the host under which each conversation's bayma keeps its state. */
  readonly stateRoot: string;
}

/** ALASIO_KUBE_TEMPLATES, checked: a profile the deployment does not render is null. */
export interface KubeTemplates {
  readonly sessions: SessionsProfile | null;
  readonly host: HostProfile | null;
}

const NAMESPACE = /^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$/u;

/** `value`'s property `key`, as `value?.[key]` reads it from parsed JSON. */
function property(value: unknown, key: string): unknown {
  if (typeof value !== "object" || value === null) return undefined;
  return Reflect.get(value, key);
}

function checkProfile(name: string, profile: unknown): object | null {
  if (profile === undefined || profile === null) return null;
  const where = `ALASIO_KUBE_TEMPLATES ${name}`;
  if (typeof profile !== "object") throw new Error(`${where} must be an object`);
  if (!NAMESPACE.test(String(property(profile, "namespace") ?? ""))) throw new Error(`${where}.namespace must be a namespace name`);
  const port = property(profile, "port");
  if (typeof port !== "number" || !Number.isInteger(port) || port < 1 || port > 65535) throw new Error(`${where}.port must be a port number`);
  const containers = property(property(property(profile, "podTemplate"), "spec"), "containers");
  if (!Array.isArray(containers) || !containers.some((container: unknown) => property(container, "name") === "bayma")) {
    throw new Error(`${where}.podTemplate.spec.containers must include one named "bayma"`);
  }
  return profile;
}

/**
 * The templates, read once at startup and checked, so a deployment that renders them
 * wrongly fails as it starts rather than at a workspace's first turn.
 */
export function loadKubeTemplates(
  env: Readonly<NodeJS.ProcessEnv> = process.env,
  read: (path: string) => string = (path) => readFileSync(path, "utf8"),
): KubeTemplates {
  const path = env["ALASIO_KUBE_TEMPLATES"]?.trim();
  if (!path) throw new Error("ALASIO_KUBE_TEMPLATES is not set: alasio runs where its Helm chart deploys it");
  // The chart renders an object, whose profiles are checked below.
  let parsed: { readonly sessions?: unknown; readonly host?: unknown };
  try {
    parsed = JSON.parse(read(path));
  } catch (error) {
    throw new Error(`ALASIO_KUBE_TEMPLATES ${path} is not readable JSON: ${error instanceof Error ? error.message : String(error)}`);
  }
  const sessions = checkProfile("sessions", parsed.sessions);
  const host = checkProfile("host", parsed.host);
  if (sessions && typeof property(sessions, "workspaceDir") !== "string") throw new Error("ALASIO_KUBE_TEMPLATES sessions.workspaceDir must be a path");
  if (host && typeof property(host, "stateRoot") !== "string") throw new Error("ALASIO_KUBE_TEMPLATES host.stateRoot must be a path");
  // What alasio relies on is checked above; the rest of each pod template, and the claim
  // templates, are Kubernetes objects the chart renders, which the API server checks as
  // each Sandbox is made.
  return { sessions: sessions as SessionsProfile | null, host: host as HostProfile | null };
}
