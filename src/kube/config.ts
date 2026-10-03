// @ts-nocheck
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

const NAMESPACE = /^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$/u;

function checkProfile(name, profile) {
  if (profile === undefined || profile === null) return null;
  const where = `ALASIO_KUBE_TEMPLATES ${name}`;
  if (typeof profile !== "object") throw new Error(`${where} must be an object`);
  if (!NAMESPACE.test(profile.namespace ?? "")) throw new Error(`${where}.namespace must be a namespace name`);
  if (!Number.isInteger(profile.port) || profile.port < 1 || profile.port > 65535) throw new Error(`${where}.port must be a port number`);
  const containers = profile.podTemplate?.spec?.containers;
  if (!Array.isArray(containers) || !containers.some((container) => container?.name === "bayma")) {
    throw new Error(`${where}.podTemplate.spec.containers must include one named "bayma"`);
  }
  return profile;
}

/**
 * The templates, read once at startup and checked, so a deployment that renders them
 * wrongly fails as it starts rather than at a workspace's first turn.
 */
export function loadKubeTemplates(env = process.env, read = (path) => readFileSync(path, "utf8")) {
  const path = env.ALASIO_KUBE_TEMPLATES?.trim();
  if (!path) throw new Error("ALASIO_KUBE_TEMPLATES is not set: alasio runs where its Helm chart deploys it");
  let parsed;
  try {
    parsed = JSON.parse(read(path));
  } catch (error) {
    throw new Error(`ALASIO_KUBE_TEMPLATES ${path} is not readable JSON: ${error.message}`);
  }
  const sessions = checkProfile("sessions", parsed.sessions);
  const host = checkProfile("host", parsed.host);
  if (sessions && typeof sessions.workspaceDir !== "string") throw new Error("ALASIO_KUBE_TEMPLATES sessions.workspaceDir must be a path");
  if (host && typeof host.stateRoot !== "string") throw new Error("ALASIO_KUBE_TEMPLATES host.stateRoot must be a path");
  return { sessions, host };
}
