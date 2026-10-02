/**
 * bayma for folder workspaces on Kubernetes: a Sandbox (../kube/sandboxes.js) per
 * conversation and harness in the host namespace, made from the chart's `host` template
 * (decision 007 of the Kubernetes design), reached over MCP HTTP (decision 005).
 *
 * The host profile is the operator's opt-in to agents that work on a machine's own
 * files: its template mounts what the operator chose from the node, and runs bayma with
 * what it needs to snapshot its REPL sessions. alasio adds only what is per
 * conversation: bayma's state directory under the template's `stateRoot`, keyed by
 * harness and conversation as the Docker runtime keys it, and the conversation's
 * telemetry settings, exported directly, as a folder's bayma always has.
 */
import { createHash } from "node:crypto";
import { join } from "node:path";

import { createKubeClient } from "../kube/client.js";
import { BAYMA_CONTAINER, createSandboxes, sandboxManifest } from "../kube/sandboxes.js";
import { conversationTelemetryEnv } from "../telemetry/index.js";

const WORKLOAD_LABEL = "alasio.dev/workload";
const HARNESS_LABEL = "alasio.dev/harness";
const CONVERSATION_ANNOTATION = "alasio.dev/conversation";

/** A path segment from `value`, as the Docker runtime makes one. */
export function sanitizePathToken(value) {
  return String(value).replace(/[^A-Za-z0-9_.-]+/g, "-").replace(/^[-.]+|-+$/g, "") || "default";
}

/** The Sandbox serving bayma to one conversation under one harness. */
export function hostBaymaName(harness, threadKey) {
  return `bayma-${createHash("sha256").update(`${harness}\0${threadKey}`).digest("hex").slice(0, 20)}`;
}

/** Where that bayma keeps its state, as the pod sees it. */
export function hostBaymaStateDir(profile, harness, threadKey) {
  return join(profile.stateRoot, sanitizePathToken(harness), sanitizePathToken(threadKey));
}

/** The Sandbox for `harness` and `threadKey` from the `host` profile. Pure, for tests. */
export function hostBaymaManifest({ harness, threadKey, profile, env = process.env }) {
  const telemetry = conversationTelemetryEnv({ conversationId: threadKey }, env);
  return sandboxManifest({
    name: hostBaymaName(harness, threadKey),
    namespace: profile.namespace,
    template: profile,
    labels: { [WORKLOAD_LABEL]: "folder", [HARNESS_LABEL]: sanitizePathToken(harness) },
    annotations: { [CONVERSATION_ANNOTATION]: threadKey },
    configure(spec) {
      const bayma = spec.containers.find((container) => container.name === BAYMA_CONTAINER);
      bayma.args = [...bayma.args, "--state-dir", hostBaymaStateDir(profile, harness, threadKey)];
      bayma.env = [...(bayma.env ?? []), ...Object.entries(telemetry).map(([name, value]) => ({ name, value }))];
      return spec;
    },
  });
}

/**
 * The folder workspaces' bayma, or null when the deployment renders no `host` template.
 * `ensure({ harness, threadKey })` resolves `{ url, headers }` once that conversation's
 * bayma answers.
 */
export function createHostBayma({ templates, kube = createKubeClient(), env = process.env }) {
  const profile = templates?.host;
  if (!profile) return null;
  const sandboxes = createSandboxes({ kube, namespace: profile.namespace, port: profile.port });
  return {
    async ensure({ harness, threadKey }) {
      return await sandboxes.ensure(hostBaymaName(harness, threadKey), () => hostBaymaManifest({ harness, threadKey, profile, env }));
    },
  };
}
