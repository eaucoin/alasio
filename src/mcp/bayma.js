/**
 * bayma, the MCP server alasio itself gives its agents, beside whatever servers the
 * operator has configured for each harness.
 *
 * A folder workspace's bayma is a Sandbox (../kube/sandboxes.js) per conversation and
 * harness in the host namespace, made from the deployment's `host` template: the
 * operator's opt-in to agents that work on a machine's own files. The template mounts
 * what the operator chose from the node and runs bayma with what it needs to snapshot its
 * REPL sessions as it stops and restore them whole as it starts; alasio adds only what is
 * per conversation: bayma's state directory under the template's `stateRoot`, keyed by
 * harness and conversation, and the conversation's telemetry settings. Harnesses reach it
 * over MCP HTTP with its token; each adapter turns the server into its own config shape.
 */
import { createHash } from "node:crypto";
import { join } from "node:path";

import { createKubeClient } from "../kube/client.js";
import { loadKubeTemplates } from "../kube/config.js";
import { BAYMA_CONTAINER, createSandboxes, sandboxManifest } from "../kube/sandboxes.js";
import { conversationTelemetryEnv } from "../telemetry/index.js";

export const BAYMA_SERVER_NAME = "bayma";

/**
 * How long a harness waits for bayma as it starts: a conversation's first turn makes its
 * Sandbox, and one that was suspended resumes, restoring its REPL sessions.
 */
export const BAYMA_STARTUP_TIMEOUT_MS = 60_000;

const WORKLOAD_LABEL = "alasio.dev/workload";
const HARNESS_LABEL = "alasio.dev/harness";
const CONVERSATION_ANNOTATION = "alasio.dev/conversation";

/** A path segment from `value`. */
export function sanitizePathToken(value) {
  return String(value).replace(/[^A-Za-z0-9_.-]+/g, "-").replace(/^[-.]+|-+$/g, "") || "default";
}

/** The Sandbox serving bayma to one conversation under one harness. */
export function hostBaymaName(harness, threadKey) {
  return `bayma-${createHash("sha256").update(`${harness}\0${threadKey}`).digest("hex").slice(0, 20)}`;
}

/**
 * Where that bayma keeps its state, as the pod sees it. bayma leases its state directory
 * to a single server, and a Codex thread keeps its server alive after the conversation
 * switches to Claude, so the directory is keyed by harness as well as conversation.
 */
export function hostBaymaStateDir(profile, harness, threadKey) {
  return join(profile.stateRoot, sanitizePathToken(harness), sanitizePathToken(threadKey));
}

/**
 * The Sandbox for `harness` and `threadKey` from the `host` profile, exporting its
 * telemetry where alasio exports its own, labelled with the conversation. Pure, for tests.
 */
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
export function createHostBayma({ templates, kube = null, env = process.env, fetchImpl = fetch }) {
  const profile = templates?.host;
  if (!profile) return null;
  const sandboxes = createSandboxes({ kube: kube ?? createKubeClient(), namespace: profile.namespace, port: profile.port, fetchImpl });
  return {
    async ensure({ harness, threadKey }) {
      return await sandboxes.ensure(hostBaymaName(harness, threadKey), () => hostBaymaManifest({ harness, threadKey, profile, env }));
    },
  };
}

let hostBayma = null;

/**
 * The bayma MCP server a folder workspace's conversation gets under `harness`, once it
 * answers: `{ type: "http", url, headers }`, in Claude Code's MCP config shape. `env`
 * is alasio's own, whose telemetry settings the harnesses' environments leave out.
 */
export async function folderBaymaServer({ harness, threadKey, env = process.env }) {
  hostBayma ??= createHostBayma({ templates: loadKubeTemplates(env), env });
  if (!hostBayma) throw new Error("this deployment offers no folder workspaces: its templates have no host profile");
  return { type: "http", ...(await hostBayma.ensure({ harness, threadKey })) };
}
