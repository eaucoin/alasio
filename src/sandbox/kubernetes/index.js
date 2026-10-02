/**
 * Session filesystems on Kubernetes: each is a Sandbox (../../kube/sandboxes.js) in the
 * sessions namespace, made from the chart's template, whose volume claim is the
 * workspace and whose gVisor pod runs bayma (decision 004 of the Kubernetes design).
 * The surface is the Docker driver's (../index.js), so the harnesses and the Telegram
 * app use either alike.
 *
 * What a session may reach is NetworkPolicy's to enforce, by the labels set here: the
 * chart's policies admit alasio alone in, and let a session out to alasio's telemetry
 * receiver only ("none") or to the internet's public addresses too ("full"). DNS is
 * set to match: public resolvers in "full", and none at all in "none", so a name cannot
 * carry anything out through the cluster's resolver.
 *
 * Policies reach a new pod asynchronously, so a session's pod first waits, in an init
 * container, until its egress is confined: until the cluster's API server, a private
 * address both modes refuse, no longer answers. Where NetworkPolicy is not enforced at
 * all a session therefore never starts, rather than starting open.
 */
import { lookup } from "node:dns/promises";
import { mkdirSync } from "node:fs";
import { join } from "node:path";

import { createKubeClient } from "../../kube/client.js";
import { BAYMA_CONTAINER, createSandboxes, sameToken, sandboxManifest, tokenSandboxName, tokenSecretName } from "../../kube/sandboxes.js";
import { createLogger } from "../../shared/log.js";
import { overLimitNote } from "../../telegram/rich-media.js";
import { assertValidVolumeId, isValidVolumeId } from "../names.js";
import { sandboxBaymaTelemetryEnv, sandboxResource } from "../telemetry.js";
import { startTelemetryReceiver } from "./telemetry-receiver.js";

const log = createLogger("sandbox");

export const NET_MODE_LABEL = "alasio.dev/net-mode";
export const WORKLOAD_LABEL = "alasio.dev/workload";
const DEFAULT_FULL_MODE_NAMESERVERS = ["1.1.1.1", "8.8.8.8"];
// The token's environment variable in bayma's container, which the OTLP headers expand.
const TOKEN_ENV = "ALASIO_SANDBOX_TOKEN";

/**
 * The egress gate's program: exits once three connections in a row to the API server
 * fail, and fails the pod if they still succeed after two minutes. Runs on the agent
 * image's Node.
 */
export const EGRESS_GATE_SCRIPT = [
  "const net = require('node:net');",
  "const host = process.env.KUBERNETES_SERVICE_HOST, port = Number(process.env.KUBERNETES_SERVICE_PORT);",
  "const deadline = Date.now() + 120000; let refused = 0;",
  "(function probe() {",
  "  const socket = net.connect({ host, port, timeout: 1000 });",
  "  const next = (blocked) => { socket.destroy(); refused = blocked ? refused + 1 : 0;",
  "    if (refused >= 3) process.exit(0);",
  "    if (Date.now() > deadline) { console.error('egress is not confined: is NetworkPolicy enforced in this cluster?'); process.exit(1); }",
  "    setTimeout(probe, 250); };",
  "  socket.once('connect', () => next(false)); socket.once('timeout', () => next(true)); socket.once('error', () => next(true));",
  "})();",
].join("\n");

/**
 * The Sandbox for a session on `volumeId` with internet mode `netMode`, from the
 * `profile` (ALASIO_KUBE_TEMPLATES `sessions`). `telemetry` is `{ endpoint, env }`, or
 * null when alasio exports none. Pure, for tests.
 */
export function sessionSandboxManifest({ volumeId, netMode, profile, telemetry }) {
  const full = netMode === "full";
  return sandboxManifest({
    name: assertValidVolumeId(volumeId),
    namespace: profile.namespace,
    template: profile,
    labels: { [WORKLOAD_LABEL]: "session", [NET_MODE_LABEL]: full ? "full" : "none" },
    configure(spec) {
      const bayma = spec.containers.find((container) => container.name === BAYMA_CONTAINER);
      if (telemetry) {
        bayma.env = [
          ...(bayma.env ?? []),
          { name: TOKEN_ENV, valueFrom: { secretKeyRef: { name: tokenSecretName(volumeId), key: "token" } } },
          ...Object.entries({ ...telemetry.env, OTEL_EXPORTER_OTLP_ENDPOINT: telemetry.endpoint }).map(([name, value]) => ({ name, value })),
          { name: "OTEL_EXPORTER_OTLP_HEADERS", value: `authorization=Bearer%20$(${TOKEN_ENV})` },
        ];
      }
      return {
        ...spec,
        // Nothing in a session speaks to the cluster as anyone, or learns its services.
        automountServiceAccountToken: false,
        enableServiceLinks: false,
        dnsPolicy: "None",
        dnsConfig: { nameservers: full ? (profile.fullModeNameservers ?? DEFAULT_FULL_MODE_NAMESERVERS) : ["127.0.0.1"] },
        initContainers: [
          ...(profile.egressGate === false ? [] : [{
            name: "egress-gate",
            image: bayma.image,
            command: ["node", "-e", EGRESS_GATE_SCRIPT],
            securityContext: bayma.securityContext,
            resources: { requests: { cpu: "10m", memory: "32Mi" }, limits: { memory: "64Mi" } },
          }]),
          ...(spec.initContainers ?? []),
        ],
      };
    },
  });
}

/** The address sessions export telemetry to: alasio's receiver Service, by IP, since "none" has no DNS. */
async function receiverEndpoint(service, port, resolve = lookup) {
  const { address } = await resolve(service);
  return `http://${address}:${port}`;
}

/**
 * The session-filesystem subsystem on Kubernetes, or null when the deployment renders
 * no `sessions` template. `env` holds the standard OpenTelemetry variables and
 * `ALASIO_TELEMETRY_RECEIVER_SERVICE` and `_PORT`, where sessions' telemetry is
 * received when alasio exports any.
 */
export function createKubernetesSandbox({
  templates,
  stateDir,
  env = process.env,
  kube = createKubeClient(),
  createForwarder,
  startReceiver = startTelemetryReceiver,
  resolve = lookup,
  fetchImpl = fetch,
}) {
  const profile = templates?.sessions;
  if (!profile) return null;
  const sandboxes = createSandboxes({ kube, namespace: profile.namespace, port: profile.port, fetchImpl });
  const telemetryEnv = sandboxBaymaTelemetryEnv(env);
  const receiverPort = Number(env.ALASIO_TELEMETRY_RECEIVER_PORT || 4318);
  const receiverService = env.ALASIO_TELEMETRY_RECEIVER_SERVICE?.trim();
  const exportsTelemetry = Object.keys(telemetryEnv).length > 0 && Boolean(receiverService);
  if (Object.keys(telemetryEnv).length > 0 && !receiverService) {
    log.warn("sessions' telemetry is not received: ALASIO_TELEMETRY_RECEIVER_SERVICE is not set");
  }

  // The receiver, and the forwarder it exports through, once started.
  let receiving = null;
  function receive() {
    receiving ??= (async () => {
      const forwarder = await createForwarder(env);
      const receiver = await startReceiver({
        port: receiverPort,
        forwarder,
        stampFor: (volumeId) => sandboxResource(volumeId, env),
        async authenticate(token) {
          const name = tokenSandboxName(token);
          if (!isValidVolumeId(name)) return null;
          const expected = await sandboxes.token(name).catch(() => null);
          return expected && sameToken(token, expected) ? name : null;
        },
      });
      return { forwarder, receiver };
    })().catch((error) => {
      receiving = null;
      throw error;
    });
    return receiving;
  }

  // Sessions that outlived the last alasio export as soon as this one is up.
  if (exportsTelemetry) {
    receive().catch((error) => log.warn(`cannot receive sessions' telemetry: ${error.message}`));
  }

  async function telemetry() {
    if (!exportsTelemetry) return null;
    await receive();
    return { endpoint: await receiverEndpoint(receiverService, receiverPort, resolve), env: telemetryEnv };
  }

  async function manifest(volumeId, netMode) {
    return sessionSandboxManifest({ volumeId, netMode, profile, telemetry: await telemetry() });
  }

  /** Makes the Sandbox, with what it is made from resolved first. */
  async function ensure(volumeId, netMode) {
    const made = await manifest(volumeId, netMode);
    return await sandboxes.ensure(volumeId, () => made);
  }

  return {
    enabled: true,
    runtime: "kubernetes",

    volumes: {
      /** A new session's Sandbox, made now so its volume is ready by its first turn. */
      async create(volumeId, netMode = "none") {
        await ensure(volumeId, netMode);
        log.info(`created session ${volumeId} (${netMode} internet)`);
        return { volumeId, netMode };
      },
      async destroy(volumeId) {
        await sandboxes.remove(assertValidVolumeId(volumeId));
      },
    },

    /** As the Docker driver's: an empty directory here, keyed by the session. */
    harnessDirectory(volumeId) {
      const directory = join(stateDir, "sessionfs", "workspaces", assertValidVolumeId(volumeId));
      mkdirSync(directory, { recursive: true });
      return directory;
    },

    /**
     * Makes sure the session's Sandbox runs and bayma answers in it, resuming one that
     * was suspended; one that is gone is made again, empty, with no internet.
     */
    async ensureSession(volumeId) {
      return { bayma: await ensure(assertValidVolumeId(volumeId), "none") };
    },

    /**
     * A file from the session as its agent sees it: read by the agent's own user in its
     * container, relative to the workspace, so its path and symlinks reach only the
     * session's files. `{ bytes }`, or `{ note }` saying why not.
     */
    async readFile(volumeId, path, maxBytes) {
      const sandbox = await kube.read("agents.x-k8s.io/v1beta1", "Sandbox", profile.namespace, assertValidVolumeId(volumeId));
      if (!sandbox || sandbox.spec?.operatingMode === "Suspended") return { note: "the session is not running" };
      const script = 'cd "$3" || exit 3; f="$1"; [ -f "$f" ] || exit 3; s=$(stat -L -c %s -- "$f") || exit 3; [ "$s" -le "$2" ] || { printf %s "$s" >&2; exit 4; }; exec cat -- "$f"';
      const { exitCode, stdout, stderr } = await kube.exec(
        profile.namespace,
        volumeId,
        BAYMA_CONTAINER,
        ["sh", "-c", script, "sh", path, String(maxBytes), profile.workspaceDir],
        { maxBytes: maxBytes + 1 },
      );
      if (exitCode === 0) return { bytes: stdout };
      if (exitCode === 3) return { note: "file not found" };
      if (exitCode === 4) return { note: overLimitNote(Number(stderr.trim()), maxBytes) };
      throw new Error(`reading ${path} in session ${volumeId} failed (exit ${exitCode}): ${stderr.trim()}`);
    },

    /**
     * alasio is shutting down: stop receiving. Sessions keep running for the next alasio,
     * and their exporters retry what they could not send meanwhile.
     */
    async close() {
      const started = await receiving?.catch(() => null);
      receiving = null;
      await started?.receiver.close();
      await started?.forwarder.close();
    },
  };
}
