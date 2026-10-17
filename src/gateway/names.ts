/**
 * What the sandbox gateway (./gateway.ts) and the session telemetry receiver
 * (../receiver/receiver.ts) are configured by: their variables, and the gateway's
 * configuration file. Free of anything but Effect's Schema, so the command line, which
 * renders them, takes it without either.
 */
import { Schema } from "effect";

/** The variable of the path of the gateway's configuration, a GatewayConfig as JSON. */
export const GATEWAY_CONFIG_VARIABLE = "ALASIO_GATEWAY_CONFIG";
/** The variables of the keys callers' tokens, and bindings', are signed with: the keys' text, as the stack's setup makes them. */
export const CALLER_KEY_VARIABLE = "CALLER_KEY";
export const BINDING_KEY_VARIABLE = "BINDING_KEY";

/** The variables of where the gateway tells of events (../events/events.ts): the log's brokers, its principal's name, and the file of its password. */
export const EVENTS_BROKERS_VARIABLE = "EVENTS_BROKERS";
export const EVENTS_USERNAME_VARIABLE = "EVENTS_USERNAME";
export const EVENTS_PASSWORD_FILE_VARIABLE = "EVENTS_PASSWORD_FILE";

/** The variables of the receiver's port, and of main's sessions' namespace, beside which it takes branch environments' bindings. */
export const RECEIVER_PORT_VARIABLE = "RECEIVER_PORT";
export const SESSIONS_NAMESPACE_VARIABLE = "SESSIONS_NAMESPACE";

/** The sandbox pods' policy, which holds every pod of a sessions' namespace to its pools' templates (cli/src/manifests/admission.ts sandboxPodPolicy). */
export const SANDBOX_POD_POLICY = "alasio-sandbox-pods";

/** The gateway's configuration, as `alasio up` renders it (cli/src/manifests/sandbox-gateway.ts). */
export const GatewayConfig = Schema.Struct({
  /** The port it serves on, and the host its replicas' headless Service resolves to, each at that port. */
  port: Schema.Number,
  peers: Schema.String,
  /** The port pool pods' bayma serves on. */
  podPort: Schema.Number,
  /** How long a bind waits for a free pod, and an idle pod is kept. */
  waitSeconds: Schema.Number,
  releaseAfterSeconds: Schema.Number,
  /** main's environment of sessions, by its name, and the namespace its pools are in; branch environments' are found (./environments.ts). */
  environment: Schema.Struct({ name: Schema.String, namespace: Schema.String }),
  /** The quota of a workspace the gateway makes, in bytes. */
  quotaBytes: Schema.Number,
  /** Each pool's name, by its sessions' internet. */
  pools: Schema.Struct({ none: Schema.String, full: Schema.String }),
  /** Where the node agents are. */
  nodeAgents: Schema.Struct({ namespace: Schema.String, selector: Schema.String, port: Schema.Number }),
});
export type GatewayConfig = typeof GatewayConfig.Type;
