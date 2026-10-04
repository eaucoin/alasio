/**
 * Every Kubernetes object an installation of alasio is, from its configuration
 * (./config.ts): what alasio installs, as the installation `alasio` in the namespace
 * `alasio`. Pure; applying them is the caller's (../install.ts).
 */
import type { KubernetesObject } from "@kubernetes/client-node";

import { agentSandboxObjects } from "./agent-sandbox.ts";
import { alasioObjects } from "./alasio.ts";
import type { InstallConfig } from "./config.ts";
import { lakeObjects } from "./lake.ts";
import { neonObjects } from "./neon.ts";
import { networkPolicyObjects } from "./network-policies.ts";
import { objectStoreObjects } from "./object-store.ts";

export { decodeInstallConfig, InstallConfig, InstallConfigError, type InstallConfigFile } from "./config.ts";
export { NAMESPACE } from "./common.ts";

/** The objects of the installation `config` describes: agent-sandbox's, alasio's, Neon's, the object store's, the lake's, and the NetworkPolicies. */
export function manifests(config: InstallConfig): readonly KubernetesObject[] {
  return [
    ...agentSandboxObjects(config),
    ...alasioObjects(config),
    ...neonObjects(config),
    ...objectStoreObjects(config),
    ...lakeObjects(config),
    ...networkPolicyObjects(config),
  ];
}
