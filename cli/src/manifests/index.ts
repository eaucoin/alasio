/**
 * Every Kubernetes object an installation of alasio is, from its configuration
 * (./config.ts): what alasio installs, as the installation `alasio` in the namespace
 * `alasio`. Pure; applying them is the caller's (../install.ts).
 */
import type { KubernetesObject } from "@kubernetes/client-node";

import { agentSandboxObjects } from "./agent-sandbox.ts";
import { alasioObjects } from "./alasio.ts";
import { logObjects } from "./automq.ts";
import { collectorObjects } from "./collector.ts";
import type { InstallConfig } from "./config.ts";
import { grafanaObjects } from "./grafana.ts";
import { headroomObjects } from "./headroom.ts";
import { kedaObjects } from "./keda.ts";
import { lakeObjects } from "./lake.ts";
import { neonObjects } from "./neon.ts";
import { networkPolicyObjects } from "./network-policies.ts";
import { objectStoreObjects } from "./object-store.ts";
import { priorityClassObjects } from "./priorities.ts";
import { vpaObjects } from "./vpa.ts";
import { webhookObjects } from "./webhook.ts";
import { workspaceStorageObjects } from "./workspace-storage.ts";

export { decodeInstallConfig, InstallConfig, InstallConfigError, type InstallConfigFile } from "./config.ts";
export { NAMESPACE } from "./common.ts";

/**
 * The objects of the installation `config` describes: the PriorityClasses its pods run
 * under, agent-sandbox's, KEDA's, VPA's, alasio's, Neon's, the object store's, the log's,
 * the webhook intake's, the lake's, Grafana's, the telemetry collector's, workspace storage's, the
 * NetworkPolicies, and the headroom's.
 */
export function manifests(config: InstallConfig): readonly KubernetesObject[] {
  return [
    ...priorityClassObjects(config),
    ...agentSandboxObjects(config),
    ...kedaObjects(config),
    ...vpaObjects(config),
    ...alasioObjects(config),
    ...neonObjects(config),
    ...objectStoreObjects(config),
    ...logObjects(config),
    ...webhookObjects(config),
    ...lakeObjects(config),
    ...grafanaObjects(config),
    ...collectorObjects(config),
    ...workspaceStorageObjects(config),
    ...networkPolicyObjects(config),
    ...headroomObjects(config),
  ];
}
