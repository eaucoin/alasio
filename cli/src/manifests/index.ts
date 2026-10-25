/**
 * Every Kubernetes object an installation of alasio is, from its configuration
 * (./config.ts): what alasio installs, as the installation `alasio` in the namespace
 * `alasio`. Pure; applying them is the caller's (../install.ts).
 */
import type { KubernetesObject } from "@kubernetes/client-node";

import { alasioObjects, type Carried, NOTHING_CARRIED } from "./alasio.ts";
import { collectorObjects } from "./collector.ts";
import { MAIN_ENVIRONMENT } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { folderObjects } from "./folders.ts";
import { grafanaObjects } from "./grafana.ts";
import { harnessPoolObjects } from "./harness-pools.ts";
import { headroomObjects } from "./headroom.ts";
import { imageHolderObjects } from "./image-holder.ts";
import { kedaObjects } from "./keda.ts";
import { lakeObjects } from "./lake.ts";
import { logObjects } from "./log.ts";
import { neonObjects } from "./neon.ts";
import { networkPolicyObjects } from "./network-policies.ts";
import { nodeAgentObjects } from "./node-agent.ts";
import { objectStoreObjects } from "./object-store.ts";
import { priorityClassObjects } from "./priorities.ts";
import { sandboxServiceObjects } from "./sandbox-gateway.ts";
import { vpaObjects } from "./vpa.ts";
import { webhookObjects } from "./webhook.ts";
import { workspaceStorageObjects } from "./workspace-storage.ts";

export { type Carried, carriedFrom, type Kept, NOTHING_CARRIED, NOTHING_KEPT } from "./alasio.ts";
export { decodeInstallConfig, InstallConfig, InstallConfigError, type InstallConfigFile } from "./config.ts";
export { NAMESPACE } from "./common.ts";

/**
 * The objects of the installation `config` describes: the PriorityClasses its pods run
 * under, KEDA's, VPA's, alasio's, what holds alasio's image on its nodes, Neon's, the object store's, the log's, the webhook
 * intake's, the lake's, Grafana's, the telemetry collector's, workspace storage's, the node
 * agents', the sandbox service's, its folder instances', its harness pools', the NetworkPolicies, and the
 * headroom's; with what alasio
 * carries forward from the installation it replaces (./alasio.ts carriedFrom).
 */
export function manifests(config: InstallConfig, carried: Carried = NOTHING_CARRIED): readonly KubernetesObject[] {
  return [
    ...priorityClassObjects(config),
    ...kedaObjects(config),
    ...vpaObjects(config),
    ...alasioObjects(config, MAIN_ENVIRONMENT, carried),
    ...imageHolderObjects(config),
    ...neonObjects(config),
    ...objectStoreObjects(config),
    ...logObjects(config),
    ...webhookObjects(config),
    ...lakeObjects(config),
    ...grafanaObjects(config),
    ...collectorObjects(config),
    ...workspaceStorageObjects(config),
    ...nodeAgentObjects(config),
    ...sandboxServiceObjects(config, carried),
    ...folderObjects(config),
    ...harnessPoolObjects(config, MAIN_ENVIRONMENT),
    ...networkPolicyObjects(config, MAIN_ENVIRONMENT, carried.apiServer),
    ...headroomObjects(config),
  ];
}
