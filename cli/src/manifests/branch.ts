/**
 * A branch environment's objects (`alasio branch create`): a different alasio, on a
 * copy-on-write copy of main's data, with a compute on the branch's timeline of Neon's,
 * which main's neon-control wakes and puts to sleep, and a lake on the branch's copy of
 * the lake's catalog, both reaching it through main's Neon proxy, in a namespace of its
 * own, and its sessions in another; named as main's are, and found, as they are deleted,
 * by those namespaces and BRANCH_LABEL.
 *
 * Its configuration is main's, with the operator's overrides, and never the host
 * profile: a folder is the machine's own files, which a branch cannot copy. Its alasio
 * reaches its sessions through main's sandbox gateway alone, by the branch's own key, and
 * main's alasio asks the gateway to fork into it the sessions the branch inherited
 * (src/branch/fork.ts). Its sessions' sandboxes are main's sandbox service's, in pools of its
 * own in its sessions' namespace, which the service finds by its label
 * (src/gateway/environments.ts), so nothing of main's changes as a branch comes or goes; and
 * its conversations' harnesses, where they run in pools, main's gateway's too, in pools of its
 * own in its own namespace (./harness-pools.ts). Its
 * Secrets are the command's to write: the database's, the
 * lake's and the compute's, as main's (the data is a copy, its roles and passwords main's);
 * its own bot's; Claude Code's token, when main has one; and its token to ask main to fork
 * with.
 */
import type { KubernetesObject } from "@kubernetes/client-node";

import { branchNamespace, branchSessionsNamespace } from "../../../src/branch/names.ts";
import { alasioObjects, type CpuBaseline } from "./alasio.ts";
import { BRANCH_LABEL, type Environment } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { lakeObjects } from "./lake.ts";
import { harnessPoolObjects } from "./harness-pools.ts";
import { compute, computeScaling } from "./neon.ts";
import { networkPolicyObjects } from "./network-policies.ts";
import { nodeAgentPodReaders } from "./node-agent.ts";
import { sandboxPoolObjects } from "./sandbox-gateway.ts";

/** The environment of the branch `name`. */
export const branchEnvironment = (name: string): Environment => ({ namespace: branchNamespace(name), branch: name });

/** `object`, labelled as the branch `name`'s. */
function branchLabelled(object: KubernetesObject, name: string): KubernetesObject {
  return { ...object, metadata: { ...object.metadata, labels: { ...object.metadata?.labels, [BRANCH_LABEL]: name } } };
}

/**
 * The objects of the branch environment `name`, of the configuration `config` (main's,
 * overridden, without the host profile), its sandbox pools main's, held to the CPU baseline
 * `cpuBaseline` main's are.
 */
export function branchObjects(config: InstallConfig, name: string, cpuBaseline: CpuBaseline): KubernetesObject[] {
  const environment = branchEnvironment(name);
  return [
    ...alasioObjects(config, environment),
    ...compute(config, environment),
    ...computeScaling(environment.namespace),
    ...lakeObjects(config, environment),
    ...networkPolicyObjects(config, environment),
    ...sandboxPoolObjects(config, environment, cpuBaseline),
    ...harnessPoolObjects(config, environment),
    // That the node agents may read the pods of the branch's namespaces they attach into, its harness pools' and its sessions'.
    ...[environment.namespace, branchSessionsNamespace(name)].flatMap(nodeAgentPodReaders),
  ].map((object) => branchLabelled(object, name));
}
