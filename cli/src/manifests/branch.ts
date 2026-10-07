/**
 * A branch environment's objects (`alasio branch create`): a different alasio, on a
 * copy-on-write copy of main's data, with a compute on the branch's timeline of Neon's
 * and a lake on the branch's copy of the lake's catalog, in a namespace of its own, and
 * its sessions in another; named as main's are, and found, as they are deleted, by those
 * namespaces and BRANCH_LABEL.
 *
 * Its configuration is main's, with the operator's overrides, and never the host
 * profile: a folder is the machine's own files, which a branch cannot copy. Its alasio's
 * identity reaches its own sessions alone; main's alasio may make Sandboxes there, and
 * read their claims, to fork into it the sessions the branch inherited
 * (src/branch/fork.ts). Its Secrets are the command's to write: the database's, the
 * lake's and the compute's, as main's (the data is a copy, its roles and passwords main's);
 * its own bot's; Claude Code's token, when main has one; and its token to ask main to fork
 * with.
 */
import type { KubernetesObject, V1Role, V1RoleBinding } from "@kubernetes/client-node";

import { branchNamespace } from "../../../src/branch/names.ts";
import { alasioObjects, sessionsNamespace } from "./alasio.ts";
import { BRANCH_LABEL, componentName, type Environment, labels, NAMESPACE, RELEASE } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { lakeObjects } from "./lake.ts";
import { compute } from "./neon.ts";
import { networkPolicyObjects } from "./network-policies.ts";

/** The environment of the branch `name`. */
export const branchEnvironment = (name: string): Environment => ({ namespace: branchNamespace(name), branch: name });

/** `object`, labelled as the branch `name`'s. */
function branchLabelled(object: KubernetesObject, name: string): KubernetesObject {
  return { ...object, metadata: { ...object.metadata, labels: { ...object.metadata?.labels, [BRANCH_LABEL]: name } } };
}

/** What main's alasio may do in the branch's sessions' namespace: make the forks of the sessions the branch inherited, and wait for their claims. */
function parentAccess(config: InstallConfig, environment: Environment): KubernetesObject[] {
  const metadata = { name: componentName("parent"), namespace: sessionsNamespace(config, environment), labels: labels("alasio") };
  const role: V1Role = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "Role",
    metadata,
    rules: [
      { apiGroups: ["agents.x-k8s.io"], resources: ["sandboxes"], verbs: ["create", "delete"] },
      { apiGroups: [""], resources: ["persistentvolumeclaims"], verbs: ["get"] },
    ],
  };
  const binding: V1RoleBinding = {
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "RoleBinding",
    metadata,
    subjects: [{ kind: "ServiceAccount", name: RELEASE, namespace: NAMESPACE }],
    roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: metadata.name },
  };
  return [role, binding];
}

/** The objects of the branch environment `name`, of the configuration `config` (main's, overridden, without the host profile). */
export function branchObjects(config: InstallConfig, name: string): KubernetesObject[] {
  const environment = branchEnvironment(name);
  return [
    ...alasioObjects(config, environment),
    ...compute(config, environment),
    ...lakeObjects(config, environment),
    ...networkPolicyObjects(config, environment),
    ...(config.sessions.enabled ? parentAccess(config, environment) : []),
  ].map((object) => branchLabelled(object, name));
}
