/**
 * The release's objects of the bundled object store, as alasio up would apply them from
 * the operator's configuration (its `install`), for 08's cut-over (gate/08/cutover.sh) to
 * apply before `alasio upgrade` does the rest: SeaweedFS's parts, the filer store, the
 * lifecycle pass and the filer store's backups (cli/src/manifests/object-store.ts); the
 * NetworkPolicies of the parts, the gateways and the filer store, and the data stack's, which
 * the release's leaves the parts out of; and the PriorityClasses they run under, which a
 * cluster an earlier release made has already. Prints a List for kubectl; or, with
 * --csi-namespace, the namespace of the JuiceFS driver, whose mount pods the cut-over waits
 * to be gone.
 *
 *   node gate/08/render.ts [--csi-namespace] [config.json]
 */
import { readFileSync } from "node:fs";

import { Result } from "effect";

import { installConfigOf } from "../../cli/src/config.ts";
import { componentName } from "../../cli/src/manifests/common.ts";
import { networkPolicyObjects } from "../../cli/src/manifests/network-policies.ts";
import { FILER, FILER_STORE, GATEWAY, MASTER, objectStoreObjects, RESTORE_SOURCE, VOLUME_SERVER } from "../../cli/src/manifests/object-store.ts";
import { priorityClassObjects } from "../../cli/src/manifests/priorities.ts";

const csi = process.argv[2] === "--csi-namespace";
const path = process.argv[csi ? 3 : 2];
const install: Record<string, unknown> = path ? (JSON.parse(readFileSync(path, "utf8")) as { install?: Record<string, unknown> }).install ?? {} : {};
const config = Result.getOrThrow(installConfigOf(install, { claude: false, shared: false }));
const policies = new Set([MASTER, VOLUME_SERVER, FILER, FILER_STORE, RESTORE_SOURCE, GATEWAY, componentName("neon")]);

if (csi) {
  console.log(config.workspaceStorage.csi.namespace);
} else {
  console.log(JSON.stringify({
    apiVersion: "v1",
    kind: "List",
    items: [
      ...priorityClassObjects(config),
      ...networkPolicyObjects(config).filter(({ metadata }) => policies.has(metadata?.name ?? "")),
      ...objectStoreObjects(config),
    ],
  }, null, 2));
}
