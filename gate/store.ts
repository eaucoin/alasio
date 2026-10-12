/**
 * The bundled object store as alasio deploys it, for a gate's scratch cluster: alasio's own
 * objects for SeaweedFS (cli/src/manifests/object-store.ts) from the default configuration,
 * its identities as the stack's setup makes them (neon/control/secrets.ts), and one of the
 * gate's own, on a bucket of its own, as an issue's change set would add them. Prints a
 * List for kubectl; the gate's credentials are in the Secret `gate-s3`, as `accessKey`,
 * `secretKey` and `bucket`, and its bucket is made by `gate/store.sh`.
 *
 *   node gate/store.ts <bucket> | kubectl apply -f -
 */
import { randomBytes } from "node:crypto";

import { Result } from "effect";

import { NAMESPACE, neonName } from "../cli/src/manifests/common.ts";
import { decodeInstallConfig } from "../cli/src/manifests/config.ts";
import { objectStoreObjects } from "../cli/src/manifests/object-store.ts";
import { completeSecrets, s3Identities } from "../neon/control/secrets.ts";

const bucket = process.argv[2];
if (!bucket) throw new Error("usage: node gate/store.ts <bucket>");

const config = Result.getOrThrow(decodeInstallConfig({ alasio: { telegram: { existingSecret: "gate-telegram" } } }));
const { secrets } = completeSecrets();
const { buckets } = config.objectStore;
const gate = { accessKey: `gate${randomBytes(6).toString("hex")}`, secretKey: randomBytes(24).toString("base64url") };
const identities = {
  identities: [
    ...s3Identities(secrets, { neon: buckets.neon, lake: buckets.lake, workspaces: buckets.workspaces }).identities,
    { name: "gate", credentials: [gate], actions: [`Read:${bucket}`, `List:${bucket}`, `Tagging:${bucket}`, `Write:${bucket}`] },
  ],
};

const secret = (name: string, data: Record<string, string>) => ({
  apiVersion: "v1",
  kind: "Secret",
  metadata: { name, namespace: NAMESPACE },
  stringData: data,
});

console.log(JSON.stringify({
  apiVersion: "v1",
  kind: "List",
  items: [
    { apiVersion: "v1", kind: "Namespace", metadata: { name: NAMESPACE } },
    secret(neonName("seaweedfs"), { "s3.json": JSON.stringify(identities, null, 2) + "\n" }),
    secret(neonName("s3-admin"), { AWS_ACCESS_KEY_ID: secrets.s3.admin.accessKey, AWS_SECRET_ACCESS_KEY: secrets.s3.admin.secretKey }),
    secret("gate-s3", { ...gate, bucket }),
    ...objectStoreObjects(config),
  ],
}, null, 2));
