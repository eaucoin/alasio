/**
 * Run inside alasio's pod, by node there, with a session's volume id (inAlasio in
 * harness.ts): brings the session up as a turn does, through alasio's own session
 * filesystems and its ServiceAccount's permissions, resuming it when it is suspended and
 * restarting it when its workspace's mount is broken. Given `anywhere`, the sessions
 * template's node selector is left out, so the session's pod may run on any node that
 * takes it. Prints one JSON line of the pod it runs in afterwards.
 *
 * Its Sandbox is made from what this script, without alasio's telemetry settings, makes
 * of the template: the first bring-up here moves a session alasio made onto that, and
 * later ones find it on its own template, where nothing but a fault restarts it.
 *
 * alasio's modules are imported by their paths in the repository, which inAlasio in
 * harness.ts rewrites to the image's as it pipes this in.
 */
import type { V1Pod } from "@kubernetes/client-node";
import { Effect } from "effect";

import { KubeClient } from "../../src/kube/client.ts";
import { loadKubeTemplates, type SessionsProfile } from "../../src/kube/config.ts";
import { SessionSandboxes } from "../../src/sandbox/index.ts";

/** The pod the session runs in once it is up. */
export interface BroughtUp {
  readonly uid: string | undefined;
  readonly node: string | undefined;
}

const [volumeId, where] = process.argv.slice(2);
if (!volumeId || (where !== undefined && where !== "anywhere")) throw new Error("usage: session-bring-up.ts <volumeId> [anywhere]");

/** `profile` without its node selector, when the session may run anywhere. */
function placed(profile: SessionsProfile): SessionsProfile {
  if (where !== "anywhere") return profile;
  const podTemplate = structuredClone(profile.podTemplate ?? {});
  delete podTemplate.spec?.nodeSelector;
  return { ...profile, podTemplate };
}

const seen = await Effect.runPromise(Effect.gen(function*() {
  const { sessions } = yield* loadKubeTemplates;
  if (!sessions) return yield* Effect.die(new Error("the installation gives no sessions template"));
  const profile = placed(sessions);
  return yield* Effect.gen(function*() {
    yield* (yield* SessionSandboxes).ensureSession(volumeId);
    // A core Pod, as the API server returns one.
    const pod = (yield* (yield* KubeClient).read("v1", "Pod", profile.namespace, volumeId)) as V1Pod | null;
    const broughtUp: BroughtUp = { uid: pod?.metadata?.uid, node: pod?.spec?.nodeName };
    return broughtUp;
  }).pipe(Effect.provide(SessionSandboxes.layer({ profile, stateDir: "/tmp/bring-up", env: {} })));
}).pipe(Effect.scoped, Effect.provide(KubeClient.layer)));
console.log(JSON.stringify(seen));
