/**
 * The environments of sessions the sandbox service serves: main's, as it is configured, and
 * each branch environment's (`alasio branch create`), found by its sessions' namespace, which
 * carries BRANCH_LABEL, its branch's name, and is named for it. They are found anew every
 * `refreshMs`, and at once when one is asked for that was not found, as a token signed for
 * one names it, so a branch made is served from its first bind, and one deleted is let go
 * of. Finds are made one at a time, an ask answered by the first begun after it, so the asks
 * made while one is under way are answered by the next, together.
 */
import { Effect, Schedule, type Scope, Semaphore } from "effect";

import { BRANCH_LABEL, branchSessionsNamespace } from "../branch/names.ts";
import { KubeClient } from "../kube/client.ts";

/** An environment's sessions: its name, and the namespace its pools are in, which names its workspaces to the node agents. */
export interface SessionsEnvironment {
  readonly name: string;
  readonly namespace: string;
}

/** The environments of sessions there are; see makeEnvironments. */
export interface Environments {
  /** Those found last. */
  readonly current: Effect.Effect<readonly SessionsEnvironment[]>;
  /** The environment `name`, found anew where it was not; null where there is none. */
  readonly named: (name: string) => Effect.Effect<SessionsEnvironment | null>;
  /** The environment whose sessions are in `namespace`, found anew where it was not; null where there is none. */
  readonly ofNamespace: (namespace: string) => Effect.Effect<SessionsEnvironment | null>;
}

/** How often the environments are found anew, by default. */
export const REFRESH_MS = 30_000;

/**
 * The branch environments whose sessions' namespaces are `namespaces`, as the API lists those
 * labelled BRANCH_LABEL: each that is named for its label's branch and is not being deleted,
 * as its branch is; the branch's other namespace (its alasio's) passed over.
 */
export function branchEnvironmentsOf(
  namespaces: readonly { readonly metadata?: { readonly name?: string; readonly labels?: Readonly<Record<string, string>>; readonly deletionTimestamp?: unknown } }[],
): SessionsEnvironment[] {
  return namespaces.flatMap(({ metadata }) => {
    const branch = metadata?.labels?.[BRANCH_LABEL];
    return branch && metadata?.name === branchSessionsNamespace(branch) && metadata.deletionTimestamp === undefined ? [{ name: branch, namespace: metadata.name }] : [];
  });
}

/** The environments, as the module says: `main`, and the branch environments the KubeClient finds. */
export const makeEnvironments = Effect.fnUntraced(function*(
  { main, refreshMs = REFRESH_MS }: { readonly main: SessionsEnvironment; readonly refreshMs?: number },
): Effect.fn.Return<Environments, never, KubeClient | Scope.Scope> {
  const kube = yield* KubeClient;
  let found: readonly SessionsEnvironment[] = [main];
  const finding = yield* Semaphore.make(1);
  /** How many finds have begun, so an ask that one begun after it answers asks no more. */
  let begun = 0;

  /** Finds the environments anew; one that fails leaves those found last. */
  const find = Effect.gen(function*() {
    begun += 1;
    const namespaces = yield* kube.list("v1", "Namespace", "", BRANCH_LABEL);
    found = [main, ...branchEnvironmentsOf(namespaces).filter(({ namespace }) => namespace !== main.namespace)];
  }).pipe(Effect.catch((error) => Effect.logWarning(`finding the branch environments failed: ${error.message}`)));

  /** Finds them anew, unless a find begun since it was asked has found them, as of every ask that waited on it. */
  const findAnew = Effect.suspend(() => {
    const asked = begun;
    return Effect.suspend(() => (begun > asked ? Effect.void : find)).pipe(Semaphore.withPermit(finding));
  });

  /** What `match` finds of those found, found anew where none is. */
  const lookup = (match: (environment: SessionsEnvironment) => boolean) =>
    Effect.gen(function*() {
      const known = found.find(match);
      if (known) return known;
      yield* findAnew;
      return found.find(match) ?? null;
    });

  yield* findAnew;
  yield* Effect.forkScoped(Effect.repeat(findAnew, Schedule.spaced(refreshMs)));
  return {
    current: Effect.sync(() => found),
    named: (name) => lookup((environment) => environment.name === name),
    ofNamespace: (namespace) => lookup((environment) => environment.namespace === namespace),
  };
});
