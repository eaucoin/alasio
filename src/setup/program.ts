/**
 * `alasio setup`, a program of alasio's image: alasio's state made in its Neon, as alasio makes
 * it as it starts (../main.ts), and nothing more. Its roles, the stores of Claude Code's
 * transcripts and Codex's rollouts, its state's tables, a branch's state its own, and the harness
 * host's grants and the policies that hold each pool pod to its own conversation's rows: so a
 * cut-over (upgrades/) may have them made before alasio serves, and do what it does between,
 * before any pod reads by them. All of it is idempotent: alasio's own making of it, as it starts after, finds it
 * made.
 */
import { Effect, Layer, type Scope } from "effect";

import { CODEX_LOGIN_ROLE, HARNESS_ROLE } from "../../neon/control/secrets.ts";
import { stateStore } from "../alasio.ts";
import type { AlasioConfig } from "../config.ts";
import { Neon } from "../neon/connect.ts";
import { LakeWake } from "../neon/lake-wake.ts";

/** alasio's state made as `config` says, as the module says; it writes nothing the lake loads, so tells its loader of nothing. */
export const setUpState = (config: Pick<AlasioConfig, "workingDirectory" | "branch">): Effect.Effect<void, unknown, Scope.Scope> =>
  Effect.gen(function*() {
    const neon = yield* Neon.make.pipe(Effect.provideService(LakeWake, LakeWake.none));
    // The session filesystems' Codex's store, as alasio makes it before its state.
    yield* neon.sessionFsRollouts;
    yield* Layer.build(stateStore({ pool: neon.pool, workingDirectory: config.workingDirectory, branch: config.branch, harnessRole: HARNESS_ROLE, codexLoginRole: CODEX_LOGIN_ROLE }));
    yield* Effect.logInfo("alasio's state is made");
  });
