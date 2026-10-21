// `alasio setup`'s entry point (./program.ts setUpState), run from alasio's image as alasio's
// pod is, with its environment: it makes alasio's state, then exits, saying why where it could not.
import { Cause, Effect } from "effect";

import { loadAlasioConfig } from "../config.ts";
import { runAlasioOnce } from "../shared/run.ts";
import { setUpState } from "./program.ts";

runAlasioOnce(Effect.suspend(() => setUpState(loadAlasioConfig())).pipe(
  Effect.tapCause((cause) => Effect.logError(`alasio's state could not be made: ${Cause.pretty(cause)}`)),
));
