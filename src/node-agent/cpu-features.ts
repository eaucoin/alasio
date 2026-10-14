// The node agent's first step, run as its pod starts (cli/src/manifests/node-agent.ts):
// the CPU features the node has under gVisor's release its argument names, as runsc
// says them, written as its container's termination message, which the pod's status
// keeps for `alasio up` to hold sessions' sandboxes to the features every node has, and
// for `alasio status` to say which node lacks them (cli/src/cpu-baseline.ts).
import { writeFile } from "node:fs/promises";

import { NodeRuntime } from "@effect/platform-node";
import { Effect } from "effect";

import { AlasioLoggerLayer } from "../shared/log.ts";
import { CONTAINERD, type CpuFeaturesReport, GVISOR_RELEASES, RUNSC_ROOT } from "./names.ts";
import { makeRunsc } from "./runsc.ts";

/** Where Kubernetes reads a container's termination message from, as it does unless told. */
const TERMINATION_LOG = "/dev/termination-log";

const [release] = process.argv.slice(2);

const report = Effect.gen(function*() {
  if (!release) return yield* Effect.fail(new Error("no gVisor release is named"));
  const runsc = makeRunsc({ releasesDir: GVISOR_RELEASES, containerdDir: CONTAINERD, root: RUNSC_ROOT });
  const cpuFeatures = yield* runsc.cpuFeatures(release);
  const reported: CpuFeaturesReport = { release, cpuFeatures };
  yield* Effect.promise(() => writeFile(TERMINATION_LOG, JSON.stringify(reported)));
  yield* Effect.logInfo(`under gVisor's ${release}, this node has the CPU features ${cpuFeatures.join(",")}`);
});

report.pipe(Effect.provide(AlasioLoggerLayer), NodeRuntime.runMain);
