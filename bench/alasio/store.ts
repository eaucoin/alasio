/**
 * The store's own measures, as the bounds read them (BENCH_MODE=store).
 */
import { writeFileSync } from "node:fs";
import { dirname, join } from "node:path";

import type { V1Pod } from "@kubernetes/client-node";

import { NAMESPACE } from "../../cli/src/manifests/common.ts";
import { kube, nodeAgents, onNode } from "../../test/e2e/harness.ts";
import { inAgentNetwork } from "./node.ts";
import { BENCH_DIR, COMMON, PROFILE } from "./settings.ts";

/** The CPU time the cgroup of the pod of uid `uid` on `node` has used, in microseconds, or null. */
async function podCpuUs(node: string, uid: string): Promise<number | null> {
  const printed = await onNode(node, [
    `dir=$(find /sys/fs/cgroup -maxdepth 6 -type d \\( -name '*pod${uid}*' -o -name '*pod${uid.replaceAll("-", "_")}*' \\) | head -n 1)`,
    `[ -n "$dir" ] && sed -n 's/^usage_usec //p' "$dir/cpu.stat"`,
  ].join("; ")).catch(() => "");
  return printed.trim() === "" ? null : Number(printed.trim());
}

/** The CPU time each of the store's pods, and each node agent, has used, in microseconds, by pod name. */
async function storeCpu(node: string): Promise<Record<string, number | null>> {
  const pods = (await kube.list<V1Pod>("Pod", { namespace: NAMESPACE })).filter(({ metadata }) => /seaweedfs|node-agent|valkey/u.test(metadata?.name ?? ""));
  return Object.fromEntries(await Promise.all(pods.map(async (pod) => [pod.metadata?.name ?? "", await podCpuUs(node, pod.metadata?.uid ?? "")] as const)));
}

/** `run` timed, with the CPU each store pod used meanwhile, in seconds. */
async function withCpu<T>(node: string, run: () => Promise<T>): Promise<{ readonly said: T; readonly seconds: number; readonly cpu_s: Record<string, number | null> }> {
  const before = await storeCpu(node);
  const started = Date.now();
  const said = await run();
  const seconds = (Date.now() - started) / 1000;
  const after = await storeCpu(node);
  const cpu = Object.fromEntries(Object.entries(after).map(([name, used]) => [name, used === null || before[name] === null || before[name] === undefined ? null : (used - (before[name] ?? 0)) / 1e6]));
  return { said, seconds, cpu_s: cpu };
}

/**
 * The store's own measures, for the job's profile, in place of its runs (BENCH_MODE=store): the
 * sweep of alasio's store, connections by range size (bench/common/store/sweep.ts), from the node
 * agent's network, three rounds, with the CPU its pods used, and its conditional PUTs; and the same
 * of a single SeaweedFS server on the runner (bench/common/store/up.sh), through the same profile,
 * which the bounds compare it with. Each in logs/store-<what>.json.
 */
export async function storeMeasures(): Promise<void> {
  const [node = ""] = (await nodeAgents()).keys();
  const write = (what: string, value: unknown) => writeFileSync(join(BENCH_DIR, "logs", `store-${what}.json`), `${JSON.stringify(value, null, 1)}\n`);
  const sweepScript = new URL("dist/sweep.mjs", COMMON).pathname;
  for (const round of [1, 2, 3]) {
    write(`sweep-alasio-${round}`, await withCpu(node, async () => JSON.parse(await inAgentNetwork(node, [sweepScript, "sweep", "--prefix", `bench-sweep-${round}/`]))));
  }
  write("conditional-alasio", await withCpu(node, async () => JSON.parse(await inAgentNetwork(node, [sweepScript, "conditional", "--prefix", "bench-conditional/"]))));
  // A single server on the runner, its profile shaped by its own toxiproxy.
  const single = join(BENCH_DIR, "single-store");
  await onNode(node, `PATH=${dirname(process.execPath)}:$PATH ${new URL("store/up.sh", COMMON).pathname} ${single} ${PROFILE} > ${BENCH_DIR}/logs/single-store-up.txt 2>&1`);
  try {
    for (const round of [1, 2, 3]) {
      const started = Date.now();
      const cpuBefore = await onNode(node, "cat /sys/fs/cgroup/system.slice/docker-$(docker inspect --format '{{.Id}}' bench-seaweedfs).scope/cpu.stat | sed -n 's/^usage_usec //p'");
      const said = await onNode(node, `set -a; . ${single}/store.env; set +a; ${process.execPath} ${sweepScript} sweep --prefix bench-sweep-${round}/`);
      const cpuAfter = await onNode(node, "cat /sys/fs/cgroup/system.slice/docker-$(docker inspect --format '{{.Id}}' bench-seaweedfs).scope/cpu.stat | sed -n 's/^usage_usec //p'");
      write(`sweep-single-${round}`, { said: JSON.parse(said.trim().split("\n").at(-1) ?? "null"), seconds: (Date.now() - started) / 1000, cpu_s: { "bench-seaweedfs": (Number(cpuAfter) - Number(cpuBefore)) / 1e6 } });
    }
    write("conditional-single", JSON.parse((await onNode(node, `set -a; . ${single}/store.env; set +a; ${process.execPath} ${sweepScript} conditional`)).trim().split("\n").at(-1) ?? "null"));
  } finally {
    await onNode(node, "docker rm -f bench-seaweedfs bench-toxiproxy >/dev/null 2>&1 || true");
  }
}
