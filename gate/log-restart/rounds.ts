/**
 * The log's restart gate's rounds (gate/log-restart/run.sh): ROUNDS rounds, each 40 spans
 * exported one at a time to the collector's gateway, each its own `gate.seq`; then the
 * broker stopped one of three ways, in turn, and started again, as the exports go on until
 * 20 more are taken. Then otel.spans read whole, from each partition's start to its end, and
 * each span counted: taken and not there, lost; there more than once, a duplicate. What it
 * finds, by way of stopping, it says and writes to OUT/results.json; it fails only where it
 * cannot run.
 */
import { writeFileSync } from "node:fs";
import { join } from "node:path";

import { describeLocal, docker, exported, held, local, OUT, say, segments, sleep, tier } from "./spans.ts";

const ROUNDS = Number(process.env["ROUNDS"] ?? 18);
/** Whether the broker tiers its segments: each round then goes on until one more is in the tier before the broker is stopped. */
const TIERED = process.env["TIERED"] === "1";

/** The ways a broker is stopped, as Docker stops its container: with its grace, as a pod deleted is; cut short, as a pod deleted at once is; killed. */
const WAYS = { graceful: ["stop", "-t", "120", "broker"], "cut short": ["stop", "-t", "2", "broker"], killed: ["kill", "broker"] } as const;
type Way = keyof typeof WAYS;

const taken = new Map<number, { readonly round: number; readonly way: Way; readonly during: boolean }>();
let seq = 0;
let tiered = 0;
for (let round = 1; round <= ROUNDS; round += 1) {
  const way = (Object.keys(WAYS) as Way[])[(round - 1) % 3] ?? "graceful";
  // Before: each taken, the broker serving; one refused waits for it, as after the round
  // before. A tiered broker's round goes on until a segment more is in the tier.
  const deadline = Date.now() + 300_000;
  for (let before = 0; before < 40 || (TIERED && segments(await tier()).length <= tiered); seq += 1) {
    if (Date.now() > deadline) throw new Error(`round ${round}: no segment more went to the tier in five minutes`);
    if (await exported(seq)) {
      taken.set(seq, { round, way, during: false });
      before += 1;
    } else await sleep(1000);
    if (before >= 40) await sleep(250);
  }
  if (TIERED) {
    tiered = segments(await tier()).length;
    say(`round ${round}: ${tiered} segments in the tier as the broker is stopped`);
  }
  const began = Date.now();
  const restarted = docker(WAYS[way]).then(() => docker(["start", "broker"]));
  let after = 0;
  let done = false;
  void restarted.then(() => {
    done = true;
  });
  for (const deadline = Date.now() + 600_000; after < 20 || !done; seq += 1) {
    if (Date.now() > deadline) throw new Error(`round ${round}: the gateway took ${after} exports in ten minutes of the broker ${way}`);
    if (await exported(seq)) {
      taken.set(seq, { round, way, during: true });
      after += 1;
    } else await sleep(250);
  }
  say(`round ${round}: the broker ${way}, back in ${Math.round((Date.now() - began) / 1000)} s; ${after} exports taken across it`);
}

if (TIERED) {
  // The rounds' first segments gone from the volume, by the topic's local retention, so they are read from the tier.
  for (const deadline = Date.now() + 180_000; Date.now() < deadline; await sleep(5000)) {
    const held = await local();
    if ([...held.values()].every((bases) => Math.min(...bases) > 0)) break;
  }
  say(`before otel.spans is read: ${segments(await tier()).length} segments in the tier; on the volume, ${describeLocal(await local())}`);
}
say("otel.spans read whole");
const { counts, unread } = await held();
if (unread.length > 0) throw new Error(`otel.spans was not read to its end: partitions ${unread.join(", ")} left`);
const results = Object.fromEntries((Object.keys(WAYS) as Way[]).map((way) => {
  const ours = [...taken].filter(([, at]) => at.way === way);
  const lost = ours.filter(([number]) => !counts.has(number)).map(([number, at]) => ({ seq: number, round: at.round, during: at.during }));
  const twice = ours.filter(([number]) => (counts.get(number) ?? 0) > 1).map(([number]) => ({ seq: number, times: counts.get(number) }));
  return [way, { rounds: new Set(ours.map(([, at]) => at.round)).size, taken: ours.length, lost, duplicates: twice }];
}));
writeFileSync(join(OUT, "results.json"), `${JSON.stringify(results, null, 2)}\n`);
writeFileSync(join(OUT, "next-seq"), `${seq}\n`);
for (const [way, { rounds, taken: count, lost, duplicates }] of Object.entries(results)) {
  say(`the broker ${way}, ${rounds} rounds: ${count} spans taken, ${lost.length} lost${lost.length ? ` (${lost.map(({ seq: number }) => number).join(", ")})` : ""}, ${duplicates.length} written twice or more`);
}
