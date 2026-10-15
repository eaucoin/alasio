/**
 * The log's restart gate's restore (gate/log-restart/run.sh, a tiered broker's): what the
 * filer store put back (alasio restore-filer-store) does to a log whose segments are in the
 * object store, and the log made anew, as the restore makes it.
 *
 * 1. The filer store's backup: the tier's objects as they are now.
 * 2. Spans exported until segments of theirs are in the tier and gone from the volume.
 * 3. The filer store put back, the broker stopped: each object made since the backup gone.
 * 4. The log as it is then, read whole: what of it is lost, and whether it reads at all.
 * 5. The log made anew, as the restore makes it: the broker stopped, its volume and the
 *    tier emptied, the broker started and its topic made again; spans exported, and read.
 *
 * What it finds it says and writes to OUT/restore.json; it fails only where it cannot run.
 */
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { describeLocal, docker, exported, held, local, OUT, run, s3, say, segments, sleep, TIER, tier, TOPIC, topicId } from "./spans.ts";

const KAFKA_IMAGE = process.env["KAFKA_IMAGE"] ?? "";
const TOPIC_CONFIGS = (process.env["TOPIC_CONFIGS"] ?? "").split(" ").filter(Boolean);

let seq = Number(readFileSync(join(OUT, "next-seq"), "utf8"));

/** Exports `count` spans, one each 500 ms, each the next seq: the seqs taken. */
async function exportSome(count: number): Promise<number[]> {
  const taken: number[] = [];
  for (; taken.length < count; seq += 1) {
    if (await exported(seq)) taken.push(seq);
    await sleep(500);
  }
  return taken;
}

const serving = async () => {
  for (const deadline = Date.now() + 300_000; Date.now() < deadline; await sleep(3000)) {
    if ((await run("docker", ["exec", "broker", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092", "--list"])).code === 0) return;
  }
  throw new Error("the broker did not serve within five minutes");
};

// 1.
const backup = new Set(await tier());
say(`the filer store's backup: ${segments([...backup]).length} segments in the tier; on the volume, ${describeLocal(await local())}`);

// 2.
const since: number[] = [];
for (const deadline = Date.now() + 600_000; ; ) {
  since.push(...await exportSome(40));
  const made = segments(await tier()).filter((key) => !backup.has(key));
  if (made.length >= 2) {
    say(`${since.length} spans exported since the backup; ${made.length} segments of theirs in the tier`);
    break;
  }
  if (Date.now() > deadline) throw new Error("no segment went to the tier in ten minutes");
}
// Their segments gone from the volume, by the topic's local retention.
await sleep(30_000);

// 3.
await docker(["stop", "-t", "120", "broker"]);
const made = (await tier()).filter((key) => !backup.has(key));
for (const key of made) await s3(["s3api", "delete-object", "--bucket", TIER, "--key", key]);
say(`the filer store put back: ${made.length} objects made since the backup gone, ${segments(made).length} of them segments`);
await docker(["start", "broker"]);
await serving();

// 4.
const before = await held(180_000);
const lostSince = since.filter((number) => !before.counts.has(number));
say(`the log as the filer store's restore leaves it: ${lostSince.length} of the ${since.length} spans exported since the backup lost` +
  (before.unread.length > 0 ? `; partitions ${before.unread.join(", ")} not read to their end` : "; read to its end"));
const errors = (await run("sh", ["-c", "docker logs broker 2>&1 | grep -E -i 'NoSuchKey|RemoteResourceNotFound|RemoteStorageException|not found' | sort | uniq -c | sort -rn | head -5"])).stdout.trim();
if (errors) say(`the broker's errors reading it:\n${errors}`);

// 5.
const idBefore = await topicId();
await docker(["stop", "-t", "120", "broker"]);
await docker(["run", "--rm", "--user", "0", "--entrypoint", "sh", "-v", `${join(OUT, "data")}:/data`, KAFKA_IMAGE, "-c", "rm -rf /data/* /data/.[!.]*"]);
await s3(["s3", "rm", `s3://${TIER}`, "--recursive", "--only-show-errors"]);
await docker(["start", "broker"]);
await serving();
const created = await run("docker", ["exec", "broker", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092", "--create", "--topic", TOPIC, "--partitions", "2",
  ...TOPIC_CONFIGS.flatMap((config) => ["--config", config])]);
if (created.code !== 0) throw new Error("otel.spans could not be made anew");
const idAfter = await topicId();
const anew = await exportSome(120);
await sleep(30_000);
const tiered = segments(await tier()).length;
const after = await held(180_000);
const lostAnew = anew.filter((number) => !after.counts.has(number));
const twiceAnew = anew.filter((number) => (after.counts.get(number) ?? 0) > 1);
say(`the log made anew: otel.spans ${idAfter}, not ${idBefore}; ${anew.length} spans exported, ${lostAnew.length} lost, ${twiceAnew.length} written twice or more; ` +
  `${tiered} segments in the tier again` + (after.unread.length > 0 ? `; partitions ${after.unread.join(", ")} not read to their end` : ""));

writeFileSync(join(OUT, "restore.json"), `${JSON.stringify({
  putBack: { exportedSince: since.length, objectsGone: made.length, segmentsGone: segments(made).length, lost: lostSince.length, unread: before.unread, errors },
  madeAnew: { topicIds: [idBefore, idAfter], exported: anew.length, lost: lostAnew, duplicates: twiceAnew, tieredSegments: tiered, unread: after.unread },
}, null, 2)}\n`);
