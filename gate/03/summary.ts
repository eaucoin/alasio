/**
 * 03's gate's numbers, from what gate/03/run.sh wrote to `dir`, as Markdown: the broker's
 * memory and CPU against 03's budget, the bucket's churn, SeaweedFS's S3 requests by
 * method, the latency, the restart's losses, the ETags, and what AutoMQ logged about its
 * compactions and S3.
 *
 *   node gate/03/summary.ts <dir>
 */
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

const dir = process.argv[2];
if (!dir) throw new Error("usage: node gate/03/summary.ts <dir>");
const read = (name: string) => (existsSync(join(dir, name)) ? readFileSync(join(dir, name), "utf8") : "");

/** 03's budget for the log, in MiB (program/budgets.md). */
const BUDGET_MIB = 3072;

const rows = read("samples.csv").trim().split("\n").slice(1).map((line) => {
  const [time = "", current, anon, rss, cpu, seaweed, objects, bytes] = line.split(",");
  const number = (value: string | undefined) => (value === undefined || value === "" || value === "None" ? NaN : Number(value));
  return { time: Date.parse(time), current: number(current), anon: number(anon), rss: number(rss), cpu: number(cpu), seaweed: number(seaweed), objects: number(objects), bytes: number(bytes) };
});

const finite = (values: number[]) => values.filter(Number.isFinite);
const stats = (values: number[]) => {
  const sorted = finite(values).sort((a, b) => a - b);
  if (sorted.length === 0) return "n/a";
  const at = (q: number) => sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
  return `median ${at(0.5)}, p95 ${at(0.95)}, max ${sorted.at(-1)}`;
};

// CPU in millicores between consecutive samples of the same broker (its counter restarts with it).
const cpu: number[] = [];
for (let i = 1; i < rows.length; i++) {
  const [a, b] = [rows[i - 1], rows[i]];
  if (a && b && Number.isFinite(a.cpu) && Number.isFinite(b.cpu) && b.cpu >= a.cpu) cpu.push(Math.round((b.cpu - a.cpu) / 1000 / ((b.time - a.time) / 1000)));
}

/** SeaweedFS's S3 request counters, summed by their labels but the bucket's. */
const requests = (text: string) => {
  const counts = new Map<string, number>();
  for (const line of text.split("\n")) {
    const match = /^SeaweedFS_s3_request_total\{(.*)\} ([0-9.e+]+)$/u.exec(line);
    if (!match?.[1] || !match[2]) continue;
    const labels = match[1].split(",").filter((label) => !label.startsWith("bucket=")).join(",");
    counts.set(labels, (counts.get(labels) ?? 0) + Number(match[2]));
  }
  return counts;
};
const start = requests(read("seaweedfs-metrics-start.txt"));
const end = requests(read("seaweedfs-metrics-end.txt"));
const minutes = rows.length > 1 ? ((rows.at(-1)?.time ?? 0) - (rows[0]?.time ?? 0)) / 60000 : NaN;
const deltas = [...end].map(([labels, count]) => [labels, count - (start.get(labels) ?? 0)] as const).filter(([, n]) => n > 0).sort((a, b) => b[1] - a[1]);

const logs = read("automq-before-restart.log") + read("automq-after-restart.log");
const compactions = logs.split("\n").filter((line) => /compact/iu.test(line) && !/(ERROR|WARN)/u.test(line));
const s3Lines = logs.split("\n").filter((line) => /(ERROR|WARN)/u.test(line) && /(S3|s3|ETag|UploadPartCopy|uploadPartCopy|compact|MultiPart|ObjectStorage)/u.test(line));

const memory = finite(rows.map((row) => row.current));
const peak = Math.max(...memory);
console.log(`## 03: AutoMQ 1.7.4 at "Tiny" on the bundled SeaweedFS

Budget: ${BUDGET_MIB} MiB (the pod's memory limit). Peak cgroup memory ${peak} MiB: **${peak <= BUDGET_MIB ? "within" : "over"}** budget.

| Measure (MiB, every 30 s over ${minutes.toFixed(0)} min) | |
|---|---|
| broker cgroup memory (page cache included) | ${stats(rows.map((row) => row.current))} |
| broker anonymous memory | ${stats(rows.map((row) => row.anon))} |
| JVM resident set | ${stats(rows.map((row) => row.rss))} |
| broker CPU (millicores) | ${stats(cpu)} |
| SeaweedFS working set | ${stats(rows.map((row) => row.seaweed))} |
| objects in the bucket | ${stats(rows.map((row) => row.objects))} |
| bytes in the bucket | ${stats(rows.map((row) => row.bytes))} |

SeaweedFS S3 requests over the run (${minutes.toFixed(0)} min), by method and status:

${deltas.map(([labels, n]) => `- ${labels}: ${n} (${(n / (minutes * 60)).toFixed(2)}/s)`).join("\n") || "- none recorded"}

Events:

\`\`\`
${read("events.log").trim()}
\`\`\`

Bulk produce: ${read("bulk-produce.txt").trim()}

Latency (kafka-e2e-latency, acks=all):

\`\`\`
${read("latency.txt").trim()}
\`\`\`

lake-intake's lag before the restart:

\`\`\`
${read("lag-before-restart.txt").trim()}
\`\`\`

What SeaweedFS answers to the copies compaction makes (gate/03/etag.sh):

\`\`\`
${read("etag.txt").trim()}
\`\`\`

AutoMQ's lines about compaction: ${compactions.length}, the last of them:

\`\`\`
${compactions.slice(-10).join("\n")}
\`\`\`

AutoMQ's WARN/ERROR lines about S3, compaction or ETags: ${s3Lines.length}

\`\`\`
${s3Lines.slice(0, 30).join("\n")}
\`\`\`
`);
