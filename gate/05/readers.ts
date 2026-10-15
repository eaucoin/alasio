/**
 * 05's gate, readers in parallel: whether several query endpoints (neon/lake/src/endpoint.ts),
 * each with its own DuckDB, read one lake at once, so a slow query on one holds no query on
 * another, and what each takes of memory meanwhile.
 *
 * The lake is the box's shape: a `claude.entries` of 160 641 entries of about 3 KB, merged
 * into one file as the box's nightly maintenance left it, and a day's small loads beside
 * it; and a day of the stack's telemetry, some 30 metric points a second, in the hour's
 * files the merge leaves. Its catalog is a throwaway PostgreSQL 17 (test/support/postgres.ts),
 * its files a local directory, its source written as alasio's session store writes it.
 * Each reader is the endpoint's own process, with the lake's reader's role and its
 * DuckDB's default memory limit, as its container runs it.
 *
 *   node gate/05/readers.ts
 *
 * GATE_SCALE shrinks the lake (1, the box's, by default); GATE_OUT is where the summary
 * goes, as JSON and as a table.
 */
import { type ChildProcess, spawn } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

import pg from "pg";

import { loadConfig } from "../../neon/lake/src/config.ts";
import { LAKE, openLake } from "../../neon/lake/src/lake.ts";
import { grantReads } from "../../neon/lake/src/reader.ts";
import { prepareLake, syncLake } from "../../neon/lake/src/sync.ts";
import { NeonSessionStore } from "../../src/harness/claude/session-store.ts";
import { ensureLakeReaderRole, ensureLakeRole, syncLakeReads } from "../../src/neon/lake.ts";
import { startPostgres } from "../../test/support/postgres.ts";

const SCALE = Number(process.env["GATE_SCALE"] ?? "1");
const OUT = process.env["GATE_OUT"] ?? join(tmpdir(), "gate-05");
/** The box's history, in one file, and a day's loads beside it. */
const ENTRIES = Math.round(160_641 * SCALE);
const DAY_LOADS = 9;
const DAY_LOAD_ENTRIES = Math.max(1, Math.round(65 * SCALE));
/** A day of the stack's metric points, some 30 a second, in an hour's files each. */
const POINTS = Math.round(2_600_000 * SCALE);
const HOURS = 24;
const LOG_RECORDS = Math.round(200_000 * SCALE);
const READER_PORTS = [18_090, 18_091];
const TOKEN = "gate-token";
const LAKE_PASSWORD = "gate-lake";
const READER_PASSWORD = "gate-reader";
/** How many times each case runs. */
const RUNS = 3;

const log = (message: string, fields: Record<string, unknown> = {}) => console.error(JSON.stringify({ time: new Date().toISOString(), message, ...fields }));

// --- The lake, the box's shape ---------------------------------------------------

const postgres = await startPostgres();
const server = new pg.Client({ connectionString: postgres.url });
await server.connect();
await server.query("create database alasio");
await server.end();
const url = new URL(postgres.url);
url.pathname = "/alasio";
const admin = new pg.Pool({ connectionString: url.toString() });
await new NeonSessionStore(admin).ensureSchema();
await ensureLakeRole(admin, LAKE_PASSWORD);
await ensureLakeReaderRole(admin, READER_PASSWORD);
await syncLakeReads(admin, true);
const dataDir = mkdtempSync(join(tmpdir(), "gate-05-lake-"));
const env = { LAKE_DATABASE_HOST: url.hostname, LAKE_DATABASE_PORT: url.port, LAKE_DATA_PATH: `${dataDir}/` };
const config = loadConfig({ ...env, LAKE_DATABASE_PASSWORD: LAKE_PASSWORD });

/**
 * Appends `count` assistant entries of about 3 KB each, a session's hundred at a time, as
 * the session store keeps them: its JSON, its doc, alasio's mtime. Their text is digests,
 * which compress about as a transcript does: the box's 160 641 entries took 213 MB.
 */
async function appendEntries(count: number, from: number): Promise<void> {
  await admin.query(
    `insert into claude_sessions.entries (project_key, session_id, subpath, uuid, entry, mtime, doc)
     select '-home-operator-gate', 'session-' || (i / 100), '', 'u' || i, entry::json, $3, entry::jsonb
     from (
       select i, json_build_object(
         'type', 'assistant', 'uuid', 'u' || i, 'timestamp', to_char(now() - make_interval(secs => $1 + $2 - i), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
         'sessionId', 'session-' || (i / 100), 'cwd', '/home/operator/gate',
         'message', json_build_object('id', 'msg_' || i, 'role', 'assistant', 'model', 'claude-opus-5-5',
           'content', json_build_array(json_build_object('type', 'text', 'text', (select string_agg(md5(i || '.' || k), '') from generate_series(1, 90) as k))),
           'usage', json_build_object('input_tokens', i % 977, 'output_tokens', i % 313)))::text as entry
       from generate_series($1::bigint, $1::bigint + $2::bigint - 1) as i
     ) entries`,
    [from, count, Date.now()],
  );
}

const started = performance.now();
for (let from = 0; from < ENTRIES; from += 20_000) await appendEntries(Math.min(20_000, ENTRIES - from), from);
log("source written", { entries: ENTRIES, seconds: (performance.now() - started) / 1000 });

const lake = await openLake(config, { source: config.source });
await prepareLake(lake.db);
await syncLake(lake.db);
// One file of the history, as the nightly merge left the box's before its target size was 64 MB.
await lake.db.run(`call ${LAKE}.set_option('target_file_size', '1GB')`);
await lake.db.run(`call ducklake_merge_adjacent_files('${LAKE}')`);
await prepareLake(lake.db);
for (let load = 0; load < DAY_LOADS; load += 1) {
  await appendEntries(DAY_LOAD_ENTRIES, ENTRIES + load * DAY_LOAD_ENTRIES);
  await syncLake(lake.db);
}
// A day of metric points and log records, an hour's in each file, as the hourly merge leaves them.
const services = ["pageserver", "safekeeper", "compute", "storage-controller", "proxy", "lake", "seaweedfs", "redis", "alasio", "grafana"];
const servicesList = `[${services.map((service) => `'${service}'`).join(", ")}]`;
for (let hour = 0; hour < HOURS; hour += 1) {
  const points = Math.round(POINTS / HOURS);
  await lake.db.run(`
    insert into ${LAKE}.otel.metrics_gauge (ServiceName, MetricName, Attributes, ResourceAttributes, TimeUnix, Value)
    select ${servicesList}[1 + i % 10], 'gate_metric_' || (i % 300), map {'pod': 'pod-' || (i % 7)}, map {'service.namespace': 'alasio-neon'},
      (now() - interval ${HOURS - hour} hour + to_microseconds((i * 3600000000 // ${points})::bigint))::timestamp_ns, (i % 1000) / 10
    from range(${points}) as r(i)`);
  const records = Math.round(LOG_RECORDS / HOURS);
  await lake.db.run(`
    insert into ${LAKE}.otel.logs (Timestamp, ServiceName, SeverityText, Body, LogAttributes)
    select (now() - interval ${HOURS - hour} hour + to_microseconds((i * 3600000000 // ${records})::bigint))::timestamp_ns, ${servicesList}[1 + i % 10], 'INFO',
      'gate log record ' || i || ' ' || repeat('x', 120), map {'event': 'gate'}
    from range(${records}) as r(i)`);
}
lake.close();
const catalog = new pg.Client({ ...config.catalog });
await catalog.connect();
await grantReads(catalog);
// Each table's live files, as the catalog lists them.
const { rows: files } = await catalog.query<{ table_name: string; files: string; bytes: string }>(
  `select t.table_name, count(*) as files, sum(f.file_size_bytes) as bytes
   from ducklake.ducklake_data_file f join ducklake.ducklake_table t using (table_id)
   where f.end_snapshot is null and t.end_snapshot is null and t.table_name in ('entries', 'metrics_gauge', 'logs') group by 1`,
);
await catalog.end();
log("lake written", { seconds: (performance.now() - started) / 1000, files });

// --- The readers ----------------------------------------------------------------

/** A reader: its process, its port, and the most memory it has held since last read. */
interface Reader {
  readonly process: ChildProcess;
  readonly port: number;
  peakKiB: number;
}

/** The resident memory of `pid`, in KiB. */
const residentKiB = (pid: number): number => Number(/^VmRSS:\s+(\d+)/mu.exec(readFileSync(`/proc/${pid}/status`, "utf8"))?.[1] ?? 0);

const readers: Reader[] = READER_PORTS.map((port) => {
  const child = spawn(process.execPath, ["src/endpoint.ts"], {
    cwd: join(import.meta.dirname, "../../neon/lake"),
    env: { ...process.env, ...env, LAKE_READER_PASSWORD: READER_PASSWORD, LAKE_QUERY_TOKEN: TOKEN, LAKE_QUERY_PORT: String(port), HOME: tmpdir() },
    stdio: ["ignore", "ignore", "inherit"],
  });
  return { process: child, port, peakKiB: 0 };
});
const sampler = setInterval(() => {
  for (const reader of readers) if (reader.process.pid) reader.peakKiB = Math.max(reader.peakKiB, residentKiB(reader.process.pid));
}, 50);

/** Asks `reader` for `sql`: how long it took, in seconds, and its status. */
async function ask(reader: Reader, sql: string): Promise<{ seconds: number; status: number; text: string }> {
  const at = performance.now();
  const response = await fetch(`http://127.0.0.1:${reader.port}/query`, { method: "POST", headers: { authorization: `Bearer ${TOKEN}` }, body: sql });
  const text = await response.text();
  return { seconds: (performance.now() - at) / 1000, status: response.status, text: text.slice(0, 300) };
}

for (const reader of readers) {
  for (let attempt = 0; ; attempt += 1) {
    if ((await fetch(`http://127.0.0.1:${reader.port}/healthz`).catch(() => null))?.ok) break;
    if (attempt > 300) throw new Error(`the reader on ${reader.port} never served`);
    await sleep(100);
  }
}

/** What Grafana's alert rules ask every minute: small, and wanted at once. */
const SMALL = "select count(*) as n from otel.logs where Timestamp > now()::timestamp - interval 5 minute";
/** What a dashboard asks of a day: slow. */
const SLOW = {
  "a day of metrics, a minute at a time": "select time_bucket(interval 1 minute, TimeUnix) as t, ServiceName, avg(Value) as v from otel.metrics_gauge where TimeUnix > now()::timestamp - interval 24 hour group by all order by all limit 50000",
  "every transcript entry read": "select count(*) as n, sum(length(entry)) as bytes, count(*) filter (where entry like '%ffff%') as matching from claude.entries",
  "a search of the transcripts' text": "select count(*) as n from claude.entries where json_extract_string(entry, '$.message.content[0].text') like '%00000%'",
} as const;

const results: Record<string, unknown>[] = [];
/** Each reader's memory once it has the lake open and has answered a small query. */
for (const reader of readers) {
  const opened = await ask(reader, SMALL);
  if (opened.status !== 200) throw new Error(`the reader on ${reader.port} answered ${opened.status}: ${opened.text}`);
}
const idle = readers.map((reader) => residentKiB(reader.process.pid!));
results.push({ case: "idle, the lake open", rssMiB: idle.map((kib) => Math.round(kib / 1024)) });

for (const [name, slow] of Object.entries(SLOW)) {
  for (let run = 0; run < RUNS; run += 1) {
    const [a, b] = readers as [Reader, Reader];
    const alone = await ask(a, SMALL);
    const slowAlone = await ask(a, slow);
    // One reader, as today: the small query waits behind the slow one.
    const slowFirst = ask(a, slow);
    await sleep(200);
    const behind = await ask(a, SMALL);
    await slowFirst;
    // Two readers: the small one goes to the other.
    const slowOnA = ask(a, slow);
    await sleep(200);
    const beside = await ask(b, SMALL);
    await slowOnA;
    // The slow query on both at once: each reader's memory under it.
    for (const reader of readers) reader.peakKiB = 0;
    const both = await Promise.all(readers.map((reader) => ask(reader, slow)));
    results.push({
      case: name,
      run,
      slowSeconds: Number(slowAlone.seconds.toFixed(3)),
      slowStatus: slowAlone.status,
      smallAloneSeconds: Number(alone.seconds.toFixed(3)),
      smallBehindSlowOnOneReaderSeconds: Number(behind.seconds.toFixed(3)),
      smallOnAnotherReaderSeconds: Number(beside.seconds.toFixed(3)),
      bothSlowSeconds: both.map(({ seconds }) => Number(seconds.toFixed(3))),
      peakRssMiB: readers.map(({ peakKiB }) => Math.round(peakKiB / 1024)),
      ...(slowAlone.status === 200 ? {} : { slowAnswer: slowAlone.text }),
    });
    log("case", results.at(-1)!);
  }
}

clearInterval(sampler);
for (const reader of readers) reader.process.kill("SIGTERM");
await admin.end();
await postgres.stop();
rmSync(dataDir, { recursive: true, force: true });

mkdirSync(OUT, { recursive: true });
writeFileSync(join(OUT, "readers.json"), `${JSON.stringify({ scale: SCALE, entries: ENTRIES, points: POINTS, results }, null, 2)}\n`);
const table = [
  "| case | run | slow (s) | small alone (s) | small behind slow, one reader (s) | small on another reader (s) | both slow (s) | peak RSS each (MiB) |",
  "|---|---|---|---|---|---|---|---|",
  ...results.filter((row) => "run" in row).map((row) =>
    `| ${row["case"]} | ${row["run"]} | ${row["slowSeconds"]} | ${row["smallAloneSeconds"]} | ${row["smallBehindSlowOnOneReaderSeconds"]} | ${row["smallOnAnotherReaderSeconds"]} | ${(row["bothSlowSeconds"] as number[]).join(" / ")} | ${(row["peakRssMiB"] as number[]).join(" / ")} |`
  ),
  "",
  `Idle, the lake open: ${idle.map((kib) => `${Math.round(kib / 1024)} MiB`).join(" / ")}.`,
];
writeFileSync(join(OUT, "readers.md"), `${table.join("\n")}\n`);
console.log(table.join("\n"));
