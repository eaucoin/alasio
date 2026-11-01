// The realistic Bun fixtures (repl-bun, repl-bun-checkpointed), installed into a bayma Bun session
// by the driver's setup exec, which prepends `const PARAMS = {...};`, then worked by a few execs
// (`work`), then left idle, as an agent's session is. The session loads common modules and holds
// about `sizeMib` of what such a session builds, in the JavaScript heap rather than in buffers:
// records of text, identifiers, dates and numbers, indexed in a Map, a fifth of them kept as JSON
// text and a tenth in an in-memory SQLite table, beside a numeric series. repl-bun-checkpointed
// also holds 20 MiB of other records in `$checkpoint`, which bayma serializes at the end of every
// exec. The content is pseudo-random from a seed of the run's id: words of a vocabulary made of
// syllables, drawn with a skew as words in text are, so it compresses as text does. Only the ticker
// runs in the background, in a Worker blocked until a window is measured.
const zlib = require("node:zlib");
const util = require("node:util");
const { EventEmitter } = require("node:events");
const { Database } = require("bun:sqlite");
const { serialize } = require("bun:jsc");

if (globalThis.bench) throw new Error("this session holds a fixture already");

const MIB = 1 << 20;
/** The bytes `$checkpoint` holds, serialized, in repl-bun-checkpointed. */
const CHECKPOINT_BYTES = 20 * MIB;
/** Records built between two looks at the process's memory. */
const BATCH = 2000;

const { fixture, sizeMib, runId } = PARAMS;
const checkpointed = fixture === "repl-bun-checkpointed";
const marker = new Bun.CryptoHasher("sha256").update(`${runId}/${fixture}`).digest("hex");

/** mulberry32, seeded with the marker's first word: the same content for the same run. */
let seed = Number.parseInt(marker.slice(0, 8), 16) >>> 0;
function random() {
  seed = (seed + 0x6d2b79f5) >>> 0;
  let t = seed;
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}
const below = (n) => Math.floor(random() * n);

const SYLLABLES = "ka lo mi ne ru sa ti vo ze ba de fi go hu ja ke li mo nu pa qui re so tu va we xi yo zu an el in on un ar er ir or ur al il ol ul as es is os us at et it ot ut".split(" ");
const VOCABULARY = Array.from({ length: 4000 }, () => Array.from({ length: 1 + below(4) }, () => SYLLABLES[below(SYLLABLES.length)]).join(""));
/** A word, the frequent ones far more often, as in text. */
const word = () => VOCABULARY[Math.floor(VOCABULARY.length * random() ** 3)];
const capital = (text) => text[0].toUpperCase() + text.slice(1);
const sentence = (words) => `${capital(Array.from({ length: words }, word).join(" "))}.`;
const hex = (bytes) => Array.from({ length: bytes }, () => below(256).toString(16).padStart(2, "0")).join("");
const EPOCH = Date.UTC(2024, 0, 1);

/** A record as an agent's session holds one: what it read from an API or a file, parsed. */
function record(id) {
  const first = word();
  const last = word();
  return {
    id,
    uuid: hex(16),
    name: `${capital(first)} ${capital(last)}`,
    email: `${first}.${last}@${word()}.com`,
    url: new URL(`https://${word()}.example/${word()}/${id}`).href,
    createdAt: new Date(EPOCH + below(3e10)).toISOString(),
    score: Math.round(random() * 10000) / 100,
    count: below(1000),
    tags: Array.from({ length: 2 + below(5) }, word),
    body: Array.from({ length: 1 + below(4) }, () => sentence(8 + below(24))).join(" "),
    meta: { views: below(1e6), ratio: random(), active: random() < 0.7, history: Array.from({ length: 5 + below(16) }, () => below(500)) },
  };
}

const rss = () => process.memoryUsage().rss;
const records = [];
const byId = new Map();
const docs = [];
const db = new Database(":memory:");
db.run("CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT, at TEXT, payload TEXT)");
const insert = db.prepare("INSERT INTO events (id, kind, at, payload) VALUES (?, ?, ?, ?)");
const events = new EventEmitter();
events.on("record", (made) => {
  if (made.id % 5 === 0) docs.push(JSON.stringify(made));
  if (made.id % 10 === 0) insert.run(made.id, made.tags[0], made.createdAt, made.body);
});
function add() {
  const made = record(records.length);
  records.push(made);
  byId.set(made.id, made);
  events.emit("record", made);
  return made;
}

// A tenth of the size in a numeric series, the rest in records, until the process has grown by the size.
const startRss = rss();
const series = new Float64Array(Math.max(1, Math.floor((sizeMib * MIB) / 10 / 8)));
for (let i = 1; i < series.length; i++) series[i] = series[i - 1] + random() - 0.5;
while (rss() - startRss < sizeMib * MIB) for (let i = 0; i < BATCH; i++) add();

// The checkpoint: records of its own, beside the size, as many as serialize to CHECKPOINT_BYTES.
if (checkpointed) {
  const kept = Array.from({ length: 1000 }, (_, i) => record(-1 - i));
  const each = serialize(kept).byteLength / kept.length;
  while (kept.length < CHECKPOINT_BYTES / each) kept.push(record(-1 - kept.length));
  $checkpoint = { records: kept };
}

/** The SHA-256 of everything the session holds, in order: every record, the JSON, the table, the series, the checkpoint. */
function digest() {
  const hash = new Bun.CryptoHasher("sha256");
  for (const each of records) hash.update(JSON.stringify(each));
  for (const doc of docs) hash.update(doc);
  for (const row of db.query("SELECT id, kind, at, payload FROM events ORDER BY id").values()) hash.update(JSON.stringify(row));
  hash.update(new Uint8Array(series.buffer));
  if (checkpointed) hash.update(JSON.stringify($checkpoint));
  return hash.digest("hex");
}

// The ticker: wakes every 1 ms while a window is measured and keeps the longest gap between two wakes.
const ctl = new Int32Array(new SharedArrayBuffer(8));
const counters = new Float64Array(new SharedArrayBuffer(16));
const MEASURE = 0;
const NEVER = 1;
const MAX_GAP_MS = 0;
const TICKS = 1;
const tickerSource = `
onmessage = ({ data: { ctl, counters } }) => {
  for (;;) {
    while (Atomics.load(ctl, ${MEASURE}) === 0) Atomics.wait(ctl, ${MEASURE}, 0);
    let last = performance.now();
    while (Atomics.load(ctl, ${MEASURE}) === 1) {
      Atomics.wait(ctl, ${NEVER}, 0, 1);
      const now = performance.now();
      if (now - last > counters[${MAX_GAP_MS}]) counters[${MAX_GAP_MS}] = now - last;
      counters[${TICKS}] += 1;
      last = now;
    }
  }
};`;
const ticker = new Worker(URL.createObjectURL(new Blob([tickerSource], { type: "application/javascript" })));
ticker.postMessage({ ctl, counters });

const now = () => Number(process.hrtime.bigint()) / 1e6;
let expected = digest();
let marked = null;

globalThis.bench = {
  baseline() {
    return { baseline_mib_s: 0, rss_bytes: rss() };
  },
  /**
   * One exec of an agent's work: a search, a top list, counts per tag, a report compressed; some
   * records updated and some added; the SQLite table queried. What it builds besides is garbage.
   */
  work() {
    const tag = records[below(records.length)].tags[0];
    const found = records.filter((each) => each.tags.includes(tag));
    const top = [...found].sort((a, b) => b.score - a.score).slice(0, 100);
    const perTag = new Map();
    for (const each of records) for (const t of each.tags) perTag.set(t, (perTag.get(t) ?? 0) + 1);
    const report = zlib.gzipSync(JSON.stringify({ tag, top, perTag: [...perTag].slice(0, 1000) }));
    for (let i = 0; i < records.length / 100; i++) byId.get(below(records.length)).meta.views += 1;
    for (let i = 0; i < records.length / 200; i++) add();
    const kinds = db.query("SELECT kind, count(*) AS n FROM events GROUP BY kind ORDER BY n DESC LIMIT 5").all();
    expected = digest();
    return { tag, found: found.length, report_bytes: report.byteLength, kinds: kinds.length, records: records.length, rss_bytes: rss(), preview: util.inspect(top[0]?.name) };
  },
  mark() {
    counters[MAX_GAP_MS] = 0;
    counters[TICKS] = 0;
    marked = now();
    Atomics.store(ctl, MEASURE, 1);
    Atomics.notify(ctl, MEASURE);
    return { marked: true };
  },
  gaps() {
    Atomics.store(ctl, MEASURE, 0);
    if (marked === null) throw new Error("no window was marked");
    return { max_gap_ms: counters[MAX_GAP_MS], ticks: counters[TICKS], window_ms: now() - marked, rate_mib_s: 0, slowdown: null };
  },
  /** The first exec after a restore, as an agent's often is: a look at what the session holds, not a walk of it. */
  first() {
    return { marker, records: records.length, last: records.at(-1).id };
  },
  /** Everything the session holds, read and compared with what the last exec left. */
  verify() {
    const found = digest();
    return { marker, memory: found === expected, children: null, file: null, socket: null, running: null, records: records.length, rss_bytes: rss() };
  },
};

console.log(`BENCH:${JSON.stringify({ marker, records: records.length, docs: docs.length, checkpointed, pids: [process.pid], rss_bytes: rss() })}`);
