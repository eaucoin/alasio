// The Bun fixtures (idle, poller, saturating), installed into a bayma Bun session by the
// driver's setup exec, which prepends `const PARAMS = {...};`. bayma runs a Bun session's main
// thread only while an exec runs, so what runs in the background runs in a Worker: the
// background loop (chunk rewrites, children, the socket, the file) and the ticker. Memory is
// 1 MiB SharedArrayBuffer chunks, random at setup; a rewrite stamps (chunk, page, version,
// STAMP) at the head of each of a chunk's 256 pages, so every page of it is dirtied and its
// random rest is kept, whose SHA-256 per chunk setup records.
const fs = require("node:fs");
const crypto = require("node:crypto");

if (globalThis.bench) throw new Error("this session holds a fixture already");

const PAGE = 4096;
const CHUNK = 1 << 20;
const PAGES = CHUNK / PAGE;
const STAMP = 0xb0b0cafe;
// Control words, shared with the workers.
const RUN = 0; // 1: the background loop runs; 0: it is asked to stop
const STOPPED = 1; // 1: it has stopped
const MEASURE = 2; // 1: the ticker measures
const NEVER = 3; // stays 0: what the ticker waits on, with a timeout
// Counters (Float64), shared with the workers.
const REWRITES = 0;
const REWRITTEN_BYTES = 1;
const APPENDED = 2;
const SENT = 3;
const ECHOED = 4;
const SOCKET_CLOSED = 5;
const HELD_TICKS = 6;
const POLLS = 7;
const MAX_GAP_MS = 8;
const TICKS = 9;

const { fixture, sizeMib, runId, rateMibS = 0, workspace = "/workspace" } = PARAMS;
const count = Math.max(1, Math.round(sizeMib));
const ctl = new Int32Array(new SharedArrayBuffer(4 * 4));
const counters = new Float64Array(new SharedArrayBuffer(8 * 16));
const versions = new Uint32Array(new SharedArrayBuffer(4 * count));
const chunks = [];

/** Stamps every page of `chunk`, the `index`th, with `version`. */
function stamp(words, index, version) {
  for (let page = 0; page < PAGES; page++) {
    const at = page * (PAGE / 4);
    words[at] = index;
    words[at + 1] = page;
    words[at + 2] = version;
    words[at + 3] = STAMP;
  }
}

const scratch = new Uint8Array(CHUNK);
/** The SHA-256 of a chunk's random rest: the chunk with its stamps zeroed. */
function digestOf(chunk) {
  scratch.set(new Uint8Array(chunk));
  for (let page = 0; page < PAGES; page++) scratch.fill(0, page * PAGE, page * PAGE + 16);
  return new Bun.CryptoHasher("sha256").update(scratch).digest("hex");
}

for (let index = 0; index < count; index++) {
  const chunk = new SharedArrayBuffer(CHUNK);
  crypto.randomFillSync(new Uint8Array(chunk));
  stamp(new Uint32Array(chunk), index, 0);
  chunks.push(chunk);
}
const digests = chunks.map(digestOf);
const marker = new Bun.CryptoHasher("sha256").update(`${runId}/${fixture}`).digest("hex");

// The ticker: wakes every 1 ms while MEASURE is 1 and keeps the longest gap between two wakes.
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

// The background loop: rewrites, and the poller's children or the saturating fixture's socket
// and file. Stops, acknowledging, while RUN is 0.
const loopSource = `
const fs = require("node:fs");
onmessage = async ({ data: p }) => {
  const { ctl, counters, versions, chunks, fixture, rateMibS, workspace } = p;
  const words = chunks.map((chunk) => new Uint32Array(chunk));
  const stamp = ${stamp.toString()};
  const PAGES = ${PAGES}, PAGE = ${PAGE}, STAMP = ${STAMP};
  const rewrite = (index) => {
    versions[index] += 1;
    stamp(words[index], index, versions[index]);
    counters[${REWRITES}] += 1;
    counters[${REWRITTEN_BYTES}] += ${CHUNK};
  };
  // A tick that finds RUN 0 says so, once every tick before it has ended, as this thread runs one at a time.
  const running = () => {
    if (Atomics.load(ctl, ${RUN}) === 1) return true;
    Atomics.store(ctl, ${STOPPED}, 1);
    return false;
  };
  const held = [];
  let next = 0;
  if (fixture === "poller") {
    const hot = Math.max(1, Math.round(chunks.length / 10));
    // 20 MiB/s over the hottest tenth: a chunk every 50 ms.
    setInterval(() => {
      if (!running()) return;
      rewrite(next);
      next = (next + 1) % hot;
    }, 50);
    const loop = Bun.spawn(["bash", "-c", "while :; do sleep 1; echo t; done"], { stdout: "pipe" });
    held.push(loop.pid);
    (async () => {
      for await (const bytes of loop.stdout) counters[${HELD_TICKS}] += new TextDecoder().decode(bytes).split("t").length - 1;
    })();
    setInterval(async () => {
      if (!running()) return;
      const poll = Bun.spawn(["bash", "-c", "echo poll $RANDOM"], { stdout: "pipe" });
      if ((await new Response(poll.stdout).text()).startsWith("poll ")) counters[${POLLS}] += 1;
    }, 2000);
  }
  if (fixture === "saturating") {
    // The job's copy bandwidth, over the whole heap, in 10 ms ticks.
    let owed = 0;
    let last = performance.now();
    setInterval(() => {
      const now = performance.now();
      if (!running()) {
        last = now;
        return;
      }
      // At most a second owed, as a loop that falls behind does not catch up in one tick.
      owed = Math.min(rateMibS, owed + (rateMibS * (now - last)) / 1000);
      last = now;
      for (; owed >= 1; owed -= 1) {
        rewrite(next);
        next = (next + 1) % chunks.length;
      }
    }, 10);
    const server = Bun.spawn([process.execPath, "-e", "const s = Bun.listen({ hostname: '127.0.0.1', port: 0, socket: { data(socket, data) { socket.write(data); } } }); console.log(s.port);"], { stdout: "pipe" });
    held.push(server.pid);
    const reader = server.stdout.getReader();
    const port = Number(new TextDecoder().decode((await reader.read()).value).trim());
    const socket = await Bun.connect({
      hostname: "127.0.0.1",
      port,
      socket: {
        data(_socket, data) {
          counters[${ECHOED}] += new TextDecoder().decode(data).split("\\n").length - 1;
        },
        close() {
          counters[${SOCKET_CLOSED}] = 1;
        },
        error() {
          counters[${SOCKET_CLOSED}] = 1;
        },
      },
    });
    const file = fs.openSync(workspace + "/bench-append.log", "a");
    setInterval(() => {
      if (!running()) return;
      fs.writeSync(file, "seq " + counters[${APPENDED}] + "\\n");
      counters[${APPENDED}] += 1;
      if (counters[${SOCKET_CLOSED}] === 0) {
        socket.write("ping\\n");
        counters[${SENT}] += 1;
      }
    }, 100);
  }
  postMessage({ held });
};`;

/** A worker running `source`, its first message posted. */
function worker(source, message) {
  const made = new Worker(URL.createObjectURL(new Blob([source], { type: "application/javascript" })));
  made.postMessage(message);
  return made;
}

const ticker = worker(tickerSource, { ctl, counters });
Atomics.store(ctl, RUN, 1);
let loop = null;
let held = [];
if (fixture !== "idle") {
  loop = worker(loopSource, { ctl, counters, versions, chunks, fixture, rateMibS, workspace });
  held = await new Promise((resolve, reject) => {
    loop.onmessage = ({ data }) => resolve(data.held);
    loop.onerror = (event) => reject(new Error(event.message));
  });
}

const now = () => Number(process.hrtime.bigint()) / 1e6;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** VmRSS of this process and the processes it holds, in bytes. */
function rssBytes() {
  const of = (pid) => {
    try {
      const line = fs.readFileSync(`/proc/${pid}/status`, "utf8").split("\n").find((each) => each.startsWith("VmRSS:"));
      return Number(line?.split(/\s+/)[1] ?? 0) * 1024;
    } catch {
      return 0;
    }
  };
  return [process.pid, ...held].reduce((sum, pid) => sum + of(pid), 0);
}

/** Stops the background loop, as a check needs memory and files to stand still, `during` it. */
async function stopped(during) {
  if (!loop) return during();
  Atomics.store(ctl, RUN, 0);
  // Said again by a tick that runs after both stores.
  Atomics.store(ctl, STOPPED, 0);
  // As long as a restore that loads every page from a store may hold the loop's tick, its pages not yet there.
  const deadline = now() + 300_000;
  while (Atomics.load(ctl, STOPPED) !== 1) {
    if (now() > deadline) throw new Error("the background loop did not stop within 300 s");
    await sleep(5);
  }
  try {
    return await during();
  } finally {
    Atomics.store(ctl, RUN, 1);
  }
}

/** Every page's stamp and every chunk's random rest: every page read. */
function memoryIntact() {
  const bad = [];
  for (let index = 0; index < count && bad.length < 8; index++) {
    const words = new Uint32Array(chunks[index]);
    let ok = true;
    for (let page = 0; page < PAGES && ok; page++) {
      const at = page * (PAGE / 4);
      ok = words[at] === index && words[at + 1] === page && words[at + 2] === versions[index] && words[at + 3] === STAMP;
    }
    if (!ok || digestOf(chunks[index]) !== digests[index]) bad.push(index);
  }
  return bad;
}

/** Whether `counter` grows within `ms`. */
async function grows(counter, ms) {
  const before = counters[counter];
  const deadline = now() + ms;
  while (now() < deadline) {
    if (counters[counter] > before) return true;
    await sleep(10);
  }
  return false;
}

const alive = (pid) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

const startedAt = now();
const startedBytes = counters[REWRITTEN_BYTES];
let marked = null;

globalThis.bench = {
  /** The rewrite rate since setup, MiB/s, which the save windows compare with. */
  baseline() {
    this.baselineMibS = (counters[REWRITTEN_BYTES] - startedBytes) / CHUNK / ((now() - startedAt) / 1000);
    return { baseline_mib_s: this.baselineMibS, rss_bytes: rssBytes() };
  },
  mark() {
    counters[MAX_GAP_MS] = 0;
    counters[TICKS] = 0;
    marked = { at: now(), bytes: counters[REWRITTEN_BYTES] };
    Atomics.store(ctl, MEASURE, 1);
    Atomics.notify(ctl, MEASURE);
    return { marked: true };
  },
  gaps() {
    Atomics.store(ctl, MEASURE, 0);
    if (!marked) throw new Error("no window was marked");
    const rate = (counters[REWRITTEN_BYTES] - marked.bytes) / CHUNK / ((now() - marked.at) / 1000);
    return {
      max_gap_ms: counters[MAX_GAP_MS],
      ticks: counters[TICKS],
      window_ms: now() - marked.at,
      rate_mib_s: rate,
      slowdown: this.baselineMibS > 0 ? rate / this.baselineMibS : null,
    };
  },
  /** The first exec after a restore: the marker it holds live, and the loop's counters; for saturating, a full GC and every page read. */
  async first() {
    const seen = { marker, chunks: chunks.length, rewrites: counters[REWRITES] };
    if (fixture === "saturating") {
      Bun.gc(true);
      seen.bad_chunks = await stopped(memoryIntact);
    }
    return seen;
  },
  /** Every check: memory, children, the file, the socket, the loop running; and what the fixture holds. */
  async verify() {
    const result = { marker, memory: null, children: null, file: null, socket: null, running: null };
    const bad = await stopped(async () => {
      const found = memoryIntact();
      if (fixture === "saturating") {
        const lines = fs.readFileSync(`${workspace}/bench-append.log`, "utf8").split("\n").filter(Boolean);
        const appended = counters[APPENDED];
        result.file = lines.length === appended && lines.every((line, index) => line === `seq ${index}`);
        result.file_lines = lines.length;
        result.appended = appended;
      }
      return found;
    });
    result.memory = bad.length === 0;
    result.bad_chunks = bad;
    if (fixture === "poller") {
      const spawned = Bun.spawnSync(["bash", "-c", "echo ok"]).stdout.toString().trim() === "ok";
      result.children = held.every(alive) && spawned && (await grows(HELD_TICKS, 2500));
    }
    if (fixture === "saturating") {
      result.file = result.file && (await grows(APPENDED, 300));
      result.socket = counters[SOCKET_CLOSED] === 1 ? "closed" : (await grows(ECHOED, 2000)) ? "open" : "hung";
    }
    if (loop) result.running = await grows(REWRITES, 1000);
    result.rss_bytes = rssBytes();
    return result;
  },
};

const installed = { marker, chunks: count, pids: [process.pid, ...held], rss_bytes: rssBytes() };
console.log(`BENCH:${JSON.stringify(installed)}`);
