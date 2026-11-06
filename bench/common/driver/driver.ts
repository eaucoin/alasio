/**
 * The fixture driver: one program, the same bytes for every system (its bundle,
 * dist/driver.mjs), which reaches a bayma over MCP streamable HTTP and nothing else.
 *
 *   node driver.mjs setup  --url U [--header 'Name: value']... --fixture F --size-mib N --run-id R [--rate-mib-s X] [--workspace /workspace]
 *   node driver.mjs mark   --url U [--header ...] --session S --fixture F
 *   node driver.mjs gaps   --url U [--header ...] --session S --fixture F
 *   node driver.mjs probe  --url U [--header ...] --session S --fixture F --run-id R [--t0-ns T]
 *   node driver.mjs close  --url U [--header ...] --session S
 *   node driver.mjs now
 *
 * Each prints one JSON object. Every time it reports is CLOCK_MONOTONIC in nanoseconds
 * (process.hrtime.bigint()), which the runner's processes and containers share.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { parseArgs } from "node:util";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";

export const FIXTURES = ["idle", "poller", "fan-out", "saturating"] as const;
export type Fixture = (typeof FIXTURES)[number];

/** The fixtures' sources, as the bundle embeds them, or read beside this file when it runs unbundled. */
declare const FIXTURE_SOURCES: Readonly<Record<string, string>> | undefined;
const source = (name: string): string =>
  typeof FIXTURE_SOURCES === "undefined" ? readFileSync(new URL(`./fixtures/${name}`, import.meta.url), "utf8") : (FIXTURE_SOURCES[name] ?? "");

/** How long any one call may take: a restore through alasio's gateway answers the first only once its pod has started. */
const CALL_TIMEOUT_MS = 900_000;
/** How long a probe retries a bayma that is not there yet, as one being restored is not: a restore's bound (the design's section 5). */
const CONNECT_TIMEOUT_MS = 300_000;
/** How long setup lets the fixture run before it measures its steady rate. */
const STEADY_MS = 10_000;

export const now = (): bigint => process.hrtime.bigint();
const ms = (from: bigint, to: bigint): number => Number(to - from) / 1e6;
const sleep = (wait: number) => new Promise<void>((resolve) => setTimeout(resolve, wait));

/** The runtime a fixture runs in. */
export const runtimeOf = (fixture: Fixture): "bun" | "python" => (fixture === "fan-out" ? "python" : "bun");

/** The marker a fixture holds live: what the driver compares what it reads back with. */
export const markerOf = (runId: string, fixture: Fixture): string => createHash("sha256").update(`${runId}/${fixture}`).digest("hex");

/** What bayma's exec and wait answer, as far as the driver reads them. */
interface ExecAnswer {
  readonly exec_id?: string;
  readonly done?: boolean;
  readonly next_seq?: number;
  readonly stdout_text?: string;
  readonly error_text?: string;
  readonly notice?: string;
}

/** A bayma's MCP endpoint: its URL and the headers every request carries. */
export interface Endpoint {
  readonly url: string;
  readonly headers: Readonly<Record<string, string>>;
}

/** Spike: how the probe's connection went, its refusals and when it connected. */
const connecting: { refusals: string[]; connected_ns: string | null } = { refusals: [], connected_ns: null };

/** A connected client of a bayma. */
class Bayma {
  private readonly client: Client;

  private constructor(client: Client) {
    this.client = client;
  }

  static async connect({ url, headers }: Endpoint): Promise<Bayma> {
    const client = new Client({ name: "snapshot-bench-driver", version: "1.0.0" });
    await client.connect(new StreamableHTTPClientTransport(new URL(url), { requestInit: { headers: { ...headers } } }), { timeout: CALL_TIMEOUT_MS });
    return new Bayma(client);
  }

  /** Connects, retrying every 10 ms while nothing answers, as a bayma being restored does not yet. */
  static async connectWhenUp(endpoint: Endpoint): Promise<Bayma> {
    const deadline = Date.now() + CONNECT_TIMEOUT_MS;
    for (;;) {
      try {
        const connected = await Bayma.connect(endpoint);
        connecting.connected_ns = String(now());
        return connected;
      } catch (error) {
        // Spike: each failed attempt, when and why.
        connecting.refusals.push(`${Date.now()} ${statusOf(error) ?? ""} ${String(error instanceof Error ? error.message : error).slice(0, 200)}`);
        if (Date.now() > deadline || !refused(error)) throw error;
        // A server that answered to be asked again (502, 503, 504), as a bayma whose restore's hook has not come yet does, is asked a second on, as its Retry-After says.
        await sleep(statusOf(error) === undefined ? 10 : 1000);
      }
    }
  }

  async call<T>(name: string, args: Record<string, unknown>): Promise<T> {
    const result = (await this.client.callTool({ name, arguments: args }, undefined, { timeout: CALL_TIMEOUT_MS })) as CallToolResult;
    const text = result.content.find((part) => part.type === "text")?.text ?? "";
    if (result.isError) throw new Error(`bayma's ${name} failed: ${text}`);
    return (result.structuredContent as T | undefined) ?? (JSON.parse(text) as T);
  }

  /** Runs `code` to its end: its stdout whole, and bayma's notice, if it gave one. */
  async exec(sessionId: string, code: string): Promise<{ readonly stdout: string; readonly notice: string | null }> {
    let answer = await this.call<ExecAnswer>("exec", { session_id: sessionId, code, yield_time_ms: 300_000 });
    let stdout = answer.stdout_text ?? "";
    const notice = answer.notice ?? null;
    while (answer.done === false) {
      answer = await this.call<ExecAnswer>("wait", { session_id: sessionId, exec_id: answer.exec_id, from_seq: answer.next_seq, yield_time_ms: 300_000 });
      stdout += answer.stdout_text ?? "";
    }
    if (answer.error_text) throw new Error(`the exec failed: ${answer.error_text}${notice === null ? "" : `; bayma's notice: ${notice}`}`);
    return { stdout, notice };
  }

  /** Runs `code`, which prints `BENCH:<json>`, and parses that line. */
  async bench<T>(sessionId: string, code: string): Promise<{ readonly value: T; readonly notice: string | null }> {
    const { stdout, notice } = await this.exec(sessionId, code);
    const line = stdout.split("\n").findLast((each) => each.startsWith("BENCH:"));
    if (line === undefined) throw new Error(`the exec printed no result: ${stdout.slice(-2000)}`);
    return { value: JSON.parse(line.slice("BENCH:".length)) as T, notice };
  }

  close(): Promise<void> {
    return this.client.close();
  }
}

/** The HTTP status an MCP request was answered with, where it was: the SDK's StreamableHTTPError's `code`, whose message holds none. */
function statusOf(error: unknown): number | undefined {
  const code = (error as { readonly code?: unknown }).code;
  return typeof code === "number" && code >= 100 ? code : undefined;
}

/** Whether `error` is of a server that is not there, or not yet answering, or asking to be asked again. */
function refused(error: unknown): boolean {
  const status = statusOf(error);
  if (status === 502 || status === 503 || status === 504) return true;
  const said = `${error instanceof Error ? `${error.message} ${String((error as { cause?: unknown }).cause)}` : String(error)}`;
  return /ECONNREFUSED|ECONNRESET|EHOSTUNREACH|ENETUNREACH|fetch failed|socket hang up|UND_ERR|other side closed|502|503|504/u.test(said);
}

/** The code that calls the fixture's `method` and prints what it answers. */
function call(fixture: Fixture, method: string): string {
  return runtimeOf(fixture) === "python"
    ? `import json\nprint("BENCH:" + json.dumps(bench.${method}()))`
    : `console.log("BENCH:" + JSON.stringify(await globalThis.bench.${method}()))`;
}

/** The setup exec: the fixture's source, given its parameters. */
function installer(fixture: Fixture, params: Record<string, unknown>): string {
  const json = JSON.stringify(params);
  return runtimeOf(fixture) === "python"
    ? `import json\nPARAMS = json.loads(${JSON.stringify(json)})\n${source("fan-out.py")}`
    : `const PARAMS = ${json};\n${source("bench.bun.js")}`;
}

/** Runs `use` with a client of `endpoint` holding control of `sessionId`, which it lets go of after. */
async function controlling<T>(endpoint: Endpoint, sessionId: string, use: (bayma: Bayma) => Promise<T>, connect = Bayma.connect): Promise<T> {
  const bayma = await connect(endpoint);
  try {
    await bayma.call("session.acquire_controller", { session_id: sessionId });
    try {
      return await use(bayma);
    } finally {
      await bayma.call("session.release_controller", { session_id: sessionId });
    }
  } finally {
    await bayma.close();
  }
}

export interface SetupOptions {
  readonly fixture: Fixture;
  readonly sizeMib: number;
  readonly runId: string;
  readonly rateMibS: number;
  readonly workspace: string;
}

export async function setup(endpoint: Endpoint, { fixture, sizeMib, runId, rateMibS, workspace }: SetupOptions): Promise<Record<string, unknown>> {
  const bayma = await Bayma.connect(endpoint);
  try {
    const { session } = await bayma.call<{ session: { session_id: string } }>("session.create", { runtime: runtimeOf(fixture), title: `bench ${fixture} ${runId}`, cwd: workspace });
    const started = now();
    const { value: installed } = await bayma.bench<Record<string, unknown>>(session.session_id, installer(fixture, { fixture, sizeMib, runId, rateMibS, workspace }));
    const installedAt = now();
    await sleep(STEADY_MS);
    const { value: steady } = await bayma.bench<Record<string, unknown>>(session.session_id, call(fixture, "baseline"));
    await bayma.call("session.release_controller", { session_id: session.session_id });
    return { session_id: session.session_id, fixture, size_mib: sizeMib, install_ms: ms(started, installedAt), ...installed, ...steady };
  } finally {
    await bayma.close();
  }
}

export const mark = (endpoint: Endpoint, sessionId: string, fixture: Fixture) =>
  controlling(endpoint, sessionId, async (bayma) => ({ ...(await bayma.bench<Record<string, unknown>>(sessionId, call(fixture, "mark"))).value, at_ns: String(now()) }));

export const gaps = (endpoint: Endpoint, sessionId: string, fixture: Fixture) =>
  controlling(endpoint, sessionId, async (bayma) => ({ ...(await bayma.bench<Record<string, unknown>>(sessionId, call(fixture, "gaps"))).value, at_ns: String(now()) }));

/** What a fixture's verify answers. */
interface Verified {
  readonly marker: string;
  readonly memory: boolean | null;
  readonly children: boolean | null;
  readonly file: boolean | null;
  readonly socket: "open" | "closed" | "hung" | null;
  readonly running: boolean | null;
  readonly rss_bytes: number;
  readonly [more: string]: unknown;
}

/** Whether bayma's notice says the session came back with its network connections closed. */
const saysClosed = (notice: string | null): boolean => notice !== null && /connections? (?:were |are )?closed|closed its (?:network )?connections|with its network connections closed/iu.test(notice);

/**
 * The restore probe: from `t0` (the restore asked; now, where not given), connects, takes the
 * session over, runs the first exec, then the verify exec, and says what they found.
 */
export async function probe(endpoint: Endpoint, sessionId: string, fixture: Fixture, runId: string, t0 = now()) {
  const t0Wall = Date.now();
  return controlling(endpoint, sessionId, async (bayma) => {
    const connected = now();
    const { value: first, notice } = await bayma.bench<Record<string, unknown>>(sessionId, call(fixture, "first"));
    const firstEnd = now();
    const { value: verified } = await bayma.bench<Verified>(sessionId, call(fixture, "verify"));
    const verifyEnd = now();
    const marker = verified.marker === markerOf(runId, fixture) && first["marker"] === verified.marker;
    const socket = verified.socket;
    const failures = [
      ...(marker ? [] : ["the marker is not the run's"]),
      ...(verified.memory === false ? [`memory not as written: chunks ${JSON.stringify(verified["bad_chunks"] ?? [])}`] : []),
      ...(verified.children === false ? ["a child is gone or does not answer"] : []),
      ...(verified.file === false ? [`the appended file is not what the process wrote (${String(verified["file_lines"])} lines, ${String(verified["appended"])} appended)`] : []),
      ...(socket === "hung" ? ["the socket is neither closed nor answering"] : []),
      ...(verified.running === false ? ["the background loop does not run"] : []),
    ];
    // The connection is to a server in the instance, over its loopback, which a whole-instance
    // restore may bring back with the rest: either way is the state as saved, and bayma's notice,
    // of connections to outside, is said beside it.
    const noticeAgrees = socket === null ? null : socket === "closed" || !saysClosed(notice);
    return {
      t0_ns: String(t0),
      t0_wall_ms: t0Wall,
      initialized_ns: connecting.connected_ns,
      refusals: connecting.refusals,
      connected_ns: String(connected),
      first_exec_end_ns: String(firstEnd),
      verify_end_ns: String(verifyEnd),
      first_exec_ms: ms(t0, firstEnd),
      verify_ms: ms(firstEnd, verifyEnd),
      notice,
      ok: failures.length === 0,
      failure: failures.length === 0 ? null : failures.join("; "),
      correctness: { marker, memory: verified.memory, children: verified.children, file: verified.file, socket, running: verified.running, notice, notice_agrees: noticeAgrees },
      workload_rss_bytes: verified.rss_bytes,
      first,
      verified,
    };
  }, Bayma.connectWhenUp);
}

export async function close(endpoint: Endpoint, sessionId: string): Promise<Record<string, unknown>> {
  const bayma = await Bayma.connect(endpoint);
  try {
    await bayma.call("session.close", { session_id: sessionId });
    return { closed: sessionId };
  } finally {
    await bayma.close();
  }
}

/** The endpoint the arguments name: `--url`, and each `--header 'Name: value'`. */
function endpointOf(values: { readonly url?: string | undefined; readonly header?: readonly string[] | undefined }): Endpoint {
  if (!values.url) throw new Error("--url is required");
  const headers: Record<string, string> = {};
  for (const header of values.header ?? []) {
    const colon = header.indexOf(":");
    if (colon <= 0) throw new Error(`--header ${header} is not 'Name: value'`);
    headers[header.slice(0, colon).trim()] = header.slice(colon + 1).trim();
  }
  return { url: values.url, headers };
}

function fixtureOf(value: string | undefined): Fixture {
  const found = FIXTURES.find((fixture) => fixture === value);
  if (!found) throw new Error(`--fixture is one of ${FIXTURES.join(", ")}`);
  return found;
}

function required(value: string | undefined, name: string): string {
  if (!value) throw new Error(`--${name} is required`);
  return value;
}

async function main(argv: readonly string[]): Promise<unknown> {
  const [command, ...rest] = argv;
  const { values } = parseArgs({
    args: [...rest],
    options: {
      url: { type: "string" },
      header: { type: "string", multiple: true },
      fixture: { type: "string" },
      "size-mib": { type: "string" },
      "run-id": { type: "string" },
      "rate-mib-s": { type: "string", default: "0" },
      workspace: { type: "string", default: "/workspace" },
      session: { type: "string" },
      "t0-ns": { type: "string" },
    },
  });
  if (command === "now") return { now_ns: String(now()) };
  const endpoint = endpointOf(values);
  if (command === "setup") {
    return setup(endpoint, {
      fixture: fixtureOf(values.fixture),
      sizeMib: Number(required(values["size-mib"], "size-mib")),
      runId: required(values["run-id"], "run-id"),
      rateMibS: Number(values["rate-mib-s"]),
      workspace: values.workspace ?? "/workspace",
    });
  }
  const sessionId = required(values.session, "session");
  if (command === "mark") return mark(endpoint, sessionId, fixtureOf(values.fixture));
  if (command === "gaps") return gaps(endpoint, sessionId, fixtureOf(values.fixture));
  if (command === "probe") {
    return probe(endpoint, sessionId, fixtureOf(values.fixture), required(values["run-id"], "run-id"), values["t0-ns"] ? BigInt(values["t0-ns"]) : now());
  }
  if (command === "close") return close(endpoint, sessionId);
  throw new Error(`no command ${command}: setup, mark, gaps, probe, close or now`);
}

// Run as a program, not imported, nor bundled into another's (../driver/build.ts).
if (/(^|\/)driver\.(ts|mjs)$/u.test(process.argv[1] ?? "")) {
  main(process.argv.slice(2)).then(
    (said) => console.log(JSON.stringify(said)),
    (error: unknown) => {
      console.error(error instanceof Error ? (error.stack ?? error.message) : String(error));
      process.exit(1);
    },
  );
}
