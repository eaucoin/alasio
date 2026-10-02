/**
 * bayma's telemetry from inside a session's sandbox, exported where alasio exports its
 * own. The sandbox has no route to any endpoint and holds no credential, so bayma
 * exports to the telemetry drain beside it on the sandbox's own loopback
 * (sandbox/agent/telemetry-drain.mjs), and alasio reads the drain through
 * `agent-connect` (SessionHost.connect), the one way into a sandbox it already uses for
 * bayma itself. What it reads, it treats as untrusted, since anything in the sandbox
 * can write to the drain or replace it: each request's resources are stamped with what
 * alasio knows (service.name bayma, the session's volume, the deployment's attributes)
 * in place of what they say, a request that does not parse is dropped, and requests are
 * read no faster than a bound, so a flood fills the drain and is dropped there.
 */
import { setTimeout as sleep } from "node:timers/promises";

import { createLogger } from "../shared/log.js";
import { meter, parseKeyValueList, resolveTelemetry, sharedResourceAttributes, SIGNALS, telemetryEnabled } from "../telemetry/index.js";
import { MalformedRequest, stampResources } from "./otlp-resource.js";

const log = createLogger("sandbox-telemetry");

/** The drain's ports on the sandbox's loopback: OTLP from bayma, and alasio's reads. */
export const DRAIN_OTLP_PORT = 4318;
export const DRAIN_READ_PORT = 7291;

const FRAME_HEAD_BYTES = 6;
const DROPPED = 3;
const HELLO = 4;
const VERSION = 1;
const ENCODINGS = ["protobuf", "json"];
// The drain refuses a larger request, so a larger frame is not the drain's.
const MAX_FRAME_BYTES = 4 * 1024 * 1024;
// What a session's bayma may send, sustained, and in a burst: far beyond what bayma
// records, and a bound on what anything else in the sandbox can push through alasio.
const RATE_BYTES_PER_SECOND = 128 * 1024;
const BURST_BYTES = 16 * 1024 * 1024;
// Connections that end before the drain greets them are retried with backoff this many
// times, and then the relay ends until the session is next used: such a sandbox has no
// drain, because it started before alasio exported telemetry.
const MAX_FAILED_CONNECTIONS = 5;
const MAX_BACKOFF_MS = 30_000;

const requests = meter.createCounter("alasio.sandbox.telemetry.requests", {
  description: "OTLP requests relayed from bayma inside session sandboxes, by outcome",
  unit: "{request}",
});

/**
 * The standard variables bayma inside a sandbox exports with: to the drain, each signal
 * alasio exports over HTTP in the protocol alasio exports it in, and no other. Empty when
 * alasio exports nothing, and then the session host starts no drain.
 */
export function sandboxBaymaTelemetryEnv(env = process.env) {
  const telemetry = resolveTelemetry(env);
  if (!telemetryEnabled(telemetry)) return {};
  const settings = { OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${DRAIN_OTLP_PORT}` };
  for (const signal of SIGNALS) {
    const upper = signal.toUpperCase();
    const protocol = telemetry[signal]?.protocol;
    const http = protocol?.startsWith("http/");
    settings[`OTEL_${upper}_EXPORTER`] = http ? "otlp" : "none";
    if (http) settings[`OTEL_EXPORTER_OTLP_${upper}_PROTOCOL`] = protocol;
  }
  return settings;
}

/** The resource attributes a session's relayed requests are stamped with. */
export function sandboxResource(volumeId, env = process.env) {
  return { ...parseKeyValueList(sharedResourceAttributes(env)), "service.name": "bayma", "alasio.volume.id": volumeId };
}

/**
 * Splits a byte stream into the drain's frames, calling `onFrame({ kind, encoding,
 * body })` for each. Throws from `push` on a stream that is not the drain's.
 */
export function createFrameReader(onFrame) {
  let buffered = Buffer.alloc(0);
  return {
    push(chunk) {
      buffered = buffered.length ? Buffer.concat([buffered, chunk]) : chunk;
      while (buffered.length >= FRAME_HEAD_BYTES) {
        const length = buffered.readUInt32BE(0);
        if (length > MAX_FRAME_BYTES) throw new MalformedRequest(`a ${length}-byte frame`);
        if (buffered.length < FRAME_HEAD_BYTES + length) return;
        const kind = buffered[4];
        const encoding = buffered[5];
        const body = buffered.subarray(FRAME_HEAD_BYTES, FRAME_HEAD_BYTES + length);
        buffered = buffered.subarray(FRAME_HEAD_BYTES + length);
        onFrame({ kind, encoding, body });
      }
    },
  };
}

/**
 * Relays one session's drain while it runs. `connect()` returns a child process whose
 * stdout is a connection to the drain's read port (SessionHost.connect); `isRunning()`
 * says whether the session host is up; `forwarder` exports (../telemetry/forward.js);
 * `stamp` is the resource attributes to stamp. Reconnects when a read ends while the
 * host runs, after `retryDelay(failures)` ms, and ends, calling `onEnd`, when the host
 * stops or has no drain. Returns `close()`.
 */
export function startTelemetryRelay({
  volumeId,
  connect,
  isRunning,
  forwarder,
  stamp,
  onEnd = () => {},
  retryDelay = (failures) => Math.min(1000 * 2 ** failures, MAX_BACKOFF_MS),
}) {
  let closed = false;
  let child = null;
  const stopped = new AbortController();
  let tokens = BURST_BYTES;
  let refilledAt = performance.now();
  let failing = false;

  const count = (outcome, attributes = {}, value = 1) => requests.add(value, { outcome, ...attributes });

  /** Waits until `bytes` more are within the rate, so the drain holds what waits. */
  async function pace(bytes) {
    const now = performance.now();
    tokens = Math.min(BURST_BYTES, tokens + ((now - refilledAt) / 1000) * RATE_BYTES_PER_SECOND);
    refilledAt = now;
    tokens -= bytes;
    if (tokens < 0) await sleep((-tokens / RATE_BYTES_PER_SECOND) * 1000, undefined, { signal: stopped.signal }).catch(() => {});
  }

  async function relay({ kind, encoding, body }) {
    if (kind === DROPPED) {
      count("dropped", { reason: "drain_full" }, body.length === 4 ? body.readUInt32BE(0) : 0);
      return;
    }
    const signal = SIGNALS[kind];
    if (!signal || !ENCODINGS[encoding]) throw new MalformedRequest(`frame kind ${kind} encoding ${encoding}`);
    if (!forwarder.protocols[signal]) return;
    await pace(body.length);
    let stamped;
    try {
      stamped = stampResources(signal, ENCODINGS[encoding], body, stamp);
    } catch (error) {
      if (!(error instanceof MalformedRequest)) throw error;
      count("dropped", { signal, reason: "malformed" });
      return;
    }
    const result = await forwarder.export(signal, ENCODINGS[encoding], stamped);
    count(result.ok ? "exported" : "dropped", { signal, ...(result.ok ? {} : { reason: "export_failed" }) });
    if (!result.ok && !failing) log.warn(`${volumeId}: exporting the session's ${signal} failed: ${result.error}`);
    else if (result.ok && failing) log.info(`${volumeId}: exporting the session's telemetry recovered`);
    failing = !result.ok;
  }

  /**
   * One read of the drain, relaying its frames in order and reading no further while
   * one is relayed. Resolves whether the drain greeted it, once it has ended and what it
   * read is relayed.
   */
  function read() {
    return new Promise((resolve) => {
      const proc = connect();
      child = proc;
      let greeted = false;
      let broken = false;
      const frames = [];
      let pumping = null;
      const reader = createFrameReader((frame) => {
        if (greeted) frames.push(frame);
        else if (frame.kind === HELLO && frame.body[0] === VERSION) greeted = true;
        else throw new MalformedRequest("the stream does not open with the drain's greeting");
      });
      // A stream that is not the drain's cannot be read on from where it went wrong.
      const drop = (error) => {
        if (broken) return;
        broken = true;
        frames.length = 0;
        log.warn(`${volumeId}: dropping the connection to the session's telemetry drain: ${error.message}`);
        count("dropped", { reason: "malformed" });
        proc.kill();
      };
      // Relays what has been read, unless that is under way; reading waits meanwhile.
      const pump = () => {
        if (pumping) return;
        proc.stdout.pause();
        pumping = (async () => {
          while (frames.length > 0 && !closed) await relay(frames.shift());
        })().catch(drop).finally(() => {
          pumping = null;
          proc.stdout.resume();
        });
      };
      proc.stdout.on("data", (chunk) => {
        if (broken) return;
        try {
          reader.push(chunk);
        } catch (error) {
          drop(error);
          return;
        }
        pump();
      });
      let finished = false;
      const finish = async () => {
        if (finished) return;
        finished = true;
        await pumping;
        resolve(greeted);
      };
      proc.stdin?.on("error", () => {});
      proc.on("error", (error) => {
        log.warn(`${volumeId}: cannot reach the session's telemetry drain: ${error.message}`);
        finish();
      });
      proc.on("close", finish);
    });
  }

  const done = (async () => {
    let failures = 0;
    while (!closed) {
      failures = (await read()) ? 0 : failures + 1;
      if (closed) break;
      if (failures >= MAX_FAILED_CONNECTIONS) {
        log.info(`${volumeId}: no telemetry drain in the session's sandbox; one started before alasio exported telemetry has none until it next starts fresh`);
        break;
      }
      if (!(await isRunning().catch(() => false))) break;
      await sleep(retryDelay(failures), undefined, { signal: stopped.signal }).catch(() => {});
    }
    child = null;
  })().finally(() => {
    if (!closed) onEnd();
  });

  return {
    async close() {
      closed = true;
      stopped.abort();
      child?.kill();
      await done;
    },
  };
}
