/**
 * Sets the resource of an OTLP export request that came from inside a session's
 * sandbox, so whatever wrote the request cannot say what it is or where it came from.
 * Every attribute `stamp` names and every `alasio.*` attribute is removed from each of
 * the request's resources, and `stamp`'s are added. Everything else is kept as sent: a
 * binary protobuf request is rewritten at the wire level, its resources alone, with
 * every other byte copied, and a JSON request is re-encoded with only the fields OTLP
 * defines around its resources, so no second spelling of one survives.
 */
import type { Signal } from "../telemetry/config.ts";
import type { OtlpEncoding } from "../telemetry/forward.ts";

/** The resource attributes a request is stamped with, each a string. */
export type ResourceStamp = Readonly<Record<string, string>>;

const RESOURCE_LISTS: Readonly<Record<Signal, readonly [parts: string, scopes: string]>> = {
  traces: ["resourceSpans", "scopeSpans"],
  metrics: ["resourceMetrics", "scopeMetrics"],
  logs: ["resourceLogs", "scopeLogs"],
};

/** Thrown for a request that does not parse as one, which is then not exported. */
export class MalformedRequest extends Error {
  override name = "MalformedRequest";
}

function isStamped(key: string, stamp: ResourceStamp): boolean {
  return Object.hasOwn(stamp, key) || key.startsWith("alasio.");
}

// --- Binary protobuf ----------------------------------------------------------
// The levels the resource is at, by field number, the same for all three signals:
//   Export{Trace,Metrics,Logs}ServiceRequest  1: repeated Resource{Spans,Metrics,Logs}
//   Resource{Spans,Metrics,Logs}               1: Resource
//   Resource                                   1: repeated KeyValue attributes
//   KeyValue                                   1: string key, 2: AnyValue value
//   AnyValue                                   1: string string_value

const VARINT = 0;
const FIXED64 = 1;
const LEN = 2;
const FIXED32 = 5;

/** One field of a protobuf message. */
interface Field {
  readonly field: number;
  readonly wire: number;
  /** The field's whole encoding, tag and all. */
  readonly whole: Buffer;
  readonly value: Buffer;
}

function readVarint(buf: Buffer, at: number): [value: number, next: number] {
  let value = 0;
  for (let shift = 0; shift < 64; shift += 7) {
    const byte = buf[at++];
    if (byte === undefined) throw new MalformedRequest("truncated varint");
    value += (byte & 0x7f) * 2 ** shift;
    if ((byte & 0x80) === 0) return [value, at];
  }
  throw new MalformedRequest("overlong varint");
}

/** Each field of a message: its number, wire type, whole encoding, and value. */
function* fields(buf: Buffer): Generator<Field> {
  let at = 0;
  while (at < buf.length) {
    const start = at;
    let tag: number;
    [tag, at] = readVarint(buf, at);
    const field = Math.floor(tag / 8);
    const wire = tag % 8;
    if (field === 0) throw new MalformedRequest("field number 0");
    let valueStart = at;
    if (wire === VARINT) [, at] = readVarint(buf, at);
    else if (wire === FIXED64) at += 8;
    else if (wire === FIXED32) at += 4;
    else if (wire === LEN) {
      let length: number;
      [length, valueStart] = readVarint(buf, at);
      at = valueStart + length;
    } else throw new MalformedRequest(`wire type ${wire}`);
    if (at > buf.length) throw new MalformedRequest("truncated field");
    yield { field, wire, whole: buf.subarray(start, at), value: buf.subarray(valueStart, at) };
  }
}

/** A message field's value, which must be length-delimited. */
function message({ field, wire, value }: Field): Buffer {
  if (wire !== LEN) throw new MalformedRequest(`field ${field} has wire type ${wire}`);
  return value;
}

function varint(value: number): Buffer {
  const bytes: number[] = [];
  for (; value >= 0x80; value = Math.floor(value / 128)) bytes.push((value % 128) | 0x80);
  bytes.push(value);
  return Buffer.from(bytes);
}

function lengthDelimited(field: number, bytes: Buffer): Buffer {
  return Buffer.concat([varint(field * 8 + LEN), varint(bytes.length), bytes]);
}

/** A KeyValue's key, the last given as protobuf merges them, or "" for none. */
function keyOf(keyValue: Buffer): string {
  let key = "";
  for (const field of fields(keyValue)) {
    if (field.field === 1) key = message(field).toString("utf8");
  }
  return key;
}

function stampedAttributesProtobuf(stamp: ResourceStamp): Buffer[] {
  return Object.entries(stamp).map(([key, value]) =>
    lengthDelimited(1, Buffer.concat([
      lengthDelimited(1, Buffer.from(key, "utf8")),
      lengthDelimited(2, lengthDelimited(1, Buffer.from(value, "utf8"))),
    ])));
}

/**
 * A Resource{Spans,Metrics,Logs} with one Resource: every Resource it held merged as
 * protobuf merges repeated occurrences, less the stamped attributes, plus the stamp.
 */
function stampResourcePart(part: Buffer, stamp: ResourceStamp): Buffer {
  const resource: Buffer[] = [];
  const rest: Buffer[] = [];
  for (const field of fields(part)) {
    if (field.field !== 1) {
      rest.push(field.whole);
      continue;
    }
    for (const resourceField of fields(message(field))) {
      if (resourceField.field === 1 && isStamped(keyOf(message(resourceField)), stamp)) continue;
      resource.push(resourceField.whole);
    }
  }
  return Buffer.concat([lengthDelimited(1, Buffer.concat([...resource, ...stampedAttributesProtobuf(stamp)])), ...rest]);
}

function stampProtobuf(body: Buffer, stamp: ResourceStamp): Buffer {
  return Buffer.concat([...fields(body)].map((field) =>
    field.field === 1 ? lengthDelimited(1, stampResourcePart(message(field), stamp)) : field.whole));
}

// --- JSON ---------------------------------------------------------------------

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

function list(value: unknown, what: string): unknown[] {
  if (value === undefined) return [];
  if (!Array.isArray(value)) throw new MalformedRequest(`${what} is not a list`);
  return value;
}

function object(value: unknown, what: string): Record<string, unknown> {
  if (value === undefined) return {};
  if (!isObject(value)) throw new MalformedRequest(`${what} is not an object`);
  return value;
}

/** The fields of `source` named by `names` that it has. */
function only(source: Record<string, unknown>, names: readonly string[]): Record<string, unknown> {
  return Object.fromEntries(names.filter((name) => source[name] !== undefined).map((name) => [name, source[name]]));
}

function stampJson(signal: Signal, body: Buffer, stamp: ResourceStamp): Buffer {
  let request: unknown;
  try {
    request = JSON.parse(body.toString("utf8"));
  } catch (error) {
    throw new MalformedRequest(error instanceof Error ? error.message : String(error));
  }
  const [partsName, scopesName] = RESOURCE_LISTS[signal];
  const parts = list(object(request, "the request")[partsName], partsName).map((value) => {
    const part = object(value, partsName);
    const resource = object(part["resource"], "resource");
    const attributes = list(resource["attributes"], "resource.attributes")
      .filter((attribute) => !(isObject(attribute) && typeof attribute["key"] === "string" && isStamped(attribute["key"], stamp)));
    return {
      resource: {
        ...only(resource, ["droppedAttributesCount", "entityRefs"]),
        attributes: [...attributes, ...Object.entries(stamp).map(([key, value]) => ({ key, value: { stringValue: value } }))],
      },
      ...only(part, [scopesName, "schemaUrl"]),
    };
  });
  return Buffer.from(JSON.stringify({ [partsName]: parts }), "utf8");
}

/**
 * `body`, an OTLP export request for `signal` in `encoding` ("protobuf" or "json"),
 * with its resources stamped. Throws MalformedRequest for one that does not parse.
 */
export function stampResources(signal: Signal, encoding: OtlpEncoding, body: Buffer, stamp: ResourceStamp): Buffer {
  if (!Object.hasOwn(RESOURCE_LISTS, signal)) throw new Error(`unknown signal ${signal}`);
  return encoding === "json" ? stampJson(signal, body, stamp) : stampProtobuf(body, stamp);
}
