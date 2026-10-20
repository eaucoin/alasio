/**
 * The object store's S3 gateway's GET limiter, as its pod runs it (./get-limiter.ts).
 *
 * Environment: GET_LIMITER_PORT (what it serves), GET_LIMITER_UPSTREAM (`weed s3`'s
 * `host:port`), GET_LIMITER_STREAMS (how many GETs may be under way there at once).
 */
import { serveGetLimiter } from "./get-limiter.ts";

/** A positive integer of the environment variable `name`. */
function count(name: string): number {
  const value = Number(process.env[name]);
  if (!Number.isInteger(value) || value < 1) throw new Error(`${name} must be a positive integer, not ${process.env[name] ?? "unset"}`);
  return value;
}

const [host = "", port = ""] = (process.env["GET_LIMITER_UPSTREAM"] ?? "").split(":");
const limiter = await serveGetLimiter({ port: count("GET_LIMITER_PORT"), upstream: { host, port: Number(port) }, streams: count("GET_LIMITER_STREAMS") });
process.once("SIGTERM", () => void limiter.close().then(() => process.exit(0)));
