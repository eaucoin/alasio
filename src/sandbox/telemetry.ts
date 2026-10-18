/**
 * What the sandbox service's receiver (../receiver/receiver.ts) stamps on what bayma inside
 * a session's sandbox exports, as it takes it from the sandbox, before it exports it where
 * alasio exports its own: what alasio's own telemetry shares, and the session's volume id.
 */
import { parseKeyValueList, sharedResourceAttributes } from "../telemetry/index.ts";
import type { ResourceStamp } from "./otlp-resource.ts";

/** The resource attributes a session's telemetry is stamped with. */
export function sandboxResource(volumeId: string, env: Readonly<NodeJS.ProcessEnv> = process.env): ResourceStamp {
  return { ...parseKeyValueList(sharedResourceAttributes(env)), "service.name": "bayma", "alasio.volume.id": volumeId };
}
