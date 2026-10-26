/**
 * Puts a profile's toxics on a toxiproxy proxy in front of the store (DESIGN.md section 4): the
 * answer's first byte delayed by the profile's latency, and each connection's bandwidth held to
 * the profile's, both directions. The proxy is made anew, so nothing of a previous profile stays.
 *
 *   node shape.ts --profile s3-standard [--api http://127.0.0.1:8474] [--listen 0.0.0.0:28333] [--upstream 127.0.0.1:8333]
 *
 * Prints the proxy as toxiproxy then describes it. The `local` profile removes the proxy.
 */
import { readFileSync } from "node:fs";
import { parseArgs } from "node:util";

export interface Profile {
  readonly emulated: boolean;
  readonly first_byte_ms: number | null;
  readonly bytes_per_second: number | null;
}

export const PROFILES = JSON.parse(readFileSync(new URL("./profiles.json", import.meta.url), "utf8")) as Readonly<Record<string, Profile>>;

/** The proxy's name in toxiproxy. */
const NAME = "store";

/** The toxics of `profile`, as toxiproxy takes them: its bandwidth toxic's rate is in KB/s, bytes per millisecond. */
export function toxicsOf(profile: Profile): readonly Record<string, unknown>[] {
  const toxics: Record<string, unknown>[] = [];
  if (profile.first_byte_ms !== null) toxics.push({ name: "first-byte", type: "latency", stream: "downstream", toxicity: 1, attributes: { latency: profile.first_byte_ms, jitter: 0 } });
  if (profile.bytes_per_second !== null) {
    const rate = Math.round(profile.bytes_per_second / 1000);
    toxics.push({ name: "bandwidth-down", type: "bandwidth", stream: "downstream", toxicity: 1, attributes: { rate } });
    toxics.push({ name: "bandwidth-up", type: "bandwidth", stream: "upstream", toxicity: 1, attributes: { rate } });
  }
  return toxics;
}

async function ask(api: string, method: string, path: string, body?: unknown): Promise<unknown> {
  const answer = await fetch(`${api}${path}`, { method, headers: { "content-type": "application/json" }, ...(body === undefined ? {} : { body: JSON.stringify(body) }) });
  const text = await answer.text();
  if (!answer.ok && !(method === "DELETE" && answer.status === 404)) throw new Error(`toxiproxy ${method} ${path}: ${answer.status} ${text}`);
  return text === "" ? null : JSON.parse(text);
}

export async function shape(api: string, profileName: string, listen: string, upstream: string): Promise<unknown> {
  const profile = PROFILES[profileName];
  if (!profile) throw new Error(`no profile ${profileName}: ${Object.keys(PROFILES).join(", ")}`);
  await ask(api, "DELETE", `/proxies/${NAME}`);
  if (!profile.emulated) return null;
  await ask(api, "POST", "/proxies", { name: NAME, listen, upstream, enabled: true });
  for (const toxic of toxicsOf(profile)) await ask(api, "POST", `/proxies/${NAME}/toxics`, toxic);
  return ask(api, "GET", `/proxies/${NAME}`);
}

// Run as a program, not imported, nor bundled into another's (../driver/build.ts).
if (/(^|\/)shape\.(ts|mjs)$/u.test(process.argv[1] ?? "")) {
  const { values } = parseArgs({
    options: {
      profile: { type: "string" },
      api: { type: "string", default: "http://127.0.0.1:8474" },
      listen: { type: "string", default: "0.0.0.0:28333" },
      upstream: { type: "string", default: "127.0.0.1:8333" },
    },
  });
  if (!values.profile) throw new Error("--profile is required");
  console.log(JSON.stringify(await shape(values.api ?? "", values.profile, values.listen ?? "", values.upstream ?? "")));
}
