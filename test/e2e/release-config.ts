/**
 * The upgrade shard's configuration of this checkout (./harness.ts), laid over the one
 * `alasio init` of the release before wrote, keeping what only that has, as the cut-over's
 * UPGRADE does just before it upgrades: so that, until it does, the configuration is the one
 * that release reads, as on the box.
 *
 *   node test/e2e/release-config.ts <config file> <install configuration file>
 */
import { readFileSync, writeFileSync } from "node:fs";

/** `over` laid over `base`: objects key by key, anything else `over` itself. */
export function overlaid(base: unknown, over: unknown): unknown {
  const isObject = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);
  if (!isObject(base) || !isObject(over)) return over;
  return { ...base, ...Object.fromEntries(Object.entries(over).map(([key, value]) => [key, overlaid(base[key], value)])) };
}

if (import.meta.main) {
  const [configFile = "", installFile = ""] = process.argv.slice(2);
  const config: Record<string, unknown> = JSON.parse(readFileSync(configFile, "utf8"));
  const install: unknown = JSON.parse(readFileSync(installFile, "utf8"));
  writeFileSync(configFile, JSON.stringify({ ...config, install: overlaid(config["install"], install) }), { mode: 0o600 });
}
