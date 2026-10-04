/**
 * The operator's config (cli/src/config.ts): checked with its defaults and errors that
 * say which key and why, where XDG says it is, and written for its owner's eyes only.
 */
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";

import { NodeServices } from "@effect/platform-node";
import { ConfigProvider, Effect, Option, Result } from "effect";

import { ConfigFlag, configPath, decodeOperatorConfig, defaultStoragePath, installConfigOf, readConfig, writeConfig } from "../src/config.ts";
import { resolveTarget } from "../src/target.ts";

const PATH = "/home/op/.config/alasio/config.json";

/** The reason `input` is refused for. */
function refusal(input: unknown): string {
  const decoded = decodeOperatorConfig(PATH, input);
  assert.ok(Result.isFailure(decoded), "refused");
  return decoded.failure.message;
}

const run = <A, E>(effect: Effect.Effect<A, E, NodeServices.NodeServices>, env: Record<string, string> = {}): Promise<A> =>
  Effect.runPromise(effect.pipe(Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env }))), Effect.provide(NodeServices.layer)));

function directory(t: TestContext): string {
  const made = mkdtempSync(join(tmpdir(), "alasio-config-"));
  t.after(() => rmSync(made, { recursive: true, force: true }));
  return made;
}

test("a local target is defaulted, and keeps the port and storage it was given", () => {
  const decoded = decodeOperatorConfig(PATH, { target: { local: { apiPort: 41873 } } });
  assert.ok(Result.isSuccess(decoded));
  assert.deepEqual(decoded.success.target, { local: { name: "alasio", apiPort: 41873, hostAliases: [], mounts: [], agents: 0 } });
  assert.deepEqual(decoded.success.install, {});
});

test("a kubeconfig target needs neither its path nor its context", () => {
  const decoded = decodeOperatorConfig(PATH, { target: { kubeconfig: {} }, install: { telemetry: { otlpEndpoint: "http://collector:4318" } } });
  assert.ok(Result.isSuccess(decoded));
  assert.deepEqual(decoded.success.target, { kubeconfig: {} });
});

test("what is refused is said with the file, the key and why", () => {
  assert.equal(refusal({}), `${PATH}: target is required: alasio init writes it`);
  assert.equal(refusal({ target: { local: {} } }), `${PATH}: target.local.apiPort is required: alasio init chooses one`);
  assert.equal(refusal({ target: { local: { apiPort: 70000 } } }), `${PATH}: target.local.apiPort must be a port from 1 to 65535`);
  assert.equal(refusal({ target: { local: { apiPort: 1, storagePath: "data" } } }), `${PATH}: target.local.storagePath must be an absolute path`);
  assert.match(refusal({ target: { kubeconfig: {} }, telemetry: {} }), /^\/home\/op\/\.config\/alasio\/config\.json: telemetry /u);
  assert.equal(
    refusal({ target: { kubeconfig: {} }, install: { host: { mounts: [{ name: "home", hostPath: "home", mountPath: "/home" }] } } }),
    `${PATH}: install.host.mounts.0.hostPath must be an absolute path`,
  );
});

test("the Secrets alasio names are not the config's to say", () => {
  assert.equal(
    refusal({ target: { kubeconfig: {} }, install: { alasio: { telegram: { existingSecret: "mine" } } } }),
    `${PATH}: install.alasio.telegram is alasio's own to say: its Secret is the one alasio init writes`,
  );
  assert.match(refusal({ target: { kubeconfig: {} }, install: { alasio: { claude: {} } } }), /install\.alasio\.claude is alasio's own/u);
});

test("the install configuration names the bot's Secret, and Claude Code's when the cluster has it", () => {
  const without = installConfigOf({}, { claude: false });
  const withClaude = installConfigOf({ alasio: { defaultHarness: "codex" } }, { claude: true });
  assert.ok(Result.isSuccess(without) && Result.isSuccess(withClaude));
  assert.equal(without.success.alasio.telegram.existingSecret, "alasio-telegram");
  assert.equal(without.success.alasio.claude.existingSecret, "");
  assert.equal(withClaude.success.alasio.claude.existingSecret, "alasio-claude");
  assert.equal(withClaude.success.alasio.defaultHarness, "codex");
});

test("the config is where XDG_CONFIG_HOME says, else under HOME, unless --config says", async () => {
  const at = (env: Record<string, string>, flag: Option.Option<string> = Option.none()) => run(Effect.provideService(configPath, ConfigFlag, flag), env);
  assert.equal(await at({ XDG_CONFIG_HOME: "/xdg", HOME: "/home/op" }), "/xdg/alasio/config.json");
  assert.equal(await at({ HOME: "/home/op" }), "/home/op/.config/alasio/config.json");
  assert.equal(await at({ XDG_CONFIG_HOME: "", HOME: "/home/op" }), "/home/op/.config/alasio/config.json");
  assert.equal(await at({ HOME: "/home/op" }, Option.some("/etc/alasio.json")), "/etc/alasio.json");
  assert.equal(await run(defaultStoragePath, { HOME: "/home/op" }), "/home/op/.local/share/alasio/storage");
  assert.equal(await run(defaultStoragePath, { XDG_DATA_HOME: "/data", HOME: "/home/op" }), "/data/alasio/storage");
});

test("the config is written readable by its owner alone, in a directory only its owner opens", async (t) => {
  const path = join(directory(t), "alasio", "config.json");
  await run(writeConfig(path, { target: { local: { apiPort: 41873 } } }));
  assert.equal(statSync(path).mode & 0o777, 0o600);
  assert.equal(statSync(join(path, "..")).mode & 0o777, 0o700);
  assert.deepEqual(JSON.parse(readFileSync(path, "utf8")), { target: { local: { apiPort: 41873 } } });
  const read = await run(readConfig(path));
  assert.equal(read?.path, path);
  assert.deepEqual(read?.file, { target: { local: { apiPort: 41873 } } });
});

test("a config that is refused is not written", async (t) => {
  const path = join(directory(t), "config.json");
  const error = await run(Effect.flip(writeConfig(path, { target: { local: { apiPort: 0 } } })));
  assert.equal(error.message, `${path}: target.local.apiPort must be a port from 1 to 65535`);
  assert.throws(() => statSync(path), /ENOENT/u);
});

test("no config reads as none, and one that is not JSON says so", async (t) => {
  const made = directory(t);
  assert.equal(await run(readConfig(join(made, "absent.json"))), null);
  writeFileSync(join(made, "broken.json"), "{");
  const error = await run(Effect.flip(readConfig(join(made, "broken.json"))));
  assert.match(error.message, /broken\.json: is not JSON: /u);
});

test("the local cluster mounts the host profile's paths, and keeps its storage under XDG_DATA_HOME unless given", async () => {
  const decoded = decodeOperatorConfig(PATH, {
    target: { local: { apiPort: 41873, mounts: [{ source: "/srv", target: "/srv" }] } },
    install: { host: { enabled: true, mounts: [{ name: "home", hostPath: "/home/op", mountPath: "/home/op" }, { name: "srv", hostPath: "/srv", mountPath: "/srv" }] } },
  });
  assert.ok(Result.isSuccess(decoded));
  const target = await run(resolveTarget(decoded.success), { XDG_DATA_HOME: "/data" });
  assert.ok(target._tag === "Local");
  assert.equal(target.cluster.storagePath, "/data/alasio/storage");
  assert.deepEqual(target.cluster.mounts, [{ source: "/srv", target: "/srv" }, { source: "/home/op", target: "/home/op", readOnly: false }]);
  assert.equal(target.kubeconfig.path, "/home/op/.config/alasio/kubeconfig");
  assert.equal(target.cluster.image, undefined);
});

test("the local cluster runs the node image the config names, such as one built here", async () => {
  const decoded = decodeOperatorConfig(PATH, { target: { local: { apiPort: 41873, storagePath: "/srv/storage", image: "alasio-node:dev" } } });
  assert.ok(Result.isSuccess(decoded));
  const target = await run(resolveTarget(decoded.success));
  assert.ok(target._tag === "Local");
  assert.equal(target.cluster.image, "alasio-node:dev");
});
