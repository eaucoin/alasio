import assert from "node:assert/strict";
import { test } from "node:test";

import { Effect } from "effect";

import { buildCodexThreadConfig, codexMcpServer } from "../src/codex/thread-config.ts";
import { claudeMcpServers } from "../src/harness/claude/mcp.ts";
import { type BaymaMcpServer, folderBayma, HostBayma, noFolderBayma } from "../src/mcp/bayma.ts";
import type { FolderScope } from "../src/sandbox/gateway-client.ts";

const BAYMA: BaymaMcpServer = { type: "http", url: "http://alasio-sandbox-gateway.alasio.svc:8080/mcp", headers: { Authorization: "Bearer folder.t" } };

test("a deployment whose gateway serves no folder instances offers no folder bayma", async () => {
  const none = Effect.runSync(folderBayma);
  assert.equal(none, noFolderBayma);
  const refused = await Effect.runPromise(Effect.flip(none({ harness: "claude", threadKey: "telegram:42" })));
  assert.equal(refused.message, "this deployment offers no folder workspaces: it has no host profile");
});

test("a folder conversation's bayma is its folder instance, bound through the gateway before its endpoint is given, in Claude Code's shape", async () => {
  const calls: string[] = [];
  const said = (verb: string) => (scope: FolderScope) => Effect.sync(() => void calls.push(`${verb} ${scope.harness} ${scope.threadKey}`));
  const folders = {
    bind: said("bind"),
    release: said("release"),
    endpoint: (scope: FolderScope) => Effect.as(said("endpoint")(scope), { url: BAYMA.url, headers: BAYMA.headers }),
  };
  const server = await Effect.runPromise(Effect.provide(
    Effect.flatMap(folderBayma, (ensure) => ensure({ harness: "claude", threadKey: "telegram:42" })),
    HostBayma.layer(folders),
  ));
  assert.deepEqual(server, BAYMA);
  assert.deepEqual(calls, ["bind claude telegram:42", "endpoint claude telegram:42"]);
});

test("alasio adds bayma to Claude Code's servers, in its shape", () => {
  assert.deepEqual(claudeMcpServers(BAYMA), { bayma: BAYMA });
});

test("Codex threads add bayma over HTTP and leave the operator's servers and apps connector on", () => {
  const config = buildCodexThreadConfig({ codexEnv: {}, bayma: BAYMA });
  assert.deepEqual(config.mcp_servers, { bayma: codexMcpServer(BAYMA) });
  assert.deepEqual(codexMcpServer(BAYMA), { url: BAYMA.url, http_headers: BAYMA.headers, startup_timeout_sec: 60 });
  // The config's type has no `features`; this pins that none is sent at all.
  assert.equal(Reflect.get(config, "features"), undefined);
});
