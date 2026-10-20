/**
 * alasio's client of the sandbox gateway (src/sandbox/gateway-client.ts), against a stand-in of
 * the gateway that checks each caller's token as it does, by the environment's own key of its
 * caller key, and records what it was asked.
 */
import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer, type IncomingMessage } from "node:http";
import type { AddressInfo } from "node:net";
import { test, type TestContext } from "node:test";

import { Effect } from "effect";

import { environmentKey } from "../src/binder/names.ts";
import { type TokenClaims, verifyToken } from "../src/binder/tokens.ts";
import { makeGatewayWorkspaces, TOKEN_TTL_MS } from "../src/sandbox/gateway-client.ts";

const master = Buffer.from("the gateway's caller key");

interface Asked {
  readonly method: string;
  readonly url: string;
  readonly claims: TokenClaims | null;
  readonly body: string;
}

/** A stand-in of the gateway at the loopback: what it was asked, and what it answers, by `answer`. */
async function gateway(t: TestContext, answer: (asked: Asked) => { status: number; body?: string | Uint8Array; headers?: Record<string, string> }) {
  const asked: Asked[] = [];
  const server = createServer(async (incoming: IncomingMessage, outgoing) => {
    const chunks: Buffer[] = [];
    for await (const chunk of incoming) chunks.push(chunk as Buffer);
    const bearer = String(incoming.headers.authorization ?? "").replace(/^Bearer /u, "");
    // As the gateway checks a caller's token: by the key of the environment it names.
    const claims = verifyToken(({ options }) => (options?.["environment"] ? environmentKey(master, options["environment"]) : null), bearer, Date.now());
    const each = { method: incoming.method ?? "", url: incoming.url ?? "", claims, body: Buffer.concat(chunks).toString("utf8") };
    asked.push(each);
    const { status, body, headers } = answer(each);
    outgoing.writeHead(status, headers).end(body);
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => server.close());
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}/`;
  return { asked, workspaces: makeGatewayWorkspaces({ url, environment: "try", callerKey: environmentKey(master, "try") }) };
}

const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect);
const refused = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(Effect.flip(effect));

test("bayma's endpoint is the gateway's /mcp, with a caller's token of the workspace's key, its environment and internet, good for hours, by its environment's own key", async (t) => {
  const { workspaces } = await gateway(t, () => ({ status: 200 }));
  const endpoint = await run(workspaces.endpoint("fs-abc123", "full"));
  assert.match(endpoint.url, /^http:\/\/127\.0\.0\.1:\d+\/mcp$/u);
  const claims = verifyToken(environmentKey(master, "try"), String(endpoint.headers["Authorization"]).replace(/^Bearer /u, ""), Date.now());
  assert.deepEqual({ ...claims, expires: undefined }, { key: "sessionfs:fs-abc123", expires: undefined, options: { environment: "try", mode: "full" } });
  assert.ok((claims?.expires ?? 0) > Date.now() + TOKEN_TTL_MS - 60_000);
  // Not by main's, nor by the master.
  assert.equal(verifyToken(master, String(endpoint.headers["Authorization"]).replace(/^Bearer /u, ""), Date.now()), null);
});

test("a workspace is made, measured, read, written, listed, forked and deleted as the gateway answers each, what it refuses its status and what it said", async (t) => {
  const { asked, workspaces } = await gateway(t, ({ method, url }) => {
    if (method === "GET" && url.startsWith("/workspace/quota")) return { status: 200, body: JSON.stringify({ capacityBytes: 10, usedBytes: 2 }) };
    if (method === "GET" && url.startsWith("/workspace/files")) return { status: 200, body: Uint8Array.from([0, 255, 10]) };
    if (method === "GET" && url === "/workspace/all") return { status: 200, body: JSON.stringify({ workspaces: ["fs-abc123", "fs-def456"] }) };
    if (method === "DELETE") return { status: 409, body: JSON.stringify({ error: "fs-abc123 is bound" }) };
    return { status: 204 };
  });
  await run(workspaces.create("fs-abc123"));
  assert.deepEqual(await run(workspaces.quota("fs-abc123")), { capacityBytes: 10, usedBytes: 2 });
  assert.deepEqual(await run(workspaces.readFile("fs-abc123", "workspace/out/a b.png", 1024)), Uint8Array.from([0, 255, 10]));
  await run(workspaces.writeFile("fs-abc123", "home/.alasio/received/1/note.txt", Buffer.from("hello")));
  assert.deepEqual(await run(workspaces.list), ["fs-abc123", "fs-def456"]);
  await run(workspaces.fork("fs-abc123", { environment: "try", workspace: "fs-fork01" }));
  const deleted = await refused(workspaces.remove("fs-abc123"));
  assert.deepEqual([deleted.status, deleted.reason], [409, "fs-abc123 is bound"]);
  assert.deepEqual(asked.map(({ method, url, body }) => `${method} ${url} ${body}`), [
    "POST /workspace ",
    "GET /workspace/quota ",
    "GET /workspace/files?path=workspace%2Fout%2Fa+b.png&max=1024 ",
    "PUT /workspace/files?path=home%2F.alasio%2Freceived%2F1%2Fnote.txt hello",
    "GET /workspace/all ",
    `POST /workspace/forks ${JSON.stringify({ environment: "try", workspace: "fs-fork01" })}`,
    "DELETE /workspace ",
  ]);
  // Each of the workspace's key, its environment's, by its environment's own key.
  assert.ok(asked.filter(({ url }) => url !== "/workspace/all").every(({ claims }) => claims?.key === "sessionfs:fs-abc123" && claims.options?.["environment"] === "try"));
  // One it has no quota of: none.
  const none = await gateway(t, () => ({ status: 404, body: JSON.stringify({ error: "fs-gone has no quota" }) }));
  assert.equal(await run(none.workspaces.quota("fs-gone")), null);
});

test("a bind asked ahead is asked again while the gateway's pool has no pod for it, then bound; one refused otherwise fails at once; and a release lets it go", async (t) => {
  let refusals = 2;
  const { asked, workspaces } = await gateway(t, ({ url }) => {
    if (url === "/bind" && refusals > 0) return ((refusals -= 1), { status: 503, body: JSON.stringify({ error: "no pod came free in time" }), headers: { "retry-after": "1" } });
    return { status: 204 };
  });
  await run(workspaces.bind("fs-abc123", "none"));
  await run(workspaces.release("fs-abc123", "none"));
  assert.deepEqual(asked.map(({ method, url }) => `${method} ${url}`), ["POST /bind", "POST /bind", "POST /bind", "POST /release"]);
  assert.ok(asked.every(({ claims }) => claims?.options?.["mode"] === "none"));
  const elsewhere = await gateway(t, () => ({ status: 403, body: JSON.stringify({ error: "there is no environment try of sessions" }) }));
  const failed = await refused(elsewhere.workspaces.bind("fs-abc123", "none"));
  assert.deepEqual([failed.status, failed.reason, elsewhere.asked.length], [403, "there is no environment try of sessions", 1]);
});

test("a file's write is asked again while the gateway answers it 503, as a node's mount is not there for it, then written; one refused otherwise fails at once", async (t) => {
  let refusals = 2;
  const { asked, workspaces } = await gateway(t, ({ method }) => {
    if (method === "PUT" && refusals > 0) return ((refusals -= 1), { status: 503, body: JSON.stringify({ error: "workspace storage is not mounted on this node now" }), headers: { "retry-after": "2" } });
    return { status: 204 };
  });
  await run(workspaces.writeFile("fs-abc123", "home/.alasio/received/1/note.txt", Buffer.from("hello")));
  assert.deepEqual(asked.map(({ method, body }) => `${method} ${body}`), ["PUT hello", "PUT hello", "PUT hello"]);
  const failing = await gateway(t, () => ({ status: 502, body: JSON.stringify({ error: "the node agent of node-a could not write it" }) }));
  const failed = await refused(failing.workspaces.writeFile("fs-abc123", "home/.alasio/received/1/note.txt", Buffer.from("hello")));
  assert.deepEqual([failed.status, failed.reason, failing.asked.length], [502, "the node agent of node-a could not write it", 1]);
});

test("a folder conversation's instance is reached, bound and released through the gateway by a caller's token of its folder key, of its environment alone; a bind that failed is not asked again, saying why, and whether it is held", async (t) => {
  let binds = 0;
  const { asked, workspaces } = await gateway(t, ({ url }) => {
    if (url === "/folders/starting") return { status: 200, body: JSON.stringify({ starting: "whose image is being pulled" }) };
    if (url !== "/bind") return { status: 204 };
    binds += 1;
    if (binds === 1) return { status: 503, body: JSON.stringify({ error: "folder:claude:telegram:42 could not be bound now", reason: "its pod is pending: no node" }) };
    if (binds === 2) return { status: 503, body: JSON.stringify({ error: "folder:claude:telegram:42 is bound again only as its caller asks", reason: "its last 3 binds failed", held: true }) };
    return { status: 204 };
  });
  const scope = { harness: "claude", threadKey: "telegram:42" };
  const endpoint = await run(workspaces.folders.endpoint(scope));
  const claims = verifyToken(environmentKey(master, "try"), String(endpoint.headers["Authorization"]).replace(/^Bearer /u, ""), Date.now());
  assert.deepEqual({ ...claims, expires: undefined }, { key: "folder:claude:telegram:42", expires: undefined, options: { environment: "try" } });
  const failed = await run(Effect.flip(workspaces.folders.bind(scope)));
  assert.deepEqual([failed._tag, failed.message, failed._tag === "FolderBindFailed" && failed.held], ["FolderBindFailed", "its pod is pending: no node", false]);
  const held = await run(Effect.flip(workspaces.folders.bind(scope)));
  assert.deepEqual([held.message, held._tag === "FolderBindFailed" && held.held], ["its last 3 binds failed", true]);
  // As the operator asks, its token saying so.
  await run(workspaces.folders.bind(scope, { retry: true }));
  await run(workspaces.folders.release(scope));
  // What a bind of it under way waits on, as the gateway answers it.
  assert.equal(await run(workspaces.folders.starting(scope)), "whose image is being pulled");
  assert.deepEqual(asked.map(({ method, url, claims: of }) => `${method} ${url} ${of?.key} ${of?.options?.["retry"] ?? ""}`), [
    "POST /bind folder:claude:telegram:42 ",
    "POST /bind folder:claude:telegram:42 ",
    "POST /bind folder:claude:telegram:42 1",
    "POST /release folder:claude:telegram:42 ",
    "GET /folders/starting folder:claude:telegram:42 ",
  ]);
});
