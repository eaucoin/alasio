import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, test } from "node:test";

import { Array as Arr, Effect, Exit, Logger, Scope } from "effect";

import { mintToken } from "../src/binder/tokens.ts";
import { loginUpstream, poolLoginKey, poolLoginRefusal, startLoginRelay, type LoginUpstream } from "../src/codex/login-relay.ts";
import { makeGivenCodexLogin } from "../src/harness/given-codex-login.ts";
import type { TurnStart } from "../src/harness/host.ts";

const dir = mkdtempSync(join(tmpdir(), "alasio-login-relay-"));
const authFile = join(dir, "auth.json");
const writeAuth = (auth: unknown) => writeFileSync(authFile, JSON.stringify(auth));

/** What a request arrived at the upstream with. */
interface UpstreamRequest {
  readonly path: string | undefined;
  readonly auth: string | null;
  readonly accountId: string | string[] | null;
  readonly body: string;
}

// A stand-in upstream that records what each request arrived with.
let upstream: Server;
let upstreamOrigin: string;
const seen: UpstreamRequest[] = [];
before(async () => {
  upstream = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => chunks.push(chunk));
    req.on("end", () => {
      seen.push({ path: req.url, auth: req.headers.authorization ?? null, accountId: req.headers["chatgpt-account-id"] ?? null, body: Buffer.concat(chunks).toString() });
      res.writeHead(200, { "content-type": "application/json" }).end('{"ok":true}');
    });
  });
  await new Promise<void>((resolve) => upstream.listen(0, "127.0.0.1", resolve));
  // Listening on a TCP port, the server's address is an AddressInfo.
  upstreamOrigin = `http://127.0.0.1:${(upstream.address() as AddressInfo).port}`;
});
after(async () => {
  await new Promise((resolve) => upstream.close(resolve));
  rmSync(dir, { recursive: true, force: true });
});

test("a ChatGPT login goes to the Codex backend with its account id; an API key to the API as it is", () => {
  writeAuth({ tokens: { access_token: "chatgpt-token", account_id: "acct-1" } });
  assert.deepEqual(loginUpstream(authFile, "/v1/responses"), {
    origin: "https://chatgpt.com",
    path: "/backend-api/codex/responses",
    headers: { authorization: "Bearer chatgpt-token", "chatgpt-account-id": "acct-1" },
  });
  writeAuth({ OPENAI_API_KEY: "sk-key", tokens: null });
  assert.deepEqual(loginUpstream(authFile, "/v1/responses"), {
    origin: "https://api.openai.com",
    path: "/v1/responses",
    headers: { authorization: "Bearer sk-key" },
  });
  writeAuth({});
  assert.equal(loginUpstream(authFile, "/v1/responses"), null);
  assert.equal(loginUpstream(join(dir, "missing.json"), "/v1/responses"), null);
});

test("the relay takes only its own bearer and the model API's paths, and reads the login fresh each time", async () => {
  // The real login resolution, pointed at the stand-in upstream.
  const upstreamFor = (file: string, path: string): LoginUpstream | null => {
    const real = loginUpstream(file, path);
    return real && { ...real, origin: upstreamOrigin };
  };
  const listening = Effect.runSync(Scope.make());
  const relay = await Effect.runPromise(startLoginRelay({ authFile, bearer: "relay-bearer", upstreamFor }).pipe(
    Scope.provide(listening),
    Effect.provide(Logger.layer([])),
  ));
  const call = (path: string, bearer?: string, body = "{}") =>
    fetch(`${relay.url.replace(/\/v1$/, "")}${path}`, { method: "POST", headers: bearer ? { authorization: `Bearer ${bearer}` } : {}, body });
  try {
    assert.match(relay.url, /^http:\/\/127\.0\.0\.1:\d+\/v1$/);
    writeAuth({ tokens: { access_token: "token-1", account_id: "acct-1" } });
    seen.length = 0;
    const ok = await call("/v1/responses", "relay-bearer", '{"input":"hi"}');
    assert.equal(ok.status, 200);
    assert.deepEqual(seen.at(-1), { path: "/backend-api/codex/responses", auth: "Bearer token-1", accountId: "acct-1", body: '{"input":"hi"}' });
    assert.equal((await call("/v1/responses/compact", "relay-bearer")).status, 200); // the Responses API's subpaths too

    // A token the operator's Codex refreshes on disk is used from the next request on.
    writeAuth({ tokens: { access_token: "token-2", account_id: "acct-1" } });
    await call("/v1/responses", "relay-bearer");
    assert.equal(seen.at(-1)?.auth,"Bearer token-2");

    const before = seen.length;
    assert.equal((await call("/v1/responses", "guessed")).status, 401);
    assert.equal((await call("/v1/responses")).status, 401);
    assert.equal((await call("/v1/files", "relay-bearer")).status, 403);
    assert.equal((await call("/backend-api/codex/responses", "relay-bearer")).status, 403);
    assert.equal(seen.length, before); // none of those reached the upstream
    writeAuth({});
    assert.equal((await call("/v1/responses", "relay-bearer")).status, 503);
  } finally {
    await Effect.runPromise(Scope.close(listening, Exit.void));
  }
  // Closed with its scope.
  await assert.rejects(fetch(relay.url));
});

test("alasio's relay to its harness pools takes a token of a conversation's turn's alone, saying why it refuses in its log alone; a pod relays to it on its loopback with the latest token its turns gave, its Codex home given the operator's configuration and never their login", async () => {
  const key = poolLoginKey(Buffer.from("main's key"));
  const token = (claims: { readonly key: string; readonly expires?: number }, signedBy = key) => mintToken(signedBy, claims);
  const upstreamFor = (file: string, path: string): LoginUpstream | null => {
    const real = loginUpstream(file, path);
    return real && { ...real, origin: upstreamOrigin };
  };
  const logged: string[] = [];
  const logger = Logger.layer([Logger.make(({ message }) => void logged.push(Arr.ensure(message).join(" ")))]);
  const listening = Effect.runSync(Scope.make());
  const home = mkdtempSync(join(tmpdir(), "alasio-pod-codex-"));
  try {
    const alasios = await Effect.runPromise(startLoginRelay({ authFile, authorize: poolLoginRefusal(key, Date.now), upstreamFor }).pipe(Scope.provide(listening), Effect.provide(logger)));
    const origin = alasios.url.replace(/\/v1$/u, "");
    const asked = (bearer: string | null) => fetch(`${origin}/v1/responses`, { method: "POST", headers: bearer === null ? {} : { authorization: `Bearer ${bearer}` }, body: "{}" });
    writeAuth({ tokens: { access_token: "alasio-token", account_id: "acct-1" } });
    const before = seen.length;
    for (const [bearer, why] of [
      [null, "it bears no token"],
      [token({ key: "harness:telegram:41", expires: Date.now() + 60_000 }, Buffer.from("another key")), "its token is none of alasio's, or has stopped"],
      [token({ key: "harness:telegram:41", expires: Date.now() - 1 }), "its token is none of alasio's, or has stopped"],
      [token({ key: "sessionfs:fs-abc123", expires: Date.now() + 60_000 }), "its token is of sessionfs:fs-abc123, no conversation's turn's"],
      [token({ key: "harness:telegram:41" }), "its token is of harness:telegram:41, no conversation's turn's"],
    ] as const) {
      const refused = await asked(bearer);
      assert.deepEqual([refused.status, await refused.text()], [401, "refused"]);
      assert.ok(logged.some((line) => line.endsWith(why)), `${why}: ${logged.join("; ")}`);
    }
    assert.equal(seen.length, before);

    // A pod's loopback, before any turn gave it a token: no login to call the model with.
    const pod = await Effect.runPromise(makeGivenCodexLogin({ home }).pipe(Scope.provide(listening), Effect.provide(logger)));
    const viaPod = () => fetch(`${pod.relay.url}/responses`, { method: "POST", headers: { authorization: `Bearer ${pod.relay.bearer}` }, body: '{"input":"hi"}' });
    assert.equal((await viaPod()).status, 503);
    // A turn gives it alasio's relay, its token, and the operator's configuration, which carries no login.
    const turn = (home: Readonly<Record<string, string>> | null, expires = Date.now() + 60_000) => ({ codex: { login: { url: alasios.url, token: token({ key: "harness:telegram:41", expires }) }, home } }) as unknown as TurnStart;
    await Effect.runPromise(pod.give(turn({ "config.toml": 'model = "gpt-5"\n', "skills/review/SKILL.md": "Review it.\n", "auth.json": "{}", "../outside": "x" })));
    assert.equal((await viaPod()).status, 200);
    assert.deepEqual(seen.at(-1), { path: "/backend-api/codex/responses", auth: "Bearer alasio-token", accountId: "acct-1", body: '{"input":"hi"}' });
    assert.deepEqual([readFileSync(join(home, "config.toml"), "utf8"), readFileSync(join(home, "skills/review/SKILL.md"), "utf8")], ['model = "gpt-5"\n', "Review it.\n"]);
    assert.deepEqual([existsSync(join(home, "auth.json")), existsSync(join(home, "..", "outside"))], [false, false]);
    // The next turn's configuration in place of the last, what the operator removed removed; and its token, the last's stopped.
    await Effect.runPromise(pod.give(turn({ "config.toml": 'model = "gpt-5-codex"\n' })));
    assert.deepEqual([readFileSync(join(home, "config.toml"), "utf8"), existsSync(join(home, "skills/review/SKILL.md"))], ['model = "gpt-5-codex"\n', false]);
    await Effect.runPromise(pod.give(turn(null, Date.now() - 1)));
    assert.equal((await viaPod()).status, 401);
    // Renewed as its turn runs: a fresh token in its place.
    await Effect.runPromise(pod.renew({ url: alasios.url, token: token({ key: "harness:telegram:41", expires: Date.now() + 60_000 }) }));
    assert.equal((await viaPod()).status, 200);
  } finally {
    await Effect.runPromise(Scope.close(listening, Exit.void));
    rmSync(home, { recursive: true, force: true });
  }
});
