// @ts-nocheck
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, test } from "node:test";

import { loginUpstream, startLoginRelay } from "../src/codex/login-relay.ts";

const dir = mkdtempSync(join(tmpdir(), "alasio-login-relay-"));
const authFile = join(dir, "auth.json");
const writeAuth = (auth) => writeFileSync(authFile, JSON.stringify(auth));

// A stand-in upstream that records what each request arrived with.
let upstream;
let upstreamOrigin;
const seen = [];
before(async () => {
  upstream = createServer((req, res) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      seen.push({ path: req.url, auth: req.headers.authorization ?? null, accountId: req.headers["chatgpt-account-id"] ?? null, body: Buffer.concat(chunks).toString() });
      res.writeHead(200, { "content-type": "application/json" }).end('{"ok":true}');
    });
  });
  await new Promise((resolve) => upstream.listen(0, "127.0.0.1", resolve));
  upstreamOrigin = `http://127.0.0.1:${upstream.address().port}`;
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
  const upstreamFor = (file, path) => {
    const real = loginUpstream(file, path);
    return real && { ...real, origin: upstreamOrigin };
  };
  const relay = await startLoginRelay({ authFile, bearer: "relay-bearer", upstreamFor });
  const call = (path, bearer, body = "{}") =>
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
    assert.equal(seen.at(-1).auth, "Bearer token-2");

    const before = seen.length;
    assert.equal((await call("/v1/responses", "guessed")).status, 401);
    assert.equal((await call("/v1/responses")).status, 401);
    assert.equal((await call("/v1/files", "relay-bearer")).status, 403);
    assert.equal((await call("/backend-api/codex/responses", "relay-bearer")).status, 403);
    assert.equal(seen.length, before); // none of those reached the upstream
    writeAuth({});
    assert.equal((await call("/v1/responses", "relay-bearer")).status, 503);
  } finally {
    await relay.close();
  }
});
