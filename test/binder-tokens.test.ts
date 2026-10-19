import assert from "node:assert/strict";
import { createHmac, randomBytes } from "node:crypto";
import { test } from "node:test";

import { environmentKey } from "../src/binder/names.ts";
import { mintToken, type TokenClaims, verifyToken } from "../src/binder/tokens.ts";

const secret = randomBytes(32);

test("a token says what it was minted for, checked by the key that signed it", () => {
  const caller = mintToken(secret, { key: "sessionfs:workspace", expires: 2_000 });
  assert.deepEqual(verifyToken(secret, caller, 1_000), { key: "sessionfs:workspace", expires: 2_000 });
  const binding = mintToken(secret, { key: "sessionfs:workspace", binding: "lease-uid" });
  assert.deepEqual(verifyToken(secret, binding, 1_000), { key: "sessionfs:workspace", binding: "lease-uid" });
});

test("a token another key signed, one changed, one stopped, and one that is no token are refused", () => {
  const token = mintToken(secret, { key: "sessionfs:workspace", expires: 2_000 });
  assert.equal(verifyToken(randomBytes(32), token, 1_000), null);
  const [claims, signature] = token.split(".");
  const other = Buffer.from(JSON.stringify({ key: "sessionfs:other", expires: 2_000 })).toString("base64url");
  assert.equal(verifyToken(secret, `${other}.${signature}`, 1_000), null);
  assert.equal(verifyToken(secret, `${claims}.${signature?.slice(1)}`, 1_000), null);
  assert.equal(verifyToken(secret, token, 2_000), null);
  for (const nothing of ["", "no-dot", ".", "a.b"]) assert.equal(verifyToken(secret, nothing, 1_000), null);
  // Signed, but not claims.
  const notClaims = Buffer.from("[]").toString("base64url");
  assert.equal(verifyToken(secret, `${notClaims}.${createHmac("sha256", secret).update(notClaims).digest("base64url")}`, 1_000), null);
});

test("an environment's callers sign with a key of their own, of the master, and a token is checked by the key of the environment it names alone", () => {
  const master = randomBytes(32);
  const keyOf = ({ options }: TokenClaims) => (options?.["environment"] ? environmentKey(master, options["environment"]) : null);
  const of = (environment: string, signer = environmentKey(master, environment)) =>
    mintToken(signer, { key: "sessionfs:workspace", expires: 2_000, options: { environment, mode: "none" } });
  assert.deepEqual(verifyToken(keyOf, of("main"), 1_000)?.options, { environment: "main", mode: "none" });
  assert.deepEqual(verifyToken(keyOf, of("try"), 1_000)?.options, { environment: "try", mode: "none" });
  // A branch's key names main's sessions in vain, as the master itself, and a token naming none.
  assert.equal(verifyToken(keyOf, of("main", environmentKey(master, "try")), 1_000), null);
  assert.equal(verifyToken(keyOf, of("main", master), 1_000), null);
  assert.equal(verifyToken(keyOf, mintToken(master, { key: "sessionfs:workspace", expires: 2_000 }), 1_000), null);
  assert.notDeepEqual(environmentKey(master, "main"), environmentKey(master, "try"));
});
