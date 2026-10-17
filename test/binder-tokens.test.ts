import assert from "node:assert/strict";
import { createHmac, randomBytes } from "node:crypto";
import { test } from "node:test";

import { mintToken, verifyToken } from "../src/binder/tokens.ts";

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
