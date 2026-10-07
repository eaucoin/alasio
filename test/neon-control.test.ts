import assert from "node:assert/strict";
import { test } from "node:test";

import { bearsScope, generateKeyPair, signToken } from "../neon/control/jwt.ts";

test("neon-control takes a token of the admin scope only, not the tenant's every compute holds", () => {
  const { privateKeyPem, publicKeyPem } = generateKeyPair();
  const bearer = (token: string) => `Bearer ${token}`;
  assert.ok(bearsScope(publicKeyPem, bearer(signToken(privateKeyPem, "admin")), "admin"));
  assert.ok(!bearsScope(publicKeyPem, bearer(signToken(privateKeyPem, "tenant", "t")), "admin"));
  assert.ok(!bearsScope(publicKeyPem, bearer(signToken(privateKeyPem, "pageserverapi")), "admin"));
  assert.ok(!bearsScope(publicKeyPem, bearer(signToken(generateKeyPair().privateKeyPem, "admin")), "admin"));
  assert.ok(!bearsScope(publicKeyPem, signToken(privateKeyPem, "admin"), "admin"));
  assert.ok(!bearsScope(publicKeyPem, undefined, "admin"));
});
