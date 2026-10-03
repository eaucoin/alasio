// @ts-nocheck
/**
 * The tokens Neon's services authenticate each other with: EdDSA JWTs with a
 * scope, and a tenant for tenant-scoped ones, with no expiry, as Neon issues
 * them (libs/utils/src/auth.rs).
 */
import { createPrivateKey, createPublicKey, generateKeyPairSync, sign, verify } from "node:crypto";

const HEADER = base64url(JSON.stringify({ alg: "EdDSA", typ: "JWT" }));

function base64url(value) {
  return Buffer.from(value).toString("base64url");
}

/** A new Ed25519 keypair, as PEM. */
export function generateKeyPair() {
  const { privateKey, publicKey } = generateKeyPairSync("ed25519");
  return {
    privateKeyPem: privateKey.export({ type: "pkcs8", format: "pem" }),
    publicKeyPem: publicKey.export({ type: "spki", format: "pem" }),
  };
}

/**
 * A token for `scope` (tenant, pageserverapi, safekeeperdata,
 * generations_api, admin, infra, ...), limited to `tenantId` if given.
 */
export function signToken(privateKeyPem, scope, tenantId = null) {
  const claims = tenantId ? { scope, tenant_id: tenantId } : { scope };
  const unsigned = `${HEADER}.${base64url(JSON.stringify(claims))}`;
  const signature = sign(null, Buffer.from(unsigned), createPrivateKey(privateKeyPem));
  return `${unsigned}.${signature.toString("base64url")}`;
}

/** The claims of `token` if `publicKeyPem` signed it, else null. */
export function verifyToken(publicKeyPem, token) {
  const parts = String(token).split(".");
  if (parts.length !== 3 || parts[0] !== HEADER) return null;
  const valid = verify(
    null,
    Buffer.from(`${parts[0]}.${parts[1]}`),
    createPublicKey(publicKeyPem),
    Buffer.from(parts[2], "base64url"),
  );
  if (!valid) return null;
  try {
    return JSON.parse(Buffer.from(parts[1], "base64url").toString("utf8"));
  } catch {
    return null;
  }
}

/** The public key as a JWK set, which compute_ctl verifies its API's tokens with. */
export function publicJwks(publicKeyPem) {
  const jwk = createPublicKey(publicKeyPem).export({ format: "jwk" });
  return { keys: [{ ...jwk, use: "sig", key_ops: ["verify"], alg: "EdDSA", kid: "alasio-neon" }] };
}
