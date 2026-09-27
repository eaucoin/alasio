/**
 * A Postgres SCRAM-SHA-256 password verifier (RFC 5802, 7677), in the form
 * Postgres stores and compute_ctl passes through as a role's
 * encrypted_password: the password itself never reaches the compute's spec.
 */
import { createHash, createHmac, pbkdf2Sync, randomBytes } from "node:crypto";

const ITERATIONS = 4096;

export function scramVerifier(password, salt = randomBytes(16)) {
  const salted = pbkdf2Sync(Buffer.from(password, "utf8"), salt, ITERATIONS, 32, "sha256");
  const clientKey = createHmac("sha256", salted).update("Client Key").digest();
  const storedKey = createHash("sha256").update(clientKey).digest();
  const serverKey = createHmac("sha256", salted).update("Server Key").digest();
  return `SCRAM-SHA-256$${ITERATIONS}:${salt.toString("base64")}$${storedKey.toString("base64")}:${serverKey.toString("base64")}`;
}
