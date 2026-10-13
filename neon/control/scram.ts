/**
 * A Postgres SCRAM-SHA-256 password verifier (RFC 5802, 7677), in the form
 * Postgres stores and compute_ctl passes through as a role's
 * encrypted_password: the password itself never reaches the compute's spec. Each is
 * salted at random, as Postgres salts one, and made once: whoever sets a role's
 * password (a compute's spec, alasio) and whoever answers for it (neon-control, to
 * Neon's proxy, which authenticates a client with the keys of the verifier it is
 * answered) are given the one verifier, which the compute must store to the byte.
 */
import { createHash, createHmac, pbkdf2Sync, randomBytes } from "node:crypto";

const ITERATIONS = 4096;

/**
 * A verifier as Postgres stores one: its iterations, its salt, and the 32-byte StoredKey
 * and ServerKey, each in base64. Nothing else, so one checked to be one may be written
 * into a statement that sets a role's password.
 */
const VERIFIER = /^SCRAM-SHA-256\$[1-9][0-9]*:[A-Za-z0-9+/]+={0,2}\$[A-Za-z0-9+/]{43}=:[A-Za-z0-9+/]{43}=$/u;

/** Whether `text` is a SCRAM-SHA-256 verifier, in the form scramVerifier makes and Postgres stores. */
export const isScramVerifier = (text: string): boolean => VERIFIER.test(text);

export function scramVerifier(password: string, salt: Buffer = randomBytes(16)): string {
  const salted = pbkdf2Sync(Buffer.from(password, "utf8"), salt, ITERATIONS, 32, "sha256");
  const clientKey = createHmac("sha256", salted).update("Client Key").digest();
  const storedKey = createHash("sha256").update(clientKey).digest();
  const serverKey = createHmac("sha256", salted).update("Server Key").digest();
  return `SCRAM-SHA-256$${ITERATIONS}:${salt.toString("base64")}$${storedKey.toString("base64")}:${serverKey.toString("base64")}`;
}
