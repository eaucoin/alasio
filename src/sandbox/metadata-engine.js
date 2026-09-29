/**
 * The JuiceFS metadata engine, kept pluggable so the store it runs on is a
 * configuration choice, not baked into the volume manager. JuiceFS takes the engine as
 * a URL, so an engine here only has to build that URL for a volume and say how volumes
 * are separated on it.
 *
 * Valkey (the open Redis fork) is the chosen engine (session-fs-research E12): on a
 * single instance JuiceFS separates volumes by logical DB index (`redis://host/N`), so
 * a volume's namespace is a DB index, and the instance's `databases` setting caps how
 * many volumes it holds. A co-located Postgres is the documented fallback; adding it
 * here is where it would go.
 */

/** Parse the configured base URL into an engine. Throws on an unsupported scheme. */
export function createMetadataEngine({ url, databases }) {
  const scheme = String(url ?? "").split("://", 1)[0];
  if (scheme !== "redis" && scheme !== "rediss") {
    throw new Error(`unsupported session-filesystem metadata engine: ${JSON.stringify(scheme)} (only redis is implemented; see metadata-engine.js)`);
  }
  const base = url.replace(/\/+$/, "");
  const capacity = Number(databases);
  if (!Number.isInteger(capacity) || capacity < 2) {
    throw new Error(`redis metadata engine needs a databases count >= 2, got ${JSON.stringify(databases)}`);
  }
  return {
    kind: "redis",
    // DB 0 is left for the instance's own use; volumes take 1..capacity-1.
    namespaceCount: capacity - 1,
    firstNamespace: 1,
    /** The metadata URL for a volume in namespace `dbIndex`; the password is passed by env, never here. */
    metaUrl(dbIndex) {
      if (!Number.isInteger(dbIndex) || dbIndex < 1 || dbIndex >= capacity) {
        throw new Error(`redis db index out of range: ${dbIndex} (1..${capacity - 1})`);
      }
      return `${base}/${dbIndex}`;
    },
  };
}
