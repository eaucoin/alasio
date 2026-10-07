/**
 * The DuckDB extensions the lake loads, which its image installs at build time, each
 * with the build it pins. DuckDB's repository serves only the newest build of an
 * extension for a DuckDB version, so a build cannot be installed by its pin: the image
 * build checks what it installed against these (./install-extensions.ts) and fails on
 * any other, so a new build upstream changes nothing until it is pinned here. They are
 * installed in this order: once httpfs is loaded, DuckDB downloads through it, which
 * the image has no CA certificates for.
 */
export const EXTENSIONS: Readonly<Record<string, string>> = {
  ducklake: "ac7595b0",
  postgres_scanner: "318dabb",
  httpfs: "4bc690d",
};
