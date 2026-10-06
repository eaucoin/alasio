/** The entries of a tar archive, as Docker's archive endpoint and gVisor's release send them. */

/** An entry of a tar archive: a regular file, with its content, or a directory. */
export interface TarEntry {
  readonly name: string;
  readonly type: "file" | "directory";
  readonly mode: number;
  readonly content: Buffer;
}

const BLOCK = 512;

/** A NUL-terminated field of a header, as text. */
const field = (header: Buffer, start: number, end: number): string => header.toString("latin1", start, end).replace(/\0.*$/su, "");

/**
 * The regular files and directories of `tar`, in order; entries of other types (links,
 * extended headers) are passed over. A POSIX header's prefix is its name's start.
 */
export function tarEntries(tar: Buffer): TarEntry[] {
  const entries: TarEntry[] = [];
  for (let offset = 0; offset + BLOCK <= tar.length;) {
    const header = tar.subarray(offset, offset + BLOCK);
    // Two zero blocks end an archive.
    if (header.every((byte) => byte === 0)) break;
    const size = Number.parseInt(field(header, 124, 136).trim() || "0", 8);
    const type = header[156];
    // POSIX's magic, "ustar" and NUL; GNU's, whose header has no prefix there, ends with a space.
    const prefix = header.toString("latin1", 257, 263) === "ustar\u0000" ? field(header, 345, 500) : "";
    const name = prefix ? `${prefix}/${field(header, 0, 100)}` : field(header, 0, 100);
    const mode = Number.parseInt(field(header, 100, 108).trim() || "0", 8);
    const start = offset + BLOCK;
    // "0", or NUL in archives older than POSIX's.
    if (type === 0x30 || type === 0) entries.push({ name, type: "file", mode, content: tar.subarray(start, start + size) });
    if (type === 0x35) entries.push({ name, type: "directory", mode, content: Buffer.alloc(0) });
    offset = start + Math.ceil(size / BLOCK) * BLOCK;
  }
  return entries;
}
