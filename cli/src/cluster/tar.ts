/** Tar archives: read, as Docker's archive endpoint and gVisor's release send them, and written, as Docker's archive endpoint takes them. */

/** An entry of a tar archive: a regular file, with its content, or a directory. */
export interface TarEntry {
  readonly name: string;
  readonly type: "file" | "directory";
  readonly mode: number;
  readonly content: Buffer;
}

const BLOCK = 512;
/** The most a header's name, and its prefix, hold. */
const NAME_SIZE = 100;
const PREFIX_SIZE = 155;

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

/** `value` in octal, as a header's numeric field of `width` bytes holds it: zero-padded, then NUL. */
const octal = (value: number, width: number): string => `${value.toString(8).padStart(width - 1, "0")}\0`;

/**
 * `name` as a POSIX header holds it: its prefix, and the rest, split at a slash when it is
 * longer than a header's name; a directory's own trailing slash stays with the rest.
 */
function splitName(name: string): { readonly prefix: string; readonly rest: string } {
  if (name.length <= NAME_SIZE) return { prefix: "", rest: name };
  const at = name.slice(0, -1).indexOf("/", name.length - NAME_SIZE - 1);
  if (at <= 0 || at > PREFIX_SIZE) throw new Error(`${name} is too long a name for a tar archive's header`);
  return { prefix: name.slice(0, at), rest: name.slice(at + 1) };
}

/** The POSIX header of `entry`, owned by root, from the epoch. */
function header({ name, type, mode, content }: TarEntry): Buffer {
  const block = Buffer.alloc(BLOCK);
  const { prefix, rest } = splitName(name);
  block.write(rest, 0, NAME_SIZE, "latin1");
  block.write(octal(mode, 8), 100, "latin1");
  block.write(octal(0, 8), 108, "latin1");
  block.write(octal(0, 8), 116, "latin1");
  block.write(octal(content.length, 12), 124, "latin1");
  block.write(octal(0, 12), 136, "latin1");
  block.write(type === "file" ? "0" : "5", 156, "latin1");
  block.write("ustar\u000000", 257, "latin1");
  block.write(prefix, 345, PREFIX_SIZE, "latin1");
  // The checksum is the sum of the header's bytes with its own field as spaces, its last one kept.
  block.write(" ".repeat(8), 148, "latin1");
  block.write(octal(block.reduce((sum, byte) => sum + byte, 0), 7), 148, "latin1");
  return block;
}

/** The POSIX tar archive of `entries`, in order: each one's header, then its content, padded to a block. */
export function tarArchive(entries: readonly TarEntry[]): Buffer {
  return Buffer.concat([
    ...entries.flatMap((entry) => [header(entry), entry.content, Buffer.alloc((BLOCK - (entry.content.length % BLOCK)) % BLOCK)]),
    Buffer.alloc(2 * BLOCK),
  ]);
}
