/**
 * Files a sandbox wrote on its volumes, read by the agent where the node mounts them, as
 * untrusted: the agent runs as the node's root, and what the sandbox can write, a symbolic
 * link, a FIFO, a device node or a file of any size among it, must lead the agent nowhere
 * outside the volume, hang it, open a device of the node's, or fill its memory.
 */
import { constants } from "node:fs";
import { type FileHandle, open, readFile } from "node:fs/promises";
import { join, posix } from "node:path";

import { Schema } from "effect";

/** How much of a file is read at a time. */
const READ_BYTES = 1024 * 1024;

/** What the agent reads of a container's OCI spec: its mounts, and where each mounts what. */
const Spec = Schema.fromJsonString(Schema.Struct({
  mounts: Schema.optional(Schema.Array(Schema.Struct({ destination: Schema.String, source: Schema.optional(Schema.String) }))),
}));

/** A path on the node: a directory the agent trusts, and the names under it, which it does not. */
export interface UnderRoot {
  readonly root: string;
  readonly names: readonly string[];
}

/**
 * Where the container's path `path` is on the node, as the container's spec in its bundle
 * (`bundle`, on the node) mounts it: under the source of the mount whose destination holds
 * it, the longest of them, by the names of `path` past that; null where no mount holds it.
 * `nodeRoot` is where the agent sees the node's filesystem.
 */
export async function hostPathOf(nodeRoot: string, bundle: string, path: string): Promise<UnderRoot | null> {
  const { mounts = [] } = Schema.decodeUnknownSync(Spec)(await readFile(join(nodeRoot, bundle, "config.json"), "utf8"));
  const names = (of: string) => posix.normalize(of).split("/").filter(Boolean);
  const wanted = names(path);
  const [held] = mounts
    .filter(({ destination, source }) => source !== undefined && names(destination).every((name, index) => wanted[index] === name))
    .sort((a, b) => names(b.destination).length - names(a.destination).length);
  if (held?.source === undefined) return null;
  return { root: join(nodeRoot, held.source), names: wanted.slice(names(held.destination).length) };
}

/**
 * Linux's O_PATH, which node:fs does not name: a descriptor of a file that opens nothing of
 * it, but tells what it is (fstat(2)), and is opened through /proc/self/fd as that file.
 */
const O_PATH = 0o10000000;

/**
 * The file `names` under the directory `root`, read whole if it is a regular file of at
 * most `limit` bytes. Each name is opened relative to the directory before it, through
 * /proc/self/fd, as openat(2) opens one, and none may be a symbolic link. The file itself
 * is opened first as a path (O_PATH), which opens nothing of it, and opened to be read only
 * once that says it is a regular file, through that descriptor, so as the very file looked
 * at: a device, whose opening may do what the device does, or a FIFO, whose opening waits
 * for a writer, is never opened. Fails, saying why, otherwise.
 */
export async function readUnder({ root, names }: UnderRoot, limit: number): Promise<Buffer> {
  const strange = names.find((name) => name === "" || name === "." || name === ".." || name.includes("/") || name.includes("\0"));
  if (strange !== undefined || names.length === 0) throw new Error(`${JSON.stringify(names)} names no file under ${root}`);
  const opened: FileHandle[] = [];
  /** Opens the `index`th of `names` in `directory`, saying, where it fails, which it was, by its path under `root`. */
  const openAt = async (directory: FileHandle, index: number, flags: number) => {
    const handle = await open(`/proc/self/fd/${directory.fd}/${names[index]}`, flags | constants.O_NOFOLLOW).catch((error: unknown) => {
      const code = (error as NodeJS.ErrnoException).code;
      const why = code === "ELOOP" || code === "ENOTDIR"
        ? "it is a symbolic link, which is not followed, or no directory"
        : code === "ENOENT"
        ? "there is none"
        : error instanceof Error
        ? error.message
        : String(error);
      throw new Error(`${join(root, ...names.slice(0, index + 1))} cannot be opened: ${why}`);
    });
    opened.push(handle);
    return handle;
  };
  try {
    let directory = await open(root, constants.O_RDONLY | constants.O_DIRECTORY);
    opened.push(directory);
    for (let index = 0; index < names.length - 1; index += 1) directory = await openAt(directory, index, constants.O_RDONLY | constants.O_DIRECTORY);
    // O_NOFOLLOW with O_PATH opens a symbolic link itself, rather than failing.
    const path = await openAt(directory, names.length - 1, O_PATH);
    const found = await path.stat();
    if (found.isSymbolicLink()) throw new Error(`${join(root, ...names)} cannot be opened: it is a symbolic link, which is not followed, or no directory`);
    if (!found.isFile()) throw new Error(`${join(root, ...names)} is not a regular file`);
    if (found.size > limit) throw new Error(`${join(root, ...names)} is ${found.size} bytes, more than the ${limit} read of it`);
    const file = await open(`/proc/self/fd/${path.fd}`, constants.O_RDONLY);
    opened.push(file);
    // To its end, which may be past what it was said to be: it may have grown since.
    const chunks: Buffer[] = [];
    let read = 0;
    for (;;) {
      const { bytesRead, buffer } = await file.read(Buffer.alloc(READ_BYTES), 0, READ_BYTES, read);
      if (bytesRead === 0) return Buffer.concat(chunks);
      read += bytesRead;
      if (read > limit) throw new Error(`${join(root, ...names)} holds more than the ${limit} bytes read of it`);
      chunks.push(buffer.subarray(0, bytesRead));
    }
  } finally {
    for (const handle of opened.reverse()) await handle.close();
  }
}
