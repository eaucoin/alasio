/**
 * Tar archives (cli/src/cluster/tar.ts): read as Docker and gVisor's release send them,
 * and written as Docker takes them, which tar itself extracts.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { tarArchive, type TarEntry, tarEntries } from "../src/cluster/tar.ts";

const directory = (name: string): TarEntry => ({ name, type: "directory", mode: 0o755, content: Buffer.alloc(0) });
const file = (name: string, content: string): TarEntry => ({ name, type: "file", mode: 0o644, content: Buffer.from(content) });

/** A name longer than a header's 100 bytes. */
const LONG = `var/lib/${"rancher/".repeat(12)}runsc.toml`;

test("tarEntries reads files and directories, a POSIX header's prefix as its name's start, and passes over other entries", () => {
  const archive = tarArchive([directory("gvisor-bin/"), { ...file("gvisor-bin/runsc-metric-server", "metrics"), mode: 0o755 }]);
  const linked = Buffer.from(archive);
  // A symbolic link, which is passed over.
  linked[512 + 156] = 0x32;
  assert.deepEqual(tarEntries(archive).map(({ name, type, content }) => [name, type, content.toString()]), [["gvisor-bin/", "directory", ""], ["gvisor-bin/runsc-metric-server", "file", "metrics"]]);
  assert.deepEqual(tarEntries(linked).map(({ name }) => name), ["gvisor-bin/"]);
  const prefixed = tarArchive([file("runsc", "runsc")]);
  prefixed.write("release", 345, "latin1");
  assert.equal(tarEntries(prefixed)[0]?.name, "release/runsc");
});

test("tarArchive writes what tarEntries reads back, a long name split into its prefix and the rest", () => {
  const entries = [directory("etc/"), file("etc/config.toml", "a = 1\n"), file("etc/empty", ""), file(LONG, "x".repeat(513))];
  assert.deepEqual(tarEntries(tarArchive(entries)), entries);
  assert.equal(tarArchive(entries).length, 512 * (1 + 2 + 1 + 3) + 1024);
  assert.throws(() => tarArchive([file("x".repeat(101), "")]), /is too long a name for a tar archive's header$/u);
});

test("tar extracts what tarArchive writes, with its modes", (t) => {
  const into = mkdtempSync(join(tmpdir(), "alasio-tar-"));
  t.after(() => rmSync(into, { recursive: true, force: true }));
  execFileSync("tar", ["--extract", "--file=-", `--directory=${into}`], { input: tarArchive([directory("etc/"), file("etc/config.toml", "a = 1\n"), { ...file(LONG, "long\n"), mode: 0o755 }]) });
  assert.equal(readFileSync(join(into, "etc/config.toml"), "utf8"), "a = 1\n");
  assert.equal(statSync(join(into, "etc/config.toml")).mode & 0o777, 0o644);
  assert.equal(readFileSync(join(into, LONG), "utf8"), "long\n");
  assert.equal(statSync(join(into, LONG)).mode & 0o777, 0o755);
});
