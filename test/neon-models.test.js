import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { codeRevision } from "../neon/control/setup.js";
import { ensurePgragModels, tarMember } from "../src/neon/models.js";

/** A tar archive of `members`, `{ name, data, type, prefix }`, as ustar lays one out. */
function tarOf(members) {
  const blocks = [];
  for (const { name, data = "", type = "0", prefix = "" } of members) {
    const body = Buffer.from(data);
    const header = Buffer.alloc(512);
    header.write(name, 0, 100, "latin1");
    header.write(body.length.toString(8).padStart(11, "0"), 124, 12, "latin1");
    header.write(type, 156, 1, "latin1");
    header.write("ustar", 257, 6, "latin1");
    header.write(prefix, 345, 155, "latin1");
    blocks.push(header, body, Buffer.alloc((512 - (body.length % 512)) % 512));
  }
  blocks.push(Buffer.alloc(1024));
  return Buffer.concat(blocks);
}

test("reads a file out of a tar archive by its full name, past headers and other members", () => {
  const tar = tarOf([
    { name: "pax_global_header", type: "g", data: "52 comment=0c1d\n" },
    { name: "pgrag-0.1.2/", type: "5" },
    { name: "README.md", prefix: "pgrag-0.1.2", data: "x".repeat(700) },
    { name: "model.onnx.tar.gz", prefix: "pgrag-0.1.2/lib/bge_small_en_v15", data: "the model" },
  ]);
  assert.equal(tarMember(tar, "pgrag-0.1.2/lib/bge_small_en_v15/model.onnx.tar.gz").toString(), "the model");
  assert.equal(tarMember(tar, "pgrag-0.1.2/README.md").length, 700);
  assert.throws(() => tarMember(tar, "pgrag-0.1.2/lib/missing"), /not in the archive/u);
});

test("refuses a pgrag release that is not the one Neon's compute is built from, and writes nothing", async () => {
  const dir = mkdtempSync(join(tmpdir(), "alasio-models-"));
  try {
    await assert.rejects(ensurePgragModels(dir, { fetchRelease: async () => Buffer.from("not pgrag") }), /not the one Neon's compute is built from/u);
    assert.deepEqual(readdirSync(dir), []);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("the stack's code revision changes with its code, and only with it", () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-revision-"));
  try {
    const control = join(root, "control");
    const models = join(root, "models");
    mkdirSync(control);
    mkdirSync(models);
    writeFileSync(join(control, "service.js"), "a");
    writeFileSync(join(models, "serve.js"), "b");
    const before = codeRevision([control, models]);
    writeFileSync(join(control, "notes.txt"), "not code");
    assert.equal(codeRevision([control, models]), before);
    writeFileSync(join(models, "serve.js"), "b, changed");
    assert.notEqual(codeRevision([control, models]), before);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
