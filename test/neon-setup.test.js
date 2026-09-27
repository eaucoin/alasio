import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { controlRevision } from "../neon/control/setup.js";

test("neon-control's revision changes with its code, and only with it", () => {
  const dir = mkdtempSync(join(tmpdir(), "alasio-control-"));
  try {
    writeFileSync(join(dir, "service.js"), "a");
    const before = controlRevision(dir);
    writeFileSync(join(dir, "notes.txt"), "not code");
    assert.equal(controlRevision(dir), before);
    writeFileSync(join(dir, "service.js"), "a, changed");
    assert.notEqual(controlRevision(dir), before);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
