#!/usr/bin/env -S node --disable-warning=ExperimentalWarning
import {
  payloadValue2,
  assertDoctorExecResult2,
  JSON_CHECKPOINT_CODEC2,
  createExecFileWorkspace2,
  cacheRoot2,
  ProcessTransport2,
  buildMarkers2,
  createPromptAwareEnvelopeCollector2
} from "./main-4vabrjre.js";

// packages/runtime-go/src/exec.ts
import { writeFileSync } from "node:fs";

// packages/runtime-go/src/transport.ts
import { readdirSync } from "node:fs";
import { dirname, join } from "node:path";
var GO_PROMPT = "BAYMA> ";
var GO_STARTUP_TIMEOUT_MS = 120000;
function createGoTransport() {
  return new ProcessTransport2({
    platformId: "stdio",
    promptRe: /(?:^|[\r\n])BAYMA> /g,
    interruptStrategy: "recycle",
    startupTimeoutMs: GO_STARTUP_TIMEOUT_MS,
    command: ({ scratchDir }) => goHostCommand(scratchDir),
    endedHostFiles: goHostModules
  });
}
function goHostModules(scratch, pid) {
  const modules = join(scratch, "go");
  let names;
  try {
    names = readdirSync(modules);
  } catch (error) {
    if (error.code === "ENOENT")
      return [];
    throw error;
  }
  return names.filter((name) => name.startsWith(`${pid}-`)).map((name) => join(modules, name));
}
function goHostCommand(scratch) {
  const root = join(cacheRoot2(), "go");
  return {
    file: payloadValue2("BAYMA_GO_HOST_BIN"),
    args: [],
    env: {
      BAYMA_GO_SCRATCH_DIR: join(scratch, "go"),
      GOROOT: dirname(dirname(payloadValue2("BAYMA_GO_BIN"))),
      GOPATH: join(root, "path"),
      GOCACHE: join(root, "build"),
      GOENV: "off",
      GOTOOLCHAIN: "local",
      GOFLAGS: "-trimpath -modcacherw",
      CGO_ENABLED: "1",
      CC: payloadValue2("BAYMA_GO_CC"),
      ZIG_GLOBAL_CACHE_DIR: join(root, "zig"),
      ZIG_LOCAL_CACHE_DIR: join(root, "zig")
    }
  };
}

// packages/runtime-go/src/exec.ts
function checkpointJson(input) {
  if (!input.checkpoint)
    return null;
  if (input.checkpoint.manifest.payloadKind !== "json-inline") {
    throw new Error(`Go does not support checkpoint payload kind ${input.checkpoint.manifest.payloadKind}`);
  }
  return JSON.stringify(input.checkpoint.manifest.inlineJson ?? null);
}
function writeGoExecFile(input) {
  const markers = buildMarkers2();
  const workspace = createExecFileWorkspace2(input.rootDir, input.sessionId, input.execId);
  try {
    const specPath = workspace.file("submission.json");
    const spec = {
      schema_version: 1,
      event_prefix: markers.eventPrefix,
      code: input.code,
      durability_mode: input.durabilityMode,
      checkpoint_json: checkpointJson(input)
    };
    writeFileSync(specPath, JSON.stringify(spec), "utf8");
    return {
      submitText: `:exec ${specPath}
`,
      collector: createPromptAwareEnvelopeCollector2(markers, [GO_PROMPT]),
      dispose: workspace.dispose
    };
  } catch (error) {
    workspace.dispose();
    throw error;
  }
}
var goDoctor = {
  probeCode: "40 + 2",
  successMessage: "ok: Bayma executed Go in the Go runtime",
  assertSuccess: (exec) => assertDoctorExecResult2(exec, "42")
};

// packages/runtime-go/src/profile.ts
var goModelProfile = {
  runtimeId: "go",
  heading: "Go",
  description: "Runs Go as one package that grows a cell at a time, in a process that stays alive: each cell may hold imports, then declarations and statements in any order; its statements run once, and everything it declares, unexported names, fields, and methods included, stays live for later cells, while a name declared again replaces the old one. A cell ending in an expression returns its value. Any package may be imported, the session working directory's module among them; modules come through the Go module proxy, and a module's version is fixed once the session has loaded it. The first session on a machine compiles Go's standard library for its cells, once. Output written while no cell runs is dropped. An interrupt restarts the session. A panic in a goroutine a cell started ends the session's process and quarantines the session, so recover in the goroutines a cell starts. Checkpointed recovery preserves explicit bayma_write_checkpoint(value) state as JSON, read back with bayma_read_checkpoint(&value). bayma_display_image(bytes) or bayma_display_image_file(path) shows the model a PNG, JPEG, GIF, or WebP image when the cell ends. The process keeps every cell's plugin loaded, a few MB each, for as long as it runs: once a cell has ended, one that has come to hold more memory of its own than the server's runtime memory budget past what it held as it started, 2 GiB unless the server is told otherwise, is started again from the session's checkpoint, and the next cell is told so."
};

// packages/runtime-go/src/index.ts
var goAdapter = {
  runtimeId: "go",
  displayName: "Go",
  checkpointCodecs: [JSON_CHECKPOINT_CODEC2],
  modelProfile: goModelProfile,
  createTransport: createGoTransport,
  prepareExec: writeGoExecFile,
  doctor: goDoctor,
  keepsOfEachCell: "each cell's plugin loaded, which Go never unloads"
};
export {
  goAdapter
};
