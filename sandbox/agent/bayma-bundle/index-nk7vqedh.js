#!/usr/bin/env -S node --disable-warning=ExperimentalWarning
import {
  payloadValue2,
  assertDoctorExecResult2,
  JSON_CHECKPOINT_CODEC2,
  createExecFileWorkspace2,
  ProcessTransport2,
  buildMarkers2,
  createPromptAwareEnvelopeCollector2
} from "./main-52rkp48f.js";

// packages/runtime-lean/src/exec.ts
import { writeFileSync } from "node:fs";

// packages/runtime-lean/src/transport.ts
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
var LEAN_PROMPT = "BAYMA> ";
var LEAN_CHECKPOINT_CODEC = {
  codecId: "lean-environment-v1",
  codecVersion: 1,
  payloadKind: "binary-sidecar"
};
var LEAN_STARTUP_TIMEOUT_MS = 60000;
function createLeanTransport() {
  return new ProcessTransport2({
    platformId: "stdio",
    promptRe: /(?:^|[\r\n])BAYMA> /g,
    interruptStrategy: "recycle",
    startupTimeoutMs: LEAN_STARTUP_TIMEOUT_MS,
    snapshots: false,
    command: ({ cwd, scratchDir }) => leanHostCommand(cwd, scratchDir)
  });
}
function leanHostCommand(cwd, scratch) {
  const host = payloadValue2("BAYMA_LEAN_HOST_BIN");
  const env = { BAYMA_LEAN_SCRATCH_DIR: scratch };
  return isLakeProject(cwd) ? { file: payloadValue2("BAYMA_LAKE_BIN"), args: ["env", host], env } : { file: host, args: [], env };
}
function isLakeProject(cwd) {
  if (!["lakefile.toml", "lakefile.lean"].some((f) => existsSync(join(cwd, f))))
    return false;
  const running = payloadValue2("BAYMA_LEAN_VERSION");
  const toolchainFile = join(cwd, "lean-toolchain");
  const named = existsSync(toolchainFile) ? readFileSync(toolchainFile, "utf8").trim() : undefined;
  if (named?.replace(/^leanprover\/lean4:/, "").replace(/^v/, "") !== running) {
    throw new Error(`the Lake project in ${cwd} names ${named ?? "no Lean"} in its lean-toolchain; this runtime runs Lean ${running}`);
  }
  return true;
}

// packages/runtime-lean/src/exec.ts
function restorePath(input) {
  if (!input.checkpoint)
    return null;
  switch (input.checkpoint.manifest.payloadKind) {
    case "binary-sidecar":
      return input.checkpoint.payloadAbsolutePath ?? null;
    case "json-inline":
      return null;
    default:
      throw new Error(`Lean does not support checkpoint payload kind ${input.checkpoint.manifest.payloadKind}`);
  }
}
function writeLeanExecFile(input) {
  const markers = buildMarkers2();
  const workspace = createExecFileWorkspace2(input.rootDir, input.sessionId, input.execId);
  try {
    const specPath = workspace.file("submission.json");
    const spec = {
      schema_version: 1,
      event_prefix: markers.eventPrefix,
      code: input.code,
      durability_mode: input.durabilityMode,
      restore_path: restorePath(input),
      checkpoint_output_path: workspace.file("checkpoint.bin")
    };
    writeFileSync(specPath, JSON.stringify(spec), "utf8");
    return {
      submitText: `:exec ${specPath}
`,
      collector: createPromptAwareEnvelopeCollector2(markers, [LEAN_PROMPT]),
      dispose: workspace.dispose
    };
  } catch (error) {
    workspace.dispose();
    throw error;
  }
}
var leanDoctor = {
  probeCode: "#eval 40 + 2",
  successMessage: "ok: Bayma executed Lean in the Lean runtime",
  assertSuccess: (exec) => assertDoctorExecResult2(exec, "42")
};

// packages/runtime-lean/src/profile.ts
var leanModelProfile = {
  runtimeId: "lean",
  heading: "Lean",
  description: "Runs Lean 4 commands. Each cell runs on the environment the previous cell left, so its definitions, theorems, instances, and attributes stay in scope; imports belong in a session's first cell. A cell's last info message, such as a final #eval's value or a #check, is its result; its earlier ones, such as what an #eval printed, are its stdout. Warnings go to stderr and errors to the exec's error, each with its line:column; a command that fails leaves the commands after it to run, and the exec ends in error. A Lake project in the session working directory supplies its libraries, such as Mathlib, once built with the lake on PATH, whose Lean its lean-toolchain must name. An interrupt restarts the session. Checkpointed recovery restores everything the session declared, with its open namespaces, options, and variables. #eval Bayma.displayImage bytes or Bayma.displayImageFile path shows the model a PNG, JPEG, GIF, or WebP image when the cell ends."
};

// packages/runtime-lean/src/index.ts
var leanAdapter = {
  runtimeId: "lean",
  displayName: "Lean",
  checkpointCodecs: [JSON_CHECKPOINT_CODEC2, LEAN_CHECKPOINT_CODEC],
  modelProfile: leanModelProfile,
  createTransport: createLeanTransport,
  prepareExec: writeLeanExecFile,
  doctor: leanDoctor
};
export {
  leanAdapter
};
