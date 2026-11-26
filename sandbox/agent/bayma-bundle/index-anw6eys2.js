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
} from "./main-52rkp48f.js";

// packages/runtime-rust/src/exec.ts
import { readFileSync as readFileSync2, writeFileSync } from "node:fs";

// packages/runtime-rust/src/transport.ts
import {
  copyFileSync,
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  renameSync,
  rmSync
} from "node:fs";
import { join } from "node:path";
var RUST_PROMPT = "BAYMA> ";
var RUST_CHECKPOINT_CODEC = {
  codecId: "rust-serde-json-v1",
  codecVersion: 1,
  payloadKind: "text-sidecar"
};
var RUST_STARTUP_TIMEOUT_MS = 60000;
function createRustTransport() {
  const root = join(cacheRoot2(), "rust");
  return new ProcessTransport2({
    platformId: "stdio",
    promptRe: /(?:^|[\r\n])BAYMA> /g,
    interruptStrategy: "recycle",
    startupTimeoutMs: RUST_STARTUP_TIMEOUT_MS,
    command: () => {
      const version = process.env.BAYMA_RUST_VERSION ?? "development";
      const cacheDir = join(root, `evcxr-${version}`);
      const configDir = join(root, "config");
      const cargoHome = join(root, `cargo-home-${version}`);
      mkdirSync(cacheDir, { recursive: true });
      mkdirSync(configDir, { recursive: true });
      seedCargoHome(cargoHome, process.env.BAYMA_RUST_CARGO_SEED_DIR);
      return {
        file: payloadValue2("BAYMA_RUST_HOST_BIN"),
        args: [],
        env: {
          EVCXR_CONFIG_DIR: configDir,
          CARGO_HOME: cargoHome,
          BAYMA_RUST_CACHE_DIR: cacheDir,
          BAYMA_RUST_OWNS_PROCESS_TREE: "1"
        }
      };
    }
  });
}
var CARGO_SEED_ID = "bayma-seed-id";
function seedCargoHome(cargoHome, seed) {
  if (existsSync(cargoHome)) {
    if (seed && readSeedId(seed) !== readSeedId(cargoHome)) {
      mergeMissing(seed, cargoHome);
      copyFileSync(join(seed, CARGO_SEED_ID), join(cargoHome, CARGO_SEED_ID));
    }
    return;
  }
  mkdirSync(join(cargoHome, ".."), { recursive: true });
  const staging = mkdtempSync(`${cargoHome}.`);
  try {
    if (seed)
      cpSync(seed, staging, { recursive: true, force: true });
    renameSync(staging, cargoHome);
  } catch (error) {
    if (!existsSync(cargoHome))
      throw error;
  } finally {
    rmSync(staging, { recursive: true, force: true });
  }
}
function readSeedId(directory) {
  try {
    return readFileSync(join(directory, CARGO_SEED_ID), "utf8").trim();
  } catch {
    return;
  }
}
function mergeMissing(from, to) {
  for (const entry of readdirSync(from, { withFileTypes: true })) {
    const source = join(from, entry.name);
    const destination = join(to, entry.name);
    if (entry.isDirectory()) {
      mkdirSync(destination, { recursive: true });
      mergeMissing(source, destination);
    } else if (!existsSync(destination)) {
      const staging = `${destination}.${process.pid}`;
      copyFileSync(source, staging);
      renameSync(staging, destination);
    }
  }
}

// packages/runtime-rust/src/exec.ts
function checkpointJson(input) {
  if (!input.checkpoint)
    return null;
  if (input.checkpoint.manifest.payloadKind === "json-inline") {
    return JSON.stringify(input.checkpoint.manifest.inlineJson ?? null);
  }
  if (input.checkpoint.manifest.payloadKind === "text-sidecar" && input.checkpoint.payloadAbsolutePath) {
    return readFileSync2(input.checkpoint.payloadAbsolutePath, "utf8");
  }
  throw new Error("Rust does not support checkpoint payload kind " + input.checkpoint.manifest.payloadKind);
}
function writeRustExecFile(input) {
  const markers = buildMarkers2();
  const workspace = createExecFileWorkspace2(input.rootDir, input.sessionId, input.execId);
  try {
    const sourcePath = workspace.file("submission.rs");
    const specPath = workspace.file("submission.json");
    const checkpointOutputPath = workspace.file("checkpoint.json");
    writeFileSync(sourcePath, input.code + `
`, "utf8");
    const spec = {
      schema_version: 1,
      event_prefix: markers.eventPrefix,
      exec_id: input.execId,
      code: input.code,
      source_path: sourcePath,
      durability_mode: input.durabilityMode,
      checkpoint_json: checkpointJson(input),
      checkpoint_output_path: checkpointOutputPath
    };
    writeFileSync(specPath, JSON.stringify(spec), "utf8");
    return {
      submitText: ":exec " + specPath + `
`,
      collector: createPromptAwareEnvelopeCollector2(markers, [RUST_PROMPT]),
      dispose: workspace.dispose
    };
  } catch (error) {
    workspace.dispose();
    throw error;
  }
}
var rustDoctor = {
  probeCode: "40 + 2",
  successMessage: "ok: Bayma executed Rust in the Rust runtime",
  assertSuccess: (exec) => assertDoctorExecResult2(exec, "42")
};

// packages/runtime-rust/src/profile.ts
var rustModelProfile = {
  runtimeId: "rust",
  heading: "Rust",
  description: 'Runs through EVcxR with real Rust ownership and borrowing. Items, imports, variables, and dependencies can persist. Add Cargo dependencies with :dep package = "version"; :lockfile path/to/Cargo.lock resolves them against that lockfile, keeping its versions, yanked ones included, and :lockfile alone stops. Checkpointed recovery preserves only serde-compatible values explicitly written through bayma_rust_support; Bayma never replays prior cells or their side effects. bayma_rust_support::display_image(bytes) or display_image_file(path) shows the model a PNG, JPEG, GIF, or WebP image when the cell ends. A panic, unsafe crash, or forced interrupt can replace the execution child and lose non-checkpointed values. Join threads and child processes before a cell returns; interactive stdin and causally attributed output from detached writers are unavailable.'
};

// packages/runtime-rust/src/index.ts
var rustAdapter = {
  runtimeId: "rust",
  displayName: "Rust",
  checkpointCodecs: [JSON_CHECKPOINT_CODEC2, RUST_CHECKPOINT_CODEC],
  modelProfile: rustModelProfile,
  createTransport: createRustTransport,
  prepareExec: writeRustExecFile,
  doctor: rustDoctor
};
export {
  rustAdapter
};
