#!/usr/bin/env -S node --disable-warning=ExperimentalWarning
import {
  assertDoctorExecResult2,
  JSON_CHECKPOINT_CODEC2,
  createExecFileWorkspace2,
  payloadValue2,
  ProcessTransport2,
  buildMarkers2,
  createPromptAwareEnvelopeCollector2
} from "./main-c0bag0nq.js";

// packages/runtime-cpp/src/exec.ts
import { writeFileSync } from "node:fs";

// packages/runtime-cpp/src/transport.ts
var CPP_PROMPT = "BAYMA> ";
var HOST_BIN = {
  c: "BAYMA_C_HOST_BIN",
  "c++": "BAYMA_CPP_HOST_BIN"
};
function createCppTransport(language) {
  return new ProcessTransport2({
    platformId: "stdio",
    promptRe: /(?:^|[\r\n])BAYMA> /g,
    interruptStrategy: "recycle",
    command: () => ({
      file: payloadValue2(HOST_BIN[language]),
      args: [`--language=${language}`]
    })
  });
}

// packages/runtime-cpp/src/exec.ts
function checkpointJson(input) {
  if (!input.checkpoint)
    return null;
  if (input.checkpoint.manifest.payloadKind !== "json-inline") {
    throw new Error(`C and C++ do not support checkpoint payload kind ${input.checkpoint.manifest.payloadKind}`);
  }
  return JSON.stringify(input.checkpoint.manifest.inlineJson ?? null);
}
function writeCppExecFile(input) {
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
      collector: createPromptAwareEnvelopeCollector2(markers, [CPP_PROMPT]),
      dispose: workspace.dispose
    };
  } catch (error) {
    workspace.dispose();
    throw error;
  }
}
function cppDoctor(displayName) {
  return {
    probeCode: "40 + 2",
    successMessage: `ok: Bayma executed ${displayName} in the ${displayName} runtime`,
    assertSuccess: (exec) => assertDoctorExecResult2(exec, "42")
  };
}

// packages/runtime-cpp/src/profile.ts
var SHARED = [
  "Clang's incremental Interpreter (clang-repl) compiles each exec and runs it in the session's process, so definitions, variables, and #includes persist across execs.",
  "A trailing expression without a semicolon is the exec's result.",
  "Quoted #include paths resolve from the session working directory.",
  "A compile_flags.txt there, one argument per line as clangd reads it, sets the session's compiler arguments (such as -I, -isystem, -D, and -std=) and loads its libraries (-L and -l, or a library's path), each with the libraries it needs from those directories.",
  "Checkpointed recovery preserves only JSON text written with bayma_write_checkpoint(json) and read with bayma_read_checkpoint().",
  "bayma_display_image(data, size) or bayma_display_image_file(path) shows the model a PNG, JPEG, GIF, or WebP image when the cell ends."
].join(" ");
var FAILURES = "the exec ends with an error and the session continues in a fresh interpreter, keeping its checkpoint but losing its definitions and values; an interrupt does the same. Output written by threads after an exec returns is discarded, and interactive stdin is unavailable.";
var cppModelProfile = {
  runtimeId: "cpp",
  heading: "C++",
  description: `C++23 with libc++, or from C++17 on with -std=; -stdlib=libstdc++ uses GCC's libstdc++, as a distribution's C++ libraries do. ${SHARED} An exec that fails to compile or link is undone, but for the headers it included that parsed, which stay included; a header that failed is read again when next included. Cells share what loaded libraries define, as a linked program would: template instantiations, type_info, and thread-local variables among them. Results print their contents when std::format can format them (numbers, strings, standard containers, pairs, and tuples, with libc++ in C++23), and otherwise when they are numbers, strings, or enumerations or have an operator<<. Clang's Interpreter cannot name a lambda written in a statement at a cell's top level, as a call's argument or in a block, if, or loop, and may crash the session on one; write the lambda as a top-level variable's initializer, or inside a function. If an exec crashes the process, throws an uncaught exception, or exits, ${FAILURES}`
};
var cModelProfile = {
  runtimeId: "c",
  heading: "C",
  description: `C23, or another standard with -std=. ${SHARED} An exec that fails to compile or link is undone entirely, the headers it included among them, which the next cell to use them must include again. If an exec crashes the process or exits, ${FAILURES}`
};

// packages/runtime-cpp/src/index.ts
var cAdapter = {
  runtimeId: "c",
  displayName: "C",
  checkpointCodecs: [JSON_CHECKPOINT_CODEC2],
  modelProfile: cModelProfile,
  createTransport: () => createCppTransport("c"),
  prepareExec: writeCppExecFile,
  doctor: cppDoctor("C")
};
var cppAdapter = {
  runtimeId: "cpp",
  displayName: "C++",
  checkpointCodecs: [JSON_CHECKPOINT_CODEC2],
  modelProfile: cppModelProfile,
  createTransport: () => createCppTransport("c++"),
  prepareExec: writeCppExecFile,
  doctor: cppDoctor("C++")
};
export {
  cAdapter,
  cppAdapter
};
