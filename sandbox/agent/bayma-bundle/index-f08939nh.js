#!/usr/bin/env -S node --disable-warning=ExperimentalWarning
import {
  payloadValue2,
  assertDoctorExecResult2,
  JSON_CHECKPOINT_CODEC2,
  RUNTIME_OUTPUT_CAPTURE_POLICY2,
  RUNTIME_OUTPUT_TRUNCATION_MARKER2,
  createExecFileWorkspace2,
  ProcessTransport2,
  buildMarkers2,
  createPromptAwareEnvelopeCollector2,
  ensureRuntimeAsset2
} from "./main-eqz69xvs.js";

// packages/runtime-python/src/profile.ts
var pythonModelProfile = {
  runtimeId: "python",
  heading: "Python",
  description: "Preserves Python globals and imports. Imports, packages available from the session working directory, and top-level await in async code are available. Checkpointed recovery preserves explicit bayma_write_checkpoint(...) state using Bayma's pickle codec. bayma_display_image(image) shows the model a PNG, JPEG, GIF, or WebP image, from a path, bytes, or an object that renders as one, such as a PIL image or a matplotlib figure."
};

// packages/runtime-python/src/transport.ts
var PYTHON_PROMPT = "BAYMA> ";
var PYTHON_CHECKPOINT_CODEC = {
  codecId: "python-pickle-protocol5-v1",
  codecVersion: 1,
  payloadKind: "binary-sidecar"
};
var PYTHON_HARNESS = String.raw`
import ast
import asyncio
import builtins
import codecs
import contextlib
import hashlib
import io
import json
import os
import pickle
import reprlib
import sys
import tempfile
import traceback

MAX_MESSAGE_BYTES = ${RUNTIME_OUTPUT_CAPTURE_POLICY2.maxMessageBytes}
TRUNCATION_MARKER = ${JSON.stringify(RUNTIME_OUTPUT_TRUNCATION_MARKER2)}.encode("utf8")
RESULT_REPR = reprlib.Repr()
RESULT_REPR.maxlevel = 6
RESULT_REPR.maxdict = 100
RESULT_REPR.maxlist = 100
RESULT_REPR.maxtuple = 100
RESULT_REPR.maxset = 100
RESULT_REPR.maxfrozenset = 100
RESULT_REPR.maxdeque = 100
RESULT_REPR.maxstring = 16_384
RESULT_REPR.maxother = 16_384


def bound_text(value: str) -> str:
    encoded = value.encode("utf8", errors="replace")
    if len(encoded) <= MAX_MESSAGE_BYTES:
        return value
    payload_bytes = MAX_MESSAGE_BYTES - len(TRUNCATION_MARKER)
    left_bytes = payload_bytes // 2
    right_bytes = payload_bytes - left_bytes
    return (
        encoded[:left_bytes].decode("utf8", errors="ignore")
        + TRUNCATION_MARKER.decode("utf8")
        + encoded[-right_bytes:].decode("utf8", errors="ignore")
    )

SESSION_GLOBALS = {
    "__name__": "__main__",
    "__builtins__": builtins,
    "_": None,
    "_error": None,
}
CHECKPOINT = None
# With checkpoint reuse, the committed payload CHECKPOINT is read from at its
# next read, in an exec or between execs, where nothing has read or written it
# since it was committed; None once something has, or where none waits.
CHECKPOINT_UNREAD = None
# The running exec's event prefix and directory, while one runs.
RUNNING_EXEC = None

SESSION_CWD = os.getcwd()
if SESSION_CWD not in sys.path:
    sys.path.insert(0, SESSION_CWD)


def emit(prefix: str, kind: str, text: str | None = None) -> None:
    payload = {"kind": kind}
    if text is not None:
        payload["text"] = bound_text(text)
    write_protocol_line(prefix + json.dumps(payload, ensure_ascii=False))


def write_protocol_line(value: str) -> None:
    # sys.__stdout__ uses the active Windows code page on hosted runners.
    # Protocol JSON is UTF-8 on every platform, independent of terminal locale.
    sys.__stdout__.buffer.write((value + "\n").encode("utf8"))
    sys.__stdout__.buffer.flush()


class ChannelBufferWriter:
    def __init__(self, owner):
        self.owner = owner
        self.decoder = codecs.getincrementaldecoder("utf8")(errors="replace")

    def write(self, data) -> int:
        payload = bytes(data)
        text = self.decoder.decode(payload, final=False)
        if text:
            self.owner.emit_text(text)
        return len(payload)

    def flush(self) -> None:
        return None

    def finish(self) -> None:
        text = self.decoder.decode(b"", final=True)
        self.decoder.reset()
        if text:
            self.owner.emit_text(text)

    def writable(self) -> bool:
        return True


class ChannelWriter:
    encoding = "utf-8"
    errors = "replace"

    def __init__(self, prefix: str, kind: str):
        self.prefix = prefix
        self.kind = kind
        self.buffer = ChannelBufferWriter(self)

    def emit_text(self, text: str) -> None:
        emit(self.prefix, self.kind, text)

    def write(self, text: str) -> int:
        if not isinstance(text, str):
            raise TypeError("write() argument must be str")
        if text:
            # Empty text writes do not establish an ordering boundary and must
            # not flush an incomplete UTF-8 scalar from the binary surface.
            self.buffer.finish()
            self.emit_text(text)
        return len(text)

    def flush(self) -> None:
        self.buffer.flush()

    def finish(self) -> None:
        self.buffer.finish()

    def writable(self) -> bool:
        return True

    def isatty(self) -> bool:
        return False


class HashingWriter:
    """A binary file that hashes what is written to it, as SHA-256."""

    def __init__(self, file):
        self.file = file
        self.hash = hashlib.sha256()

    def write(self, data) -> int:
        self.hash.update(data)
        return self.file.write(data)


def checkpoint_compatibility():
    """What a checkpoint this runtime makes says of it, as its manifest keeps it."""
    return {
        "runtimeVersion": sys.version.split()[0],
        "languageVersion": sys.version.split()[0],
        "platform": sys.platform,
    }


def bayma_read_checkpoint():
    global CHECKPOINT, CHECKPOINT_UNREAD
    if CHECKPOINT_UNREAD is not None:
        with open(CHECKPOINT_UNREAD, "rb") as payload:
            CHECKPOINT = pickle.load(payload)
        CHECKPOINT_UNREAD = None
    return CHECKPOINT


def bayma_write_checkpoint(value):
    global CHECKPOINT, CHECKPOINT_UNREAD
    CHECKPOINT = value
    CHECKPOINT_UNREAD = None
    return value


def image_bytes(image) -> bytes:
    if isinstance(image, (str, os.PathLike)):
        with open(image, "rb") as file:
            return file.read()
    if isinstance(image, (bytes, bytearray, memoryview)):
        return bytes(image)
    for render in ("_repr_png_", "_repr_jpeg_"):
        method = getattr(image, render, None)
        if callable(method) and (rendered := method()) is not None:
            return rendered
    if callable(getattr(image, "savefig", None)):
        buffer = io.BytesIO()
        image.savefig(buffer, format="png")
        return buffer.getvalue()
    raise TypeError(
        "bayma_display_image takes a path, bytes, or an object with _repr_png_, _repr_jpeg_, or savefig, not "
        + type(image).__name__
    )


def bayma_display_image(image) -> None:
    """Shows the model a PNG, JPEG, GIF, or WebP image: a file's path, its
    bytes, or an object that renders as one, such as a PIL image or a
    matplotlib figure."""
    if RUNNING_EXEC is None:
        raise RuntimeError("bayma_display_image shows images only while an exec runs")
    prefix, directory = RUNNING_EXEC
    data = image_bytes(image)
    # The exec's directory, which bayma removes after the exec, once it has
    # kept a copy of each image.
    descriptor, path = tempfile.mkstemp(prefix="image-", dir=directory)
    with os.fdopen(descriptor, "wb") as file:
        file.write(data)
    write_protocol_line(prefix + json.dumps({"kind": "image", "payloadPath": path}, ensure_ascii=False))


SESSION_GLOBALS["bayma_read_checkpoint"] = bayma_read_checkpoint
SESSION_GLOBALS["bayma_write_checkpoint"] = bayma_write_checkpoint
SESSION_GLOBALS["bayma_display_image"] = bayma_display_image


def compile_submission(source: str, filename: str):
    tree = ast.parse(source, filename=filename, mode="exec")
    result_name = "__bayma_result__"
    has_result = bool(tree.body) and isinstance(tree.body[-1], ast.Expr)
    if has_result:
        tree.body[-1] = ast.Assign(
            targets=[ast.Name(id=result_name, ctx=ast.Store())],
            value=tree.body[-1].value,
        )
        ast.fix_missing_locations(tree)
    flags = getattr(ast, "PyCF_ALLOW_TOP_LEVEL_AWAIT", 0)
    code = compile(tree, filename, "exec", flags=flags, dont_inherit=True)
    return code, has_result, result_name


def format_result(value):
    try:
        return RESULT_REPR.repr(value)
    except Exception:
        return "<unreprable result>"


def run_submission(spec_path: str) -> None:
    global CHECKPOINT, CHECKPOINT_UNREAD, RUNNING_EXEC
    with open(spec_path, "r", encoding="utf8") as handle:
        spec = json.load(handle)
    prefix = spec["event_prefix"]
    # With checkpoint reuse, the committed checkpoint is read only as the exec
    # reads it, as it would be at its start without: so what the exec reads,
    # and what it commits, are what they would be, but that an exec that
    # neither reads nor writes it reads nothing of it, and commits nothing.
    reuse = spec.get("checkpoint_reuse") is True
    checkpoint = spec.get("checkpoint")
    CHECKPOINT_UNREAD = None
    if spec.get("durability_mode") == "checkpointed":
        if checkpoint is None:
            CHECKPOINT = None
        elif checkpoint.get("payload_kind") == "binary-sidecar":
            if reuse:
                CHECKPOINT = None
                CHECKPOINT_UNREAD = checkpoint["payload_path"]
            else:
                with open(checkpoint["payload_path"], "rb") as payload:
                    CHECKPOINT = pickle.load(payload)
        elif checkpoint.get("payload_kind") == "json-inline":
            CHECKPOINT = checkpoint.get("inline_json")
        else:
            raise RuntimeError("unsupported Python checkpoint payload kind: " + str(checkpoint.get("payload_kind")))
    stdout = ChannelWriter(prefix, "stdout")
    stderr = ChannelWriter(prefix, "stderr")
    RUNNING_EXEC = (prefix, os.path.dirname(spec_path))
    try:
        SESSION_GLOBALS.pop("__bayma_result__", None)
        code, has_result, result_name = compile_submission(spec["code"], spec["source_path"])
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            value = eval(code, SESSION_GLOBALS)
            if asyncio.iscoroutine(value):
                asyncio.run(value)
        if has_result and result_name in SESSION_GLOBALS:
            result = SESSION_GLOBALS.pop(result_name)
            if result is not None:
                SESSION_GLOBALS["_"] = result
                emit(prefix, "result", format_result(result))
    except BaseException as error:
        SESSION_GLOBALS["_error"] = error
        emit(prefix, "error", "".join(traceback.format_exception(error)).rstrip())
    finally:
        stdout.finish()
        stderr.finish()
        if spec.get("durability_mode") == "checkpointed":
            try:
                checkpoint_output_path = spec["checkpoint_output_path"]
                # The committed checkpoint is the one this exec would commit only
                # where it was made as this runtime makes one now.
                committed = checkpoint if reuse and checkpoint is not None and checkpoint.get("payload_kind") == "binary-sidecar" else None
                made_alike = (
                    committed is not None
                    and committed.get("codec_version") == ${PYTHON_CHECKPOINT_CODEC.codecVersion}
                    and committed.get("compatibility") == checkpoint_compatibility()
                )
                if made_alike and CHECKPOINT_UNREAD is not None:
                    # Neither read nor written: nothing to commit.
                    event = {"kind": "checkpoint-unchanged"}
                else:
                    # Unread, it is read to be committed as this runtime makes one.
                    value = bayma_read_checkpoint()
                    with open(checkpoint_output_path, "wb") as payload:
                        written = HashingWriter(payload)
                        pickle.dump(value, written, protocol=5)
                    if made_alike and written.hash.hexdigest() == committed.get("sha256"):
                        # Left as committed: nothing to commit, and what it holds
                        # is read again from the committed payload, at its next
                        # read, between execs or in the next.
                        os.remove(checkpoint_output_path)
                        event = {"kind": "checkpoint-unchanged"}
                        CHECKPOINT = None
                        CHECKPOINT_UNREAD = committed["payload_path"]
                    else:
                        event = {
                            "kind": "checkpoint",
                            "checkpoint": {
                                "runtimeId": "python",
                                "codecId": ${JSON.stringify(PYTHON_CHECKPOINT_CODEC.codecId)},
                                "codecVersion": ${PYTHON_CHECKPOINT_CODEC.codecVersion},
                                "payloadKind": ${JSON.stringify(PYTHON_CHECKPOINT_CODEC.payloadKind)},
                                "payloadPath": checkpoint_output_path,
                                "compatibility": checkpoint_compatibility(),
                            },
                        }
                write_protocol_line(prefix + json.dumps(event, ensure_ascii=False))
            except Exception as error:
                emit(prefix, "error", "".join(traceback.format_exception(error)).rstrip())
        RUNNING_EXEC = None
        emit(prefix, "done")


def main() -> None:
    sys.stdout.write("BAYMA> ")
    sys.stdout.flush()
    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            sys.stdout.write("BAYMA> ")
            sys.stdout.flush()
            continue
        if line.startswith(":exec "):
            run_submission(line[len(":exec "):])
        elif line.startswith(":probe "):
            write_protocol_line("__BAYMA_READY_" + line[len(":probe "):] + "__")
        sys.stdout.write("BAYMA> ")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
`;
function pythonHarnessPath() {
  return ensureRuntimeAsset2("python", "python-harness.py", PYTHON_HARNESS.trimStart());
}
function createPythonTransport() {
  return new ProcessTransport2({
    platformId: "stdio",
    promptRe: /(?:^|[\r\n])BAYMA> /g,
    interruptStrategy: "sigint",
    interruptProbe: (nonce) => ({
      input: `:probe ${nonce}
`,
      expectedOutput: `__BAYMA_READY_${nonce}__`
    }),
    command: () => ({
      file: payloadValue2("BAYMA_PYTHON_BIN"),
      args: ["-u", pythonHarnessPath()],
      env: {
        PYTHONNOUSERSITE: "1",
        PYTHONUNBUFFERED: "1"
      }
    })
  });
}

// packages/runtime-python/src/exec.ts
import { writeFileSync } from "node:fs";
function writePythonExecFile(input) {
  const markers = buildMarkers2();
  const workspace = createExecFileWorkspace2(input.rootDir, input.sessionId, input.execId);
  try {
    const sourcePath = workspace.file("submission.py");
    const specPath = workspace.file("submission.json");
    const checkpointOutputPath = workspace.file("checkpoint.pkl");
    writeFileSync(sourcePath, input.code + `
`, "utf8");
    const spec = {
      event_prefix: markers.eventPrefix,
      code: input.code,
      source_path: sourcePath,
      durability_mode: input.durabilityMode,
      checkpoint: input.checkpoint ? {
        codec_id: input.checkpoint.manifest.codecId,
        payload_kind: input.checkpoint.manifest.payloadKind,
        inline_json: input.checkpoint.manifest.inlineJson,
        payload_path: input.checkpoint.payloadAbsolutePath,
        sha256: input.checkpoint.manifest.sha256,
        codec_version: input.checkpoint.manifest.codecVersion,
        compatibility: { ...input.checkpoint.manifest.compatibility }
      } : null,
      checkpoint_output_path: checkpointOutputPath,
      checkpoint_reuse: input.checkpointReuse ?? false
    };
    writeFileSync(specPath, JSON.stringify(spec), "utf8");
    return {
      submitText: `:exec ${specPath}
`,
      collector: createPromptAwareEnvelopeCollector2(markers, [PYTHON_PROMPT]),
      dispose: workspace.dispose
    };
  } catch (error) {
    workspace.dispose();
    throw error;
  }
}
var pythonDoctor = {
  probeCode: "40 + 2",
  successMessage: "ok: Bayma executed Python in the Python runtime",
  assertSuccess: (exec) => assertDoctorExecResult2(exec, "42")
};

// packages/runtime-python/src/index.ts
var pythonAdapter = {
  runtimeId: "python",
  displayName: "Python",
  checkpointCodecs: [JSON_CHECKPOINT_CODEC2, PYTHON_CHECKPOINT_CODEC],
  modelProfile: pythonModelProfile,
  createTransport: createPythonTransport,
  prepareExec: writePythonExecFile,
  doctor: pythonDoctor
};
export {
  pythonAdapter
};
