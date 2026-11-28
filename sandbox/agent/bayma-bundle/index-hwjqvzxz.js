#!/usr/bin/env -S node --disable-warning=ExperimentalWarning
import {
  payloadValue2,
  assertDoctorExecResult2,
  JSON_CHECKPOINT_CODEC2,
  RUNTIME_OUTPUT_CAPTURE_POLICY2,
  RUNTIME_OUTPUT_TRUNCATION_MARKER2,
  createExecFileWorkspace2,
  cacheRoot2,
  ProcessTransport2,
  buildMarkers2,
  createPromptAwareEnvelopeCollector2,
  ensureRuntimeAsset2
} from "./main-4vabrjre.js";

// packages/runtime-dotnet-script/src/profile.ts
var dotnetScriptModelProfile = {
  runtimeId: "dotnet-script",
  heading: "C# (dotnet-script)",
  description: "Supports persistent script state, top-level async code, console output, common namespaces, cwd-relative script references, notebook-style multiline submissions, and REPL-style last-expression rendering. Checkpointed recovery preserves explicit bayma_write_checkpoint(...) state using Bayma's JSON codec. bayma_display_image(path) or bayma_display_image(bytes) shows the model a PNG, JPEG, GIF, or WebP image. The session's process keeps every cell it compiled, each costing more memory the longer the session runs: once a cell has ended, one that has come to hold more memory of its own than the server's runtime memory budget past what it held as it started, 2 GiB unless the server is told otherwise, is started again from the session's checkpoint, and the next cell is told so."
};

// packages/runtime-dotnet-script/src/transport.ts
import { join as join2 } from "node:path";

// packages/runtime-dotnet-script/src/paths.ts
import { existsSync, readFileSync, readdirSync, realpathSync } from "node:fs";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
function parseVersion(version) {
  const buildParts = version.split("+");
  if (buildParts.length > 2 || buildParts.length === 2 && !/^[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*$/.test(buildParts[1])) {
    return null;
  }
  const withoutBuild = buildParts[0];
  const prereleaseIndex = withoutBuild.indexOf("-");
  const coreText = prereleaseIndex === -1 ? withoutBuild : withoutBuild.slice(0, prereleaseIndex);
  const prereleaseText = prereleaseIndex === -1 ? null : withoutBuild.slice(prereleaseIndex + 1);
  if (!/^\d+(?:\.\d+)*$/.test(coreText) || prereleaseText !== null && !/^[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*$/.test(prereleaseText)) {
    return null;
  }
  return {
    core: coreText.split("."),
    prerelease: prereleaseText?.split(".") ?? null
  };
}
function isValidVersion(version) {
  return parseVersion(version) !== null;
}
function compareNumericIdentifiers(left, right) {
  const normalizedLeft = left.replace(/^0+(?=\d)/, "");
  const normalizedRight = right.replace(/^0+(?=\d)/, "");
  return normalizedLeft.length - normalizedRight.length || normalizedLeft.localeCompare(normalizedRight);
}
function compareVersionPrecedence(left, right) {
  const parsedLeft = parseVersion(left);
  const parsedRight = parseVersion(right);
  if (!parsedLeft || !parsedRight)
    return left.localeCompare(right);
  const maxLength = Math.max(parsedLeft.core.length, parsedRight.core.length);
  for (let index = 0;index < maxLength; index += 1) {
    const delta = compareNumericIdentifiers(parsedLeft.core[index] ?? "0", parsedRight.core[index] ?? "0");
    if (delta !== 0)
      return delta;
  }
  if (parsedLeft.prerelease === null || parsedRight.prerelease === null) {
    if (parsedLeft.prerelease === parsedRight.prerelease) {
      return left.localeCompare(right);
    }
    return parsedLeft.prerelease === null ? 1 : -1;
  }
  const maxPrereleaseLength = Math.max(parsedLeft.prerelease.length, parsedRight.prerelease.length);
  for (let index = 0;index < maxPrereleaseLength; index += 1) {
    const leftIdentifier = parsedLeft.prerelease[index];
    const rightIdentifier = parsedRight.prerelease[index];
    if (leftIdentifier === undefined || rightIdentifier === undefined) {
      return leftIdentifier === undefined ? -1 : 1;
    }
    const leftNumeric = /^\d+$/.test(leftIdentifier);
    const rightNumeric = /^\d+$/.test(rightIdentifier);
    if (leftNumeric && rightNumeric) {
      const delta = compareNumericIdentifiers(leftIdentifier, rightIdentifier);
      if (delta !== 0)
        return delta;
      continue;
    }
    if (leftNumeric !== rightNumeric)
      return leftNumeric ? -1 : 1;
    const delta = leftIdentifier.localeCompare(rightIdentifier);
    if (delta !== 0)
      return delta;
  }
  return left.localeCompare(right);
}
function parseTfmVersion(tfm) {
  const match = tfm.match(/^net(\d+)\.(\d+)$/);
  if (!match)
    return null;
  return [Number(match[1]), Number(match[2])];
}
function compareTfmVersions(left, right) {
  const leftVersion = parseTfmVersion(left);
  const rightVersion = parseTfmVersion(right);
  if (!leftVersion || !rightVersion)
    return right.localeCompare(left);
  return rightVersion[0] - leftVersion[0] || rightVersion[1] - leftVersion[1];
}
function resolveEmbeddedLibraryDir(toolsRoot) {
  const scriptBinaryPath = join(toolsRoot, "dotnet-script");
  if (!existsSync(scriptBinaryPath))
    return null;
  const embeddedPath = readFileSync(scriptBinaryPath, "latin1").match(/\.store[\\/]+dotnet-script[\\/]+[^\0\r\n]+?[\\/]+tools[\\/]+net\d+\.\d+[\\/]+any[\\/]+dotnet-script\.dll/)?.[0];
  if (!embeddedPath)
    return null;
  const relativeDir = embeddedPath.replace(/\\/g, "/").split("/").slice(0, -1);
  if (relativeDir.some((segment) => segment.length === 0 || segment === "." || segment === "..")) {
    return null;
  }
  const candidate = join(toolsRoot, ...relativeDir);
  if (!existsSync(join(candidate, "Dotnet.Script.Core.dll")))
    return null;
  const physicalRelative = relative(realpathSync(toolsRoot), realpathSync(candidate));
  if (physicalRelative === ".." || physicalRelative.startsWith(`..${sep}`) || isAbsolute(physicalRelative)) {
    return null;
  }
  return resolve(candidate);
}
function resolveDotnetScriptLibraryDirFromToolsRoot(toolsRoot) {
  const embeddedLibraryDir = resolveEmbeddedLibraryDir(toolsRoot);
  if (embeddedLibraryDir)
    return embeddedLibraryDir;
  const root = join(toolsRoot, ".store", "dotnet-script");
  if (!existsSync(root)) {
    throw new Error("dotnet-script libraries not found");
  }
  const versions = readdirSync(root, { withFileTypes: true }).filter((entry) => entry.isDirectory() && isValidVersion(entry.name)).map((entry) => entry.name).sort(compareVersionPrecedence).reverse();
  for (const version of versions) {
    const base = join(root, version, "dotnet-script", version, "tools");
    if (!existsSync(base))
      continue;
    const tfms = readdirSync(base).filter((entry) => parseTfmVersion(entry)).sort(compareTfmVersions);
    for (const tfm of tfms) {
      const candidate = join(base, tfm, "any");
      if (existsSync(join(candidate, "Dotnet.Script.Core.dll"))) {
        return resolve(candidate);
      }
    }
  }
  throw new Error("dotnet-script libraries not found");
}

// packages/runtime-dotnet-script/src/transport.ts
var DOTNET_PROMPT = "BAYMA> ";
var DOTNET_STARTUP_TIMEOUT_MS = 20000;
var DOTNET_CHECKPOINT_CODEC = {
  codecId: "dotnet-system-text-json-v2",
  codecVersion: 2,
  payloadKind: "text-sidecar"
};
var DOTNET_LEGACY_INLINE_CHECKPOINT_CODEC = {
  codecId: "dotnet-system-text-json-v2",
  codecVersion: 2,
  payloadKind: "json-inline"
};
function resolveDotnetScriptCacheRoot() {
  return join2(cacheRoot2(), "dotnet-script");
}
function csharpReferenceLiteral(filename) {
  return JSON.stringify(filename.replaceAll("\\", "/"));
}
function dotnetHarnessContent(libraryDir) {
  return `#nullable enable
#r ${csharpReferenceLiteral(join2(libraryDir, "Microsoft.CodeAnalysis.dll"))}
#r ${csharpReferenceLiteral(join2(libraryDir, "Microsoft.CodeAnalysis.Scripting.dll"))}
#r ${csharpReferenceLiteral(join2(libraryDir, "Microsoft.CodeAnalysis.CSharp.Scripting.dll"))}
#r "System.Text.Json"
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.CSharp.Scripting.Hosting;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Scripting;

public static class BaymaHost
{
    private static readonly StreamWriter RawStdout = new StreamWriter(System.Console.OpenStandardOutput()) { AutoFlush = true };
    private const int MaxMessageBytes = ${RUNTIME_OUTPUT_CAPTURE_POLICY2.maxMessageBytes};
    private const string TruncationMarker = ${JSON.stringify(RUNTIME_OUTPUT_TRUNCATION_MARKER2)};
    private static readonly PortableExecutableReference[] BaseReferences = AppDomain.CurrentDomain
        .GetAssemblies()
        .Where((assembly) => !assembly.IsDynamic && !string.IsNullOrWhiteSpace(assembly.Location))
        .Select((assembly) => assembly.Location)
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .Select((path) => MetadataReference.CreateFromFile(path))
        .ToArray();
    private static readonly CSharpParseOptions ScriptParseOptions =
        new CSharpParseOptions(kind: SourceCodeKind.Script);
    private static ScriptState<object?>? State;

    public static void Emit(string prefix, string kind, string? text = null)
    {
        var payload = new System.Collections.Generic.Dictionary<string, object?>
        {
            ["kind"] = kind,
        };
        if (text != null)
        {
            payload["text"] = BoundText(text);
        }
        RawStdout.WriteLine(prefix + JsonSerializer.Serialize(payload));
    }

    public static string BoundText(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        if (bytes.Length <= MaxMessageBytes)
        {
            return value;
        }
        var markerBytes = Encoding.UTF8.GetBytes(TruncationMarker);
        var payloadBytes = MaxMessageBytes - markerBytes.Length;
        var left = payloadBytes / 2;
        var right = bytes.Length - (payloadBytes - left);
        while (left > 0 && left < bytes.Length && (bytes[left] & 0xc0) == 0x80)
        {
            left -= 1;
        }
        while (right < bytes.Length && (bytes[right] & 0xc0) == 0x80)
        {
            right += 1;
        }
        return Encoding.UTF8.GetString(bytes, 0, left)
            + TruncationMarker
            + Encoding.UTF8.GetString(bytes, right, bytes.Length - right);
    }

    public static string FormatCompilationError(CompilationErrorException error)
    {
        return error.ToString().TrimEnd();
    }

    public static string FormatRuntimeError(Exception error)
    {
        return error.ToString().TrimEnd();
    }

    private static bool IsSimpleType(Type type)
    {
        return type.IsPrimitive
            || type.IsEnum
            || type == typeof(decimal)
            || type == typeof(DateTime)
            || type == typeof(DateTimeOffset)
            || type == typeof(Guid)
            || type == typeof(TimeSpan);
    }

    private static bool IsAnonymousType(Type type)
    {
        return Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false)
            && type.Name.Contains("AnonymousType", StringComparison.Ordinal);
    }

    private static bool ShouldExpandProperties(Type type, string formatted)
    {
        if (IsAnonymousType(type))
        {
            return true;
        }

        if (type == typeof(string) || typeof(IEnumerable).IsAssignableFrom(type) || IsSimpleType(type))
        {
            return false;
        }

        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
        if (properties.Length == 0)
        {
            return false;
        }

        return formatted == type.FullName
            || formatted == type.Name
            || formatted == type.ToString()
            || (!string.IsNullOrWhiteSpace(type.FullName) && formatted.StartsWith(type.FullName + "(", StringComparison.Ordinal))
            || formatted.StartsWith(type.Name + "(", StringComparison.Ordinal)
            || formatted.Contains("AnonymousType", StringComparison.Ordinal);
    }

    public static string FormatDisplay(object? value, int depth = 0)
    {
        if (value is null)
        {
            return "null";
        }

        if (depth >= 3)
        {
            return value.GetType().Name;
        }

        switch (value)
        {
            case string text:
                return JsonSerializer.Serialize(text);
            case char character:
                return JsonSerializer.Serialize(character.ToString());
            case Exception error:
                return CSharpObjectFormatter.Instance.FormatException(error).TrimEnd();
            case ITuple tuple:
                var tupleParts = new List<string>();
                for (var index = 0; index < tuple.Length; index += 1)
                {
                    tupleParts.Add(FormatDisplay(tuple[index], depth + 1));
                }
                return "(" + string.Join(", ", tupleParts) + ")";
        }

        var type = value.GetType();
        var formatted = CSharpObjectFormatter.Instance.FormatObject(value);
        if (ShouldExpandProperties(type, formatted))
        {
            var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            var parts = new List<string>();
            foreach (var property in properties)
            {
                if (property.GetMethod is null || property.GetIndexParameters().Length > 0)
                {
                    continue;
                }

                string rendered;
                try
                {
                    rendered = FormatDisplay(property.GetValue(value), depth + 1);
                }
                catch (Exception propertyError)
                {
                    rendered = "<error: " + propertyError.Message + ">";
                }

                parts.Add(property.Name + " = " + rendered);
            }

            if (parts.Count > 0)
            {
                return "{ " + string.Join(", ", parts) + " }";
            }
        }

        return formatted;
    }

    private static ScriptOptions BuildOptions(string sourcePath)
    {
        var baseDirectory = Path.GetDirectoryName(sourcePath);
        if (string.IsNullOrWhiteSpace(baseDirectory))
        {
            baseDirectory = Directory.GetCurrentDirectory();
        }

        return ScriptOptions.Default
            .WithReferences(BaseReferences)
            .WithFilePath(sourcePath)
            .WithFileEncoding(Encoding.UTF8)
            .WithSourceResolver(
                new SourceFileResolver(
                    new[] { baseDirectory, Directory.GetCurrentDirectory() },
                    baseDirectory
                )
            )
            .WithMetadataResolver(
                ScriptMetadataResolver.Default.WithBaseDirectory(baseDirectory)
            );
    }

    private static string? BuildNormalizedSubmission(string code)
    {
        var normalized = code;
        var changed = false;
        for (var attempt = 0; attempt < 32; attempt += 1)
        {
            var insertions = FindNotebookBoundaryInsertions(normalized);
            if (insertions.Length == 0)
            {
                if (changed)
                {
                    return normalized;
                }
                insertions = FindSemicolonDiagnosticInsertions(normalized);
            }
            if (insertions.Length == 0)
            {
                return changed ? normalized : null;
            }

            var builder = new StringBuilder(normalized);
            foreach (var position in insertions)
            {
                builder.Insert(position, ";");
            }
            normalized = builder.ToString();
            changed = true;
        }
        return normalized;
    }

    private static bool EndsWithContinuationToken(string trimmed)
    {
        string[] continuationTokens =
        {
            ".",
            "?.",
            "??",
            "??=",
            "+",
            "-",
            "*",
            "/",
            "%",
            "|",
            "&",
            "^",
            "&&",
            "||",
            "=",
            "=>",
            ",",
            ":",
            "(",
            "[",
            "{",
        };

        return continuationTokens.Any((token) => trimmed.EndsWith(token, StringComparison.Ordinal));
    }

    private static bool StartsWithContinuationToken(string trimmed)
    {
        string[] continuationTokens =
        {
            ".",
            "?.",
            "??",
            "??=",
            ",",
            ":",
            ")",
            "]",
            "}",
            "+",
            "-",
            "*",
            "/",
            "%",
            "|",
            "&",
            "^",
            "&&",
            "||",
            "=>",
        };

        return continuationTokens.Any((token) => trimmed.StartsWith(token, StringComparison.Ordinal));
    }

    private static bool PrefixCanTerminateAtBoundary(string prefix)
    {
        var diagnostics = CSharpSyntaxTree.ParseText(prefix, ScriptParseOptions)
            .GetDiagnostics()
            .Where((diagnostic) => diagnostic.Severity == DiagnosticSeverity.Error)
            .ToArray();

        return diagnostics.Length == 0
            || diagnostics.All((diagnostic) =>
                diagnostic.Id == "CS1002"
                && diagnostic.GetMessage().Contains("; expected", StringComparison.Ordinal)
                && diagnostic.Location.SourceSpan.Start == prefix.Length
            );
    }

    private static int[] FindNotebookBoundaryInsertions(string code)
    {
        var lines = code.Split('\\n');
        var lineStarts = new int[lines.Length];
        var offset = 0;
        for (var index = 0; index < lines.Length; index += 1)
        {
            lineStarts[index] = offset;
            offset += lines[index].Length;
            if (index < lines.Length - 1)
            {
                offset += 1;
            }
        }

        var insertions = new List<int>();
        for (var index = 0; index < lines.Length - 1; index += 1)
        {
            var currentLine = lines[index];
            if (currentLine.EndsWith("\\r", StringComparison.Ordinal))
            {
                currentLine = currentLine[..^1];
            }

            var trimmedCurrent = currentLine.Trim();
            if (string.IsNullOrWhiteSpace(trimmedCurrent)
                || trimmedCurrent.EndsWith(";", StringComparison.Ordinal)
                || EndsWithContinuationToken(trimmedCurrent))
            {
                continue;
            }

            string? nextTrimmed = null;
            for (var nextIndex = index + 1; nextIndex < lines.Length; nextIndex += 1)
            {
                var nextLine = lines[nextIndex];
                if (nextLine.EndsWith("\\r", StringComparison.Ordinal))
                {
                    nextLine = nextLine[..^1];
                }
                var trimmed = nextLine.Trim();
                if (trimmed.Length == 0)
                {
                    continue;
                }
                nextTrimmed = trimmed;
                break;
            }

            if (string.IsNullOrWhiteSpace(nextTrimmed)
                || StartsWithContinuationToken(nextTrimmed))
            {
                continue;
            }

            var boundaryPosition = lineStarts[index] + currentLine.Length;
            if (PrefixCanTerminateAtBoundary(code[..boundaryPosition]))
            {
                insertions.Add(boundaryPosition);
            }
        }

        return insertions
            .Distinct()
            .OrderByDescending((position) => position)
            .ToArray();
    }

    private static int[] FindSemicolonDiagnosticInsertions(string code)
    {
        return CSharpSyntaxTree.ParseText(code, ScriptParseOptions)
            .GetDiagnostics()
            .Where((diagnostic) =>
                diagnostic.Id == "CS1002"
                && diagnostic.GetMessage().Contains("; expected", StringComparison.Ordinal)
            )
            .Select((diagnostic) => diagnostic.Location.SourceSpan.Start)
            .Where((position) => position >= 0 && position <= code.Length)
            .Distinct()
            .OrderByDescending((position) => position)
            .ToArray();
    }

    private static string BuildPrelude(string checkpointJson)
    {
        return $$"""
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis.CSharp.Scripting.Hosting;

public static class __bayma_internal_channels
{
    private static readonly StreamWriter RawStdout = new StreamWriter(System.Console.OpenStandardOutput()) { AutoFlush = true };
    private const int MaxMessageBytes = ${RUNTIME_OUTPUT_CAPTURE_POLICY2.maxMessageBytes};
    private const string TruncationMarker = ${JSON.stringify(RUNTIME_OUTPUT_TRUNCATION_MARKER2)};
    public static string CurrentPrefix { get; set; } = string.Empty;
    /// The running exec's directory, which bayma removes after the exec, once
    /// it has kept a copy of each image the exec showed there.
    public static string CurrentDirectory { get; set; } = string.Empty;

    private static string BoundText(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        if (bytes.Length <= MaxMessageBytes)
        {
            return value;
        }
        var markerBytes = Encoding.UTF8.GetBytes(TruncationMarker);
        var payloadBytes = MaxMessageBytes - markerBytes.Length;
        var left = payloadBytes / 2;
        var right = bytes.Length - (payloadBytes - left);
        while (left > 0 && left < bytes.Length && (bytes[left] & 0xc0) == 0x80)
        {
            left -= 1;
        }
        while (right < bytes.Length && (bytes[right] & 0xc0) == 0x80)
        {
            right += 1;
        }
        return Encoding.UTF8.GetString(bytes, 0, left)
            + TruncationMarker
            + Encoding.UTF8.GetString(bytes, right, bytes.Length - right);
    }

    public static void Emit(string kind, string? text = null)
    {
        var payload = new Dictionary<string, object?>
        {
            ["kind"] = kind,
        };
        if (text != null)
        {
            payload["text"] = BoundText(text);
        }
        RawStdout.WriteLine(CurrentPrefix + JsonSerializer.Serialize(payload));
    }

    public static void EmitEnvelope(object payload)
    {
        RawStdout.WriteLine(CurrentPrefix + JsonSerializer.Serialize(payload));
    }

    public static void EmitImage(byte[] image)
    {
        var payloadPath = Path.Combine(CurrentDirectory, "image-" + Guid.NewGuid().ToString("N"));
        File.WriteAllBytes(payloadPath, image);
        EmitEnvelope(new Dictionary<string, object?>
        {
            ["kind"] = "image",
            ["payloadPath"] = payloadPath,
        });
    }
}

public sealed class __bayma_internal_channel_writer : TextWriter
{
    private readonly string _kind;

    public __bayma_internal_channel_writer(string kind)
    {
        _kind = kind;
    }

    public override Encoding Encoding => Encoding.UTF8;

    public override void Write(string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            __bayma_internal_channels.Emit(_kind, value);
        }
    }

    public override void Write(char value)
    {
        Write(value.ToString());
    }

    public override void WriteLine(string? value)
    {
        Write((value ?? string.Empty) + Environment.NewLine);
    }

    public override void WriteLine()
    {
        Write(Environment.NewLine);
    }
}

bool __bayma_internal_is_simple_type(Type type)
{
    return type.IsPrimitive
        || type.IsEnum
        || type == typeof(decimal)
        || type == typeof(DateTime)
        || type == typeof(DateTimeOffset)
        || type == typeof(Guid)
        || type == typeof(TimeSpan);
}

bool __bayma_internal_is_anonymous_type(Type type)
{
    return Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false)
        && type.Name.Contains("AnonymousType", StringComparison.Ordinal);
}

bool __bayma_internal_should_expand_properties(Type type, string formatted)
{
    if (__bayma_internal_is_anonymous_type(type))
    {
        return true;
    }

    if (type == typeof(string) || typeof(IEnumerable).IsAssignableFrom(type) || __bayma_internal_is_simple_type(type))
    {
        return false;
    }

    var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
    if (properties.Length == 0)
    {
        return false;
    }

    return formatted == type.FullName
        || formatted == type.Name
        || formatted == type.ToString()
        || (!string.IsNullOrWhiteSpace(type.FullName) && formatted.StartsWith(type.FullName + "(", StringComparison.Ordinal))
        || formatted.StartsWith(type.Name + "(", StringComparison.Ordinal)
        || formatted.Contains("AnonymousType", StringComparison.Ordinal);
}

string __bayma_internal_format_display(object? value, int depth = 0)
{
    if (value is null)
    {
        return "null";
    }

    if (depth >= 3)
    {
        return value.GetType().Name;
    }

    switch (value)
    {
        case string text:
            return JsonSerializer.Serialize(text);
        case char character:
            return JsonSerializer.Serialize(character.ToString());
        case Exception error:
            return CSharpObjectFormatter.Instance.FormatException(error).TrimEnd();
        case ITuple tuple:
            var tupleParts = new List<string>();
            for (var index = 0; index < tuple.Length; index += 1)
            {
                tupleParts.Add(__bayma_internal_format_display(tuple[index], depth + 1));
            }
            return "(" + string.Join(", ", tupleParts) + ")";
    }

    var type = value.GetType();
    var formatted = CSharpObjectFormatter.Instance.FormatObject(value);
    if (__bayma_internal_should_expand_properties(type, formatted))
    {
        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
        var parts = new List<string>();
        foreach (var property in properties)
        {
            if (property.GetMethod is null || property.GetIndexParameters().Length > 0)
            {
                continue;
            }

            string rendered;
            try
            {
                rendered = __bayma_internal_format_display(property.GetValue(value), depth + 1);
            }
            catch (Exception propertyError)
            {
                rendered = "<error: " + propertyError.Message + ">";
            }

            parts.Add(property.Name + " = " + rendered);
        }

        if (parts.Count > 0)
        {
            return "{ " + string.Join(", ", parts) + " }";
        }
    }

    return formatted;
}

string __bayma_internal_checkpoint_json = {{JsonSerializer.Serialize(checkpointJson)}};

T? bayma_read_checkpoint<T>()
{
    if (string.IsNullOrWhiteSpace(__bayma_internal_checkpoint_json))
    {
        return default;
    }

    return JsonSerializer.Deserialize<T>(__bayma_internal_checkpoint_json);
}

object? bayma_read_checkpoint()
{
    return JsonSerializer.Deserialize<object?>(__bayma_internal_checkpoint_json);
}

T bayma_write_checkpoint<T>(T value)
{
    __bayma_internal_checkpoint_json = JsonSerializer.Serialize(value);
    return value;
}

void __bayma_internal_emit_checkpoint(string checkpointOutputPath)
{
    File.WriteAllText(
        checkpointOutputPath,
        __bayma_internal_checkpoint_json,
        new UTF8Encoding(false)
    );
    __bayma_internal_channels.EmitEnvelope(new Dictionary<string, object?>
    {
        ["kind"] = "checkpoint",
        ["checkpoint"] = new Dictionary<string, object?>
        {
            ["runtimeId"] = "dotnet-script",
            ["codecId"] = ${JSON.stringify(DOTNET_CHECKPOINT_CODEC.codecId)},
            ["codecVersion"] = ${DOTNET_CHECKPOINT_CODEC.codecVersion},
            ["payloadKind"] = ${JSON.stringify(DOTNET_CHECKPOINT_CODEC.payloadKind)},
            ["payloadPath"] = checkpointOutputPath,
            ["compatibility"] = new Dictionary<string, object?>
            {
                ["runtimeVersion"] = Environment.Version.ToString(),
                ["languageVersion"] = Environment.Version.ToString(),
                ["platform"] = Environment.OSVersion.Platform.ToString(),
            },
        },
    });
}

void bayma_display_image(byte[] image)
{
    __bayma_internal_channels.EmitImage(image);
}

void bayma_display_image(string path)
{
    __bayma_internal_channels.EmitImage(File.ReadAllBytes(path));
}

void Print(object? value)
{
    Console.WriteLine(__bayma_internal_format_display(value));
}

// What the host does of each exec in the session's own code, handed it once
// here: compiled as a submission of its own each exec, it would lengthen the
// chain of submissions, each of which Roslyn keeps, its cost growing with the
// chain's length.
AppDomain.CurrentDomain.SetData("bayma.prepare-exec", (Action<string, string?>)((prefix, directory) =>
{
    __bayma_internal_channels.CurrentPrefix = prefix;
    __bayma_internal_channels.CurrentDirectory = directory!;
    System.Console.SetOut(new __bayma_internal_channel_writer("stdout"));
    System.Console.SetError(new __bayma_internal_channel_writer("stderr"));
}));
AppDomain.CurrentDomain.SetData("bayma.emit-checkpoint", (Action<string, string>)((prefix, checkpointOutputPath) =>
{
    __bayma_internal_channels.CurrentPrefix = prefix;
    __bayma_internal_emit_checkpoint(checkpointOutputPath);
}));
""";
    }

    private static async Task EnsureStateInitialized(string sourcePath, string? checkpointJson)
    {
        if (State != null)
        {
            return;
        }

        State = await CSharpScript.RunAsync(
            BuildPrelude(string.IsNullOrWhiteSpace(checkpointJson) ? "null" : checkpointJson),
            BuildOptions(sourcePath)
        );
    }

    public static async Task<object?> ExecuteSubmission(
        string code,
        string sourcePath,
        string? checkpointJson,
        string eventPrefix
    )
    {
        await EnsureStateInitialized(sourcePath, checkpointJson);
        Handed<Action<string, string?>>("bayma.prepare-exec")(eventPrefix, Path.GetDirectoryName(sourcePath));
        try
        {
            State = await State!.ContinueWithAsync(code, BuildOptions(sourcePath));
            return State.ReturnValue;
        }
        catch (CompilationErrorException error)
        {
            var retryCode = BuildNormalizedSubmission(code);
            if (string.IsNullOrWhiteSpace(retryCode) || string.Equals(retryCode, code, StringComparison.Ordinal))
            {
                throw;
            }

            try
            {
                State = await State!.ContinueWithAsync(retryCode, BuildOptions(sourcePath));
                return State.ReturnValue;
            }
            catch (CompilationErrorException)
            {
                throw error;
            }
        }
    }

    public static void EmitCheckpoint(
        string eventPrefix,
        string checkpointOutputPath
    )
    {
        if (State == null)
        {
            return;
        }

        Handed<Action<string, string>>("bayma.emit-checkpoint")(eventPrefix, checkpointOutputPath);
    }

    /// What the session's prelude handed the host under the name given.
    private static T Handed<T>(string name) where T : Delegate
    {
        return AppDomain.CurrentDomain.GetData(name) as T
            ?? throw new InvalidOperationException("the session's prelude handed the host no " + name);
    }

    public static string FormatResult(object result)
    {
        return BoundText(FormatDisplay(result));
    }

}

public sealed class ChannelTextWriter : TextWriter
{
    private readonly string _prefix;
    private readonly string _kind;

    public ChannelTextWriter(string prefix, string kind)
    {
        _prefix = prefix;
        _kind = kind;
    }

    public override Encoding Encoding => Encoding.UTF8;

    public override void Write(string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            BaymaHost.Emit(_prefix, _kind, value);
        }
    }

    public override void Write(ReadOnlySpan<char> buffer)
    {
        if (!buffer.IsEmpty)
        {
            BaymaHost.Emit(_prefix, _kind, buffer.ToString());
        }
    }

    public override void Write(char value)
    {
        Write(value.ToString());
    }

    public override void Write(char[] buffer, int index, int count)
    {
        if (count > 0)
        {
            BaymaHost.Emit(_prefix, _kind, new string(buffer, index, count));
        }
    }

    public override void WriteLine(string? value)
    {
        Write((value ?? string.Empty) + Environment.NewLine);
    }

    public override void WriteLine()
    {
        Write(Environment.NewLine);
    }

    public override void WriteLine(ReadOnlySpan<char> buffer)
    {
        if (!buffer.IsEmpty)
        {
            Write(buffer.ToString());
        }
        Write(Environment.NewLine);
    }

    public override void WriteLine(char[] buffer, int index, int count)
    {
        Write(buffer, index, count);
        Write(Environment.NewLine);
    }

    public override void Flush()
    {
    }
}

public sealed class SubmissionSpec
{
    public string event_prefix { get; set; } = string.Empty;
    public string code { get; set; } = string.Empty;
    public string source_path { get; set; } = string.Empty;
    public string durability_mode { get; set; } = string.Empty;
    public string? checkpoint_json { get; set; }
    public string checkpoint_output_path { get; set; } = string.Empty;
}

async Task RunSubmission(string specPath)
{
    var spec = JsonSerializer.Deserialize<SubmissionSpec>(File.ReadAllText(specPath))
        ?? throw new InvalidOperationException("invalid submission spec");
    var stdout = new ChannelTextWriter(spec.event_prefix, "stdout");
    var stderr = new ChannelTextWriter(spec.event_prefix, "stderr");
    var originalOut = System.Console.Out;
    var originalError = System.Console.Error;
    try
    {
        System.Console.SetOut(stdout);
        System.Console.SetError(stderr);
        var result = await BaymaHost.ExecuteSubmission(
            spec.code,
            spec.source_path,
            spec.checkpoint_json,
            spec.event_prefix
        );
        if (result != null)
        {
            BaymaHost.Emit(
                spec.event_prefix,
                "result",
                BaymaHost.FormatResult(result)
            );
        }
    }
    catch (CompilationErrorException error)
    {
        BaymaHost.Emit(
            spec.event_prefix,
            "error",
            BaymaHost.FormatCompilationError(error)
        );
    }
    catch (Exception error)
    {
        BaymaHost.Emit(
            spec.event_prefix,
            "error",
            BaymaHost.FormatRuntimeError(error)
        );
    }
    finally
    {
        System.Console.SetOut(originalOut);
        System.Console.SetError(originalError);
        if (spec.durability_mode == "checkpointed")
        {
            try
            {
                BaymaHost.EmitCheckpoint(
                    spec.event_prefix,
                    spec.checkpoint_output_path
                );
            }
            catch (Exception checkpointError)
            {
                BaymaHost.Emit(
                    spec.event_prefix,
                    "error",
                    BaymaHost.FormatRuntimeError(checkpointError)
                );
            }
        }
        BaymaHost.Emit(spec.event_prefix, "done");
    }
}

System.Console.Write("BAYMA> ");
System.Console.Out.Flush();
string? line;
while ((line = System.Console.ReadLine()) != null)
{
    line = line.Trim();
    if (string.IsNullOrEmpty(line))
    {
        System.Console.Write("BAYMA> ");
        System.Console.Out.Flush();
        continue;
    }
    if (line.StartsWith(":exec "))
    {
        await RunSubmission(line.Substring(6));
    }
    else if (line.StartsWith(":probe "))
    {
        System.Console.WriteLine("__BAYMA_READY_" + line.Substring(7) + "__");
        System.Console.Out.Flush();
    }
    System.Console.Write("BAYMA> ");
    System.Console.Out.Flush();
}
`;
}
function dotnetHarnessPath(libraryDir) {
  return ensureRuntimeAsset2("dotnet-script", "bayma-dotnet-harness.csx", dotnetHarnessContent(libraryDir).trimStart());
}
function createDotnetScriptTransport() {
  return new ProcessTransport2({
    platformId: "stdio",
    promptRe: /(?:^|[\r\n])BAYMA> /g,
    interruptStrategy: "sigint",
    interruptProbe: (nonce) => ({
      input: `:probe ${nonce}
`,
      expectedOutput: `__BAYMA_READY_${nonce}__`
    }),
    startupTimeoutMs: DOTNET_STARTUP_TIMEOUT_MS,
    command: () => {
      const dotnetRoot = process.env.BAYMA_DOTNET_ROOT ?? process.env.DOTNET_ROOT;
      if (!dotnetRoot)
        throw new Error("BAYMA_DOTNET_ROOT is not set; the payload is not resolved");
      return {
        file: payloadValue2("BAYMA_DOTNET_SCRIPT_BIN"),
        args: [dotnetHarnessPath(dotnetScriptLibraryDir())],
        env: {
          DOTNET_SCRIPT_CACHE_LOCATION: resolveDotnetScriptCacheRoot(),
          DOTNET_ROOT: dotnetRoot,
          DOTNET_ROOT_X64: dotnetRoot
        }
      };
    }
  });
}
function dotnetScriptLibraryDir() {
  const configured = process.env.BAYMA_DOTNET_SCRIPT_LIB_DIR;
  if (configured)
    return configured;
  return resolveDotnetScriptLibraryDirFromToolsRoot(join2(payloadValue2("BAYMA_DOTNET_SCRIPT_BIN"), ".."));
}

// packages/runtime-dotnet-script/src/exec.ts
import { readFileSync as readFileSync2, writeFileSync } from "node:fs";
function checkpointJson(input) {
  if (!input.checkpoint)
    return null;
  if (input.checkpoint.manifest.payloadKind === "json-inline") {
    return JSON.stringify(input.checkpoint.manifest.inlineJson ?? null);
  }
  if (input.checkpoint.manifest.payloadKind === "text-sidecar" && input.checkpoint.payloadAbsolutePath) {
    return readFileSync2(input.checkpoint.payloadAbsolutePath, "utf8");
  }
  throw new Error(`dotnet-script does not support checkpoint payload kind ${input.checkpoint.manifest.payloadKind}`);
}
function writeDotnetExecFile(input) {
  const markers = buildMarkers2();
  const workspace = createExecFileWorkspace2(input.rootDir, input.sessionId, input.execId);
  try {
    const sourcePath = workspace.file("submission.csx");
    const specPath = workspace.file("submission.json");
    const checkpointOutputPath = workspace.file("checkpoint.json");
    writeFileSync(sourcePath, input.code + `
`, "utf8");
    const spec = {
      event_prefix: markers.eventPrefix,
      code: input.code,
      source_path: sourcePath,
      durability_mode: input.durabilityMode,
      checkpoint_json: checkpointJson(input),
      checkpoint_output_path: checkpointOutputPath
    };
    writeFileSync(specPath, JSON.stringify(spec), "utf8");
    return {
      submitText: `:exec ${specPath}
`,
      collector: createPromptAwareEnvelopeCollector2(markers, [DOTNET_PROMPT]),
      dispose: workspace.dispose
    };
  } catch (error) {
    workspace.dispose();
    throw error;
  }
}
var dotnetScriptDoctor = {
  probeCode: "40 + 2",
  successMessage: "ok: Bayma executed C# in the dotnet-script runtime",
  assertSuccess: (exec) => assertDoctorExecResult2(exec, "42")
};

// packages/runtime-dotnet-script/src/index.ts
var dotnetScriptAdapter = {
  runtimeId: "dotnet-script",
  displayName: "dotnet-script",
  checkpointCodecs: [
    JSON_CHECKPOINT_CODEC2,
    DOTNET_LEGACY_INLINE_CHECKPOINT_CODEC,
    DOTNET_CHECKPOINT_CODEC
  ],
  modelProfile: dotnetScriptModelProfile,
  createTransport: createDotnetScriptTransport,
  prepareExec: writeDotnetExecFile,
  doctor: dotnetScriptDoctor,
  keepsOfEachCell: "every cell it compiled, each costing more the longer the session runs"
};
export {
  dotnetScriptAdapter
};
