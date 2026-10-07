/**
 * Queries the lake, read-only, from inside the stack:
 *
 *   node src/query.ts [--format table|csv|json] "<SQL>"
 *
 * `alasio lake [--format table|csv|json] "<SQL>"` runs it in the lake's container. The
 * lake is the default database, so its tables and views are named as `claude.entries`,
 * `codex.turns`, `otel.traces`, and so on (model.ts, otel.ts). The lake is attached
 * read-only, so a query can change nothing.
 */
import { loadConfig } from "./config.ts";
import { type Lake, LAKE, openLake, rows } from "./lake.ts";

const FORMATS = ["table", "csv", "json"] as const;

/** How query results are printed. */
export type Format = (typeof FORMATS)[number];

function isFormat(value: string | undefined): value is Format {
  return FORMATS.some((format) => format === value);
}

/** A value as text: JSON for structures, as it is otherwise. */
function text(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "object") return JSON.stringify(value, (_, inner) => (typeof inner === "bigint" ? inner.toString() : inner));
  return String(value);
}

/** `results` (an array of row objects) in `format`. */
export function format(results: readonly Readonly<Record<string, unknown>>[], format: Format): string {
  const [first] = results;
  const columns = first ? Object.keys(first) : [];
  if (format === "json") {
    return results.map((row) => JSON.stringify(row, (_, value) => (typeof value === "bigint" ? value.toString() : value))).join("\n");
  }
  if (format === "csv") {
    const field = (value: string) => (/[",\n\r]/.test(value) ? `"${value.replaceAll("\"", "\"\"")}"` : value);
    return [columns.map(field).join(","), ...results.map((row) => columns.map((column) => field(text(row[column]))).join(","))].join("\n");
  }
  const cells = results.map((row) => columns.map((column) => text(row[column]).replaceAll("\n", "\\n")));
  const widths = columns.map((column, index) => Math.max(column.length, ...cells.map((row) => (row[index] ?? "").length)));
  const line = (values: string[]) => values.map((value, index) => value.padEnd(widths[index] ?? 0)).join(" | ").trimEnd();
  return [line(columns), widths.map((width) => "-".repeat(width)).join("-+-"), ...cells.map(line), `(${results.length} rows)`].join("\n");
}

function parseArguments(argv: readonly string[]): { output: Format; sql: string } {
  let output: string | undefined = "table";
  const rest: (string | undefined)[] = [];
  for (let index = 0; index < argv.length; index += 1) {
    if (argv[index] === "--format") output = argv[++index];
    else rest.push(argv[index]);
  }
  const [sql] = rest;
  if (!isFormat(output) || rest.length !== 1 || sql === undefined) {
    throw new Error(`usage: query.ts [--format ${FORMATS.join("|")}] "<SQL>"`);
  }
  return { output, sql };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  let lake: Lake | undefined;
  try {
    const { output, sql } = parseArguments(process.argv.slice(2));
    lake = await openLake(loadConfig(), { readOnly: true });
    await lake.db.run(`use ${LAKE}`);
    console.log(format(await rows(lake.db, sql), output));
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  } finally {
    lake?.close();
  }
}
