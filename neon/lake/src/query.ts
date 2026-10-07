/**
 * Queries the lake, read-only, from inside the stack:
 *
 *   node src/query.ts [--format table|csv|json] "<SQL>"
 *
 * `alasio lake [--format table|csv|json] "<SQL>"` runs it in the lake's query container,
 * where it asks the query endpoint beside it (./endpoint.ts), as Grafana does: the lake
 * is its reader's, so a query can change nothing, and its tables are named as from
 * inside it, as `claude.entries`, `codex.turns`, `otel.traces`, and so on (model.ts,
 * otel.ts).
 */
import { loadEndpointConfig } from "./config.ts";
import type { AnsweredRow } from "./reader.ts";

const FORMATS = ["table", "csv", "json"] as const;

/** How query results are printed. */
export type Format = (typeof FORMATS)[number];

function isFormat(value: string | undefined): value is Format {
  return FORMATS.some((format) => format === value);
}

/** A value as text: as it is, nothing for null. */
function text(value: AnsweredRow[string]): string {
  return value === null ? "" : typeof value === "object" ? JSON.stringify(value) : String(value);
}

/** `results`, rows as the endpoint answers them, in `format`. */
export function format(results: readonly AnsweredRow[], format: Format): string {
  const [first] = results;
  const columns = first ? Object.keys(first) : [];
  if (format === "json") return results.map((row) => JSON.stringify(row)).join("\n");
  if (format === "csv") {
    const field = (value: string) => (/[",\n\r]/.test(value) ? `"${value.replaceAll("\"", "\"\"")}"` : value);
    return [columns.map(field).join(","), ...results.map((row) => columns.map((column) => field(text(row[column] ?? null))).join(","))].join("\n");
  }
  const cells = results.map((row) => columns.map((column) => text(row[column] ?? null).replaceAll("\n", "\\n")));
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
  try {
    const { output, sql } = parseArguments(process.argv.slice(2));
    const { port, token } = loadEndpointConfig();
    const response = await fetch(`http://127.0.0.1:${port}/query`, { method: "POST", headers: { authorization: `Bearer ${token}` }, body: sql });
    if (!response.ok) throw new Error((await response.text()).trim());
    // The endpoint answers rows.
    console.log(format((await response.json()) as AnsweredRow[], output));
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
