// @ts-nocheck
/**
 * Queries the lake, read-only, from inside the stack:
 *
 *   node src/query.ts [--format table|csv|json] "<SQL>"
 *
 * alasio's `npm run lake -- "<SQL>"` runs it in the lake's container. The lake is the
 * default database, so its tables and views are named as `claude.entries`,
 * `codex.turns`, and so on (model.ts). The lake is attached read-only, so a
 * query can change nothing.
 */
import { loadConfig } from "./config.ts";
import { LAKE, openLake, rows } from "./lake.ts";

const FORMATS = ["table", "csv", "json"];

/** A value as text: JSON for structures, as it is otherwise. */
function text(value) {
  if (value === null || value === undefined) return "";
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "object") return JSON.stringify(value, (_, inner) => (typeof inner === "bigint" ? inner.toString() : inner));
  return String(value);
}

/** `results` (an array of row objects) in `format`. */
export function format(results, format) {
  const columns = results.length ? Object.keys(results[0]) : [];
  if (format === "json") {
    return results.map((row) => JSON.stringify(row, (_, value) => (typeof value === "bigint" ? value.toString() : value))).join("\n");
  }
  if (format === "csv") {
    const field = (value) => (/[",\n\r]/.test(value) ? `"${value.replaceAll("\"", "\"\"")}"` : value);
    return [columns.map(field).join(","), ...results.map((row) => columns.map((column) => field(text(row[column]))).join(","))].join("\n");
  }
  const cells = results.map((row) => columns.map((column) => text(row[column]).replaceAll("\n", "\\n")));
  const widths = columns.map((column, index) => Math.max(column.length, ...cells.map((row) => row[index].length)));
  const line = (values) => values.map((value, index) => value.padEnd(widths[index])).join(" | ").trimEnd();
  return [line(columns), widths.map((width) => "-".repeat(width)).join("-+-"), ...cells.map(line), `(${results.length} rows)`].join("\n");
}

function parseArguments(argv) {
  let output = "table";
  const rest = [];
  for (let index = 0; index < argv.length; index += 1) {
    if (argv[index] === "--format") output = argv[++index];
    else rest.push(argv[index]);
  }
  if (!FORMATS.includes(output) || rest.length !== 1) {
    throw new Error(`usage: query.ts [--format ${FORMATS.join("|")}] "<SQL>"`);
  }
  return { output, sql: rest[0] };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  let lake;
  try {
    const { output, sql } = parseArguments(process.argv.slice(2));
    lake = await openLake(loadConfig(), { readOnly: true });
    await lake.db.run(`use ${LAKE}`);
    console.log(format(await rows(lake.db, sql), output));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  } finally {
    lake?.close();
  }
}
