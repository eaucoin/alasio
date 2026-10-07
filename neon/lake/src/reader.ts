/**
 * The lake's reader: the lake opened read-only for the query endpoint (./endpoint.ts),
 * which answers Grafana's dashboards and alert rules and `alasio lake`, and the rows of
 * a query as JSON for them.
 *
 * It reads by construction, whatever the SQL it is given: the lake is attached
 * read-only, as role `lake_reader`, which may only select from the catalog, with the
 * object store's read-only identity; DuckDB may then reach no file but the lake's, and
 * its configuration is locked, so a query can undo neither. A query is one statement,
 * which runs for at most QUERY_TIMEOUT_MS and returns at most MAX_ROWS rows.
 */
import { type DuckDBConnection, type DuckDBResultReader, type DuckDBType, DuckDBTypeId, type Json } from "@duckdb/node-api";
import type pg from "pg";

import { type LakeAccess, READER_ROLE } from "./config.ts";
import { type Lake, LAKE, literal, openLake } from "./lake.ts";

/** The most rows a query returns: more than a panel shows, or a terminal. */
export const MAX_ROWS = 100_000;
/** How long a query may run. */
export const QUERY_TIMEOUT_MS = 30_000;

/** A query the reader does not run, or that failed: what it says is for whoever sent it. */
export class QueryRefused extends Error {}

/** A row as the reader answers it: its columns' values as JSON. */
export type AnsweredRow = Record<string, Json>;

/**
 * Lets the reader's role read the catalog, as the lake's role, which owns it: its
 * schema, every table in it, and every table it makes from now on, as DuckLake makes
 * one for each table's inlined rows. Idempotent.
 */
export async function grantReads(catalog: pg.ClientBase): Promise<void> {
  await catalog.query(`grant usage on schema ducklake to ${READER_ROLE}`);
  await catalog.query(`grant select on all tables in schema ducklake to ${READER_ROLE}`);
  await catalog.query(`alter default privileges in schema ducklake grant select on tables to ${READER_ROLE}`);
}

/**
 * Opens the lake for reading, as its reader: read-only, its tables named as from inside
 * it (`otel.traces`), its times UTC's, as the lake keeps them, and DuckDB confined to the
 * lake's files, its configuration locked.
 */
export async function openReader(config: LakeAccess): Promise<Lake> {
  const lake = await openLake(config, { readOnly: true });
  try {
    await lake.db.run(`use ${LAKE}`);
    await lake.db.run("set TimeZone = 'UTC'");
    await lake.db.run(`set allowed_directories = [${literal(config.dataPath)}]`);
    await lake.db.run("set enable_external_access = false");
    await lake.db.run("set lock_configuration = true");
    return lake;
  } catch (error) {
    lake.close();
    throw error;
  }
}

/** `value` of `type`, as the reader answers it: a map, list or struct as JSON text, a timestamp as RFC 3339, a number as a number where it fits. */
function answered(value: Json, type: DuckDBType): Json {
  if (value === null) return null;
  switch (type.typeId) {
    case DuckDBTypeId.MAP:
    case DuckDBTypeId.LIST:
    case DuckDBTypeId.ARRAY:
    case DuckDBTypeId.STRUCT:
    case DuckDBTypeId.UNION:
      return JSON.stringify(nested(value, type));
    case DuckDBTypeId.TIMESTAMP:
    case DuckDBTypeId.TIMESTAMP_S:
    case DuckDBTypeId.TIMESTAMP_MS:
    case DuckDBTypeId.TIMESTAMP_NS:
      // DuckDB's text of a timestamp, which is UTC's time.
      return `${String(value).replace(" ", "T")}Z`;
    case DuckDBTypeId.TIMESTAMP_TZ: {
      // As `2026-10-07 04:09:17+00`: its offset in hours, and in minutes too where it has them.
      const text = String(value).replace(" ", "T");
      return /[+-]\d\d$/u.test(text) ? `${text}:00` : text;
    }
    case DuckDBTypeId.BIGINT:
    case DuckDBTypeId.UBIGINT:
    case DuckDBTypeId.HUGEINT:
    case DuckDBTypeId.UHUGEINT:
    case DuckDBTypeId.DECIMAL: {
      const number = Number(value);
      return Number.isSafeInteger(number) || (type.typeId === DuckDBTypeId.DECIMAL && Number.isFinite(number)) ? number : value;
    }
    default:
      return value;
  }
}

/** A nested value as JSON: a map as an object of its keys, as its key's text. */
function nested(value: Json, type: DuckDBType): Json {
  if (value === null) return null;
  if (type.typeId === DuckDBTypeId.MAP && Array.isArray(value)) {
    // A map's JSON is its entries, each { key, value }.
    const entries = value as { key: Json; value: Json }[];
    return Object.fromEntries(entries.map((entry) => [typeof entry.key === "string" ? entry.key : JSON.stringify(entry.key), nested(entry.value, type.valueType)]));
  }
  if ((type.typeId === DuckDBTypeId.LIST || type.typeId === DuckDBTypeId.ARRAY) && Array.isArray(value)) {
    return value.map((item) => nested(item, type.valueType));
  }
  if (type.typeId === DuckDBTypeId.STRUCT && typeof value === "object" && !Array.isArray(value)) {
    return Object.fromEntries(type.entryNames.map((name, index) => [name, nested(value[name] ?? null, type.entryTypes[index] ?? type)]));
  }
  return value;
}

/** The rows `reader` read, as the reader answers them, each column by its name (made unique). */
function answeredRows(reader: DuckDBResultReader): AnsweredRow[] {
  const names = reader.deduplicatedColumnNames();
  const types = reader.columnTypes();
  return reader.getRowsJson().map((row) => Object.fromEntries(names.map((name, index) => [name, answered(row[index] ?? null, types[index]!)])));
}

/**
 * The rows of `sql` on the reader's connection `db`: one statement, which is
 * interrupted past QUERY_TIMEOUT_MS, and refused past MAX_ROWS rows.
 */
export async function answer(db: DuckDBConnection, sql: string, { maxRows = MAX_ROWS, timeoutMs = QUERY_TIMEOUT_MS } = {}): Promise<AnsweredRow[]> {
  let statements;
  try {
    statements = await db.extractStatements(sql);
  } catch (error) {
    throw new QueryRefused(error instanceof Error ? error.message : String(error));
  }
  if (statements.count !== 1) throw new QueryRefused(`a query is one statement, not ${statements.count}`);
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    db.interrupt();
  }, timeoutMs);
  try {
    const reader = await db.streamAndReadUntil(sql, maxRows + 1);
    if (reader.currentRowCount > maxRows) throw new QueryRefused(`the query returns more than ${maxRows} rows: aggregate them, or limit them`);
    return answeredRows(reader);
  } catch (error) {
    if (error instanceof QueryRefused) throw error;
    throw new QueryRefused(timedOut ? `the query ran past ${timeoutMs / 1000} s` : error instanceof Error ? error.message : String(error));
  } finally {
    clearTimeout(timer);
  }
}
