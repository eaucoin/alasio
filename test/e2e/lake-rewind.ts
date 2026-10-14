/**
 * The lake put back to before what the intake's group was told, as a restore of its catalog
 * puts it: run by node in the intake's container (`node --input-type=module-typescript -` from
 * /opt/lake, where the lake's code is), as the intake, with its configuration. It deletes the
 * spans of the run its argument names, and every offset the lake holds of otel.spans, under
 * the lake's write lock, so that what the lake held of the topic is the log's to give it again.
 */
import { loadIntakeConfig } from "../../neon/lake/src/config.ts";
import { LAKE, openLake, transaction, writeLock } from "../../neon/lake/src/lake.ts";

const [run = ""] = process.argv.slice(2);
const config = loadIntakeConfig();
writeLock(config.catalog);
const lake = await openLake(config);
try {
  await transaction(lake.db, async () => {
    await lake.db.run(`delete from ${LAKE}.otel.traces where SpanAttributes['e2e.run'] = $1`, [run]);
    await lake.db.run(`delete from ${LAKE}.otel.intake_offsets where Topic = 'otel.spans'`);
  });
} finally {
  lake.close();
}
console.log(JSON.stringify({ rewound: run }));
// The write lock's connection holds the process open.
process.exit(0);
