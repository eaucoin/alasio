/**
 * A reporter for node's test runner, beside the one that says the run (package.json's test
 * scripts): it fails the run where a suite failed, as Node's runner does not. Node 24 counts a
 * suite whose body throws as it is built, or whose promise rejects, among its suites and not its
 * failures, says "fail 0", and exits 0, however it reports: a file whose suite could not be built
 * would pass, its tests never run. Each suite that failed is said, with why, and the run's exit
 * code set to 1, which the runner leaves as it is where nothing else failed; one that failed as a
 * test of it did is the runner's to fail, as it does.
 */
import type { TestEvent } from "node:test/reporters";

export default async function* suiteFailures(source: AsyncIterable<TestEvent>): AsyncGenerator<string> {
  for await (const event of source) {
    if (event.type !== "test:fail" || event.data.details.type !== "suite") continue;
    const { error } = event.data.details;
    // One that failed as a test of it did, the runner counts that test's failure already.
    if ((error as Error & { readonly failureType?: string }).failureType === "subtestsFailed") continue;
    process.exitCode = 1;
    const cause = error.cause instanceof Error ? error.cause.message : String(error.cause ?? error.message);
    yield `the suite ${JSON.stringify(event.data.name)}${event.data.file ? ` of ${event.data.file}` : ""} failed, which fails the run: ${cause}\n`;
  }
}
