// A suite whose body's promise rejects, its tests never registered (../../runner.test.ts).
import { describe, test } from "node:test";

describe("a suite whose body rejects", async () => {
  await Promise.resolve();
  throw new Error("rejected as it was built");
  test("never registered", () => {});
});
