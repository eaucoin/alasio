// A suite whose body throws as it is built, as one that asks the cluster before there is one does, beside a test that passes (../../runner.test.ts).
import { describe, test } from "node:test";

describe("a suite that builds", () => {
  test("passes", () => {});
});

describe("a suite that cannot be built", () => {
  throw new Error("asked before there was anything to ask");
});
