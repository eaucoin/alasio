// A test that never ends, and a suite whose hook never ends, each holding the process open as it waits (../../runner.test.ts).
import { before, describe, test } from "node:test";

/** What never settles, a timer holding the process open meanwhile, as a call that is never answered does. */
const never = () => new Promise<void>(() => void setInterval(() => {}, 1_000));

test("a test that never ends", never);

describe("a suite whose hook never ends", () => {
  before(never);
  test("after the hook", () => {});
});
