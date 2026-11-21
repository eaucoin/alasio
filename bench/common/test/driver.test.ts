import assert from "node:assert/strict";
import { test } from "node:test";

import { endpointOf } from "../driver/driver.ts";

test("the driver's endpoint: its URL and each header the arguments name, and with --header-stdin one more, the first line of its input, in no argument", () => {
  assert.deepEqual(endpointOf({ url: "http://gateway/mcp", header: ["X-One: 1"] }), { url: "http://gateway/mcp", headers: { "X-One": "1" } });
  assert.deepEqual(
    endpointOf({ url: "http://gateway/mcp", header: ["X-One: 1"], "header-stdin": true }, "Authorization: Bearer abc.def\nwhat follows\n"),
    { url: "http://gateway/mcp", headers: { "X-One": "1", Authorization: "Bearer abc.def" } },
  );
  // Its input read only where it is told to.
  assert.deepEqual(endpointOf({ url: "http://gateway/mcp" }, "Authorization: Bearer abc.def\n"), { url: "http://gateway/mcp", headers: {} });
  assert.throws(() => endpointOf({ url: "http://gateway/mcp", "header-stdin": true }, ""), /is not 'Name: value'/u);
});
