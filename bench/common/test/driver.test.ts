import assert from "node:assert/strict";
import { test } from "node:test";

import { endpointOf, timedFetch } from "../driver/driver.ts";

test("the client notes when it first asked, of every attempt, and when its initialize was last answered, not a refused one's nor another request's", async () => {
  const asked = { first_request_ns: null, initialize_answered_ns: null };
  const statuses = [503, 200, 202];
  const fetched = timedFetch(asked, async () => new Response("{}", { status: statuses.shift() ?? 200 }));
  const initialize = { method: "POST", body: JSON.stringify({ jsonrpc: "2.0", id: 0, method: "initialize", params: {} }) };
  await fetched("http://gateway/mcp", initialize);
  const first = asked.first_request_ns;
  assert.ok(first !== null);
  assert.equal(asked.initialize_answered_ns, null);
  await fetched("http://gateway/mcp", initialize);
  const answered = asked.initialize_answered_ns;
  assert.ok(answered !== null && BigInt(answered) >= BigInt(first));
  await fetched("http://gateway/mcp", { method: "POST", body: JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" }) });
  assert.deepEqual(asked, { first_request_ns: first, initialize_answered_ns: answered });
});

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
