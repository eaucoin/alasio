// @ts-nocheck
/**
 * The Agent SDK's SessionStore conformance suite
 * (claude-agent-sdk-typescript examples/session-stores/shared/conformance.ts),
 * its thirteen checks case for case, for node:test. `makeStore` returns a
 * fresh, empty store for each case.
 */
import assert from "node:assert/strict";
import { describe, test } from "node:test";

export const KEY = { projectKey: "proj", sessionId: "sess" };
export const E = (type, extra = {}) => ({ type, ...extra });

/** Sorted-key JSON, so deep-equality ignores the key order JSONB returns. */
function canon(value) {
  return JSON.stringify(value, (_key, inner) =>
    inner && typeof inner === "object" && !Array.isArray(inner)
      ? Object.fromEntries(Object.entries(inner).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
      : inner,
  );
}

export const expectEntries = (actual, expected) => assert.equal(canon(actual), canon(expected));

export function sessionStoreConformance(makeStore, { skip = false } = {}) {
  describe("SessionStore conformance", { skip }, () => {
    test("append then load returns same entries in same order", async () => {
      const store = await makeStore();
      const entries = [E("a", { n: 1, nested: { x: [1, 2] } }), E("b", { n: 2 })];
      await store.append(KEY, entries);
      expectEntries(await store.load(KEY), entries);
    });

    test("load unknown key returns null", async () => {
      const store = await makeStore();
      assert.equal(await store.load(KEY), null);
      assert.equal(await store.load({ ...KEY, subpath: "subagents/a" }), null);
    });

    test("multiple append calls preserve call order", async () => {
      const store = await makeStore();
      await store.append(KEY, [E("a")]);
      await store.append(KEY, [E("b"), E("c")]);
      await store.append(KEY, [E("d")]);
      expectEntries(await store.load(KEY), [E("a"), E("b"), E("c"), E("d")]);
    });

    test("append([]) is a no-op", async () => {
      const store = await makeStore();
      await store.append(KEY, []);
      assert.equal(await store.load(KEY), null);
      await store.append(KEY, [E("a")]);
      await store.append(KEY, []);
      expectEntries(await store.load(KEY), [E("a")]);
    });

    test("subpath keys are stored independently of main", async () => {
      const store = await makeStore();
      await store.append(KEY, [E("main")]);
      await store.append({ ...KEY, subpath: "subagents/x" }, [E("sub")]);
      expectEntries(await store.load(KEY), [E("main")]);
      expectEntries(await store.load({ ...KEY, subpath: "subagents/x" }), [E("sub")]);
    });

    test("projectKey isolation", async () => {
      const store = await makeStore();
      const A = { projectKey: "A", sessionId: "s" };
      const B = { projectKey: "B", sessionId: "s" };
      await store.append(A, [E("a")]);
      await store.append(B, [E("b")]);
      expectEntries(await store.load(A), [E("a")]);
      expectEntries(await store.load(B), [E("b")]);
    });

    test("listSessions returns sessionIds for project", async () => {
      const store = await makeStore();
      await store.append({ projectKey: "P", sessionId: "s1" }, [E("a")]);
      await store.append({ projectKey: "P", sessionId: "s2" }, [E("b")]);
      await store.append({ projectKey: "Q", sessionId: "s3" }, [E("c")]);
      const listed = await store.listSessions("P");
      assert.deepEqual(listed.map((s) => s.sessionId).sort(), ["s1", "s2"]);
      assert.ok(listed.every((s) => s.mtime > 1e12));
      assert.deepEqual(await store.listSessions("never-seen"), []);
    });

    test("listSessions excludes subagent subpaths", async () => {
      const store = await makeStore();
      await store.append({ projectKey: "P", sessionId: "s1", subpath: "subagents/x" }, [E("sub")]);
      assert.ok(!(await store.listSessions("P")).map((s) => s.sessionId).includes("s1"));
    });

    test("delete main then load returns null", async () => {
      const store = await makeStore();
      await store.append(KEY, [E("a")]);
      await store.delete(KEY);
      assert.equal(await store.load(KEY), null);
      await store.delete({ projectKey: "x", sessionId: "never" });
    });

    test("delete main cascades to subkeys", async () => {
      const store = await makeStore();
      await store.append(KEY, [E("main")]);
      await store.append({ ...KEY, subpath: "subagents/a" }, [E("sa")]);
      await store.append({ ...KEY, subpath: "subagents/b" }, [E("sb")]);
      await store.append({ projectKey: "proj", sessionId: "other" }, [E("o")]);
      await store.append({ projectKey: "proj2", sessionId: "sess" }, [E("p2")]);
      await store.delete(KEY);
      assert.equal(await store.load(KEY), null);
      assert.equal(await store.load({ ...KEY, subpath: "subagents/a" }), null);
      assert.equal(await store.load({ ...KEY, subpath: "subagents/b" }), null);
      expectEntries(await store.load({ projectKey: "proj", sessionId: "other" }), [E("o")]);
      expectEntries(await store.load({ projectKey: "proj2", sessionId: "sess" }), [E("p2")]);
      assert.deepEqual(await store.listSubkeys(KEY), []);
    });

    test("delete with subpath removes only that subkey", async () => {
      const store = await makeStore();
      await store.append(KEY, [E("main")]);
      await store.append({ ...KEY, subpath: "subagents/a" }, [E("sa")]);
      await store.append({ ...KEY, subpath: "subagents/b" }, [E("sb")]);
      await store.delete({ ...KEY, subpath: "subagents/a" });
      expectEntries(await store.load(KEY), [E("main")]);
      assert.equal(await store.load({ ...KEY, subpath: "subagents/a" }), null);
      expectEntries(await store.load({ ...KEY, subpath: "subagents/b" }), [E("sb")]);
    });

    test("listSubkeys returns subpaths for the session", async () => {
      const store = await makeStore();
      await store.append({ ...KEY, subpath: "subagents/a" }, [E("sa")]);
      await store.append({ ...KEY, subpath: "subagents/b" }, [E("sb")]);
      await store.append({ projectKey: "proj", sessionId: "other", subpath: "subagents/c" }, [E("sc")]);
      assert.deepEqual((await store.listSubkeys(KEY)).sort(), ["subagents/a", "subagents/b"]);
    });

    test("listSubkeys excludes main transcript", async () => {
      const store = await makeStore();
      await store.append(KEY, [E("main")]);
      assert.deepEqual(await store.listSubkeys(KEY), []);
      assert.deepEqual(await store.listSubkeys({ projectKey: "x", sessionId: "never" }), []);
    });
  });
}
