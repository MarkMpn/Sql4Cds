import assert from "node:assert/strict";
import test from "node:test";
import { findInvalidFilter } from "../src/filterRecovery";
import type { ResultSetFilter } from "../src/protocol";

test("findInvalidFilter returns undefined with no filters", async () => {
  const filter = await findInvalidFilter(undefined, async () => true);
  assert.equal(filter, undefined);
});

test("findInvalidFilter identifies the failing filter by probing remaining filters", async () => {
  const filters = [
    { columnIndex: 0, operator: "contains", value: "ok" },
    { columnIndex: 1, operator: "greaterThan", value: "not-a-number" }
  ] as const;

  const invalid = await findInvalidFilter([...filters], async remaining => {
    const values = (remaining ?? []).map(filter => filter.value);
    return !values.includes("not-a-number");
  });

  assert.deepEqual(invalid, filters[1]);
});

test("findInvalidFilter returns undefined if no single filter removal resolves the issue", async () => {
  const filters: ResultSetFilter[] = [
    { columnIndex: 1, operator: "greaterThan", value: "bad-1" },
    { columnIndex: 2, operator: "lessThan", value: "bad-2" }
  ];

  const invalid = await findInvalidFilter(filters, async remaining => {
    const values = (remaining ?? []).map(filter => filter.value);
    return values.length === 0 || (values[0] !== "bad-1" && values[0] !== "bad-2");
  });

  assert.equal(invalid, undefined);
});