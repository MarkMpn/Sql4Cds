import assert from "node:assert/strict";
import test from "node:test";
import { buildDataverseRecordUrl, extractDataverseRecordReference } from "../src/dataverseRecordLink";

test("extractDataverseRecordReference returns normalized values", () => {
  const reference = extractDataverseRecordReference({
    DataSource: " Sales ",
    LogicalName: " account ",
    Id: " 11111111-1111-1111-1111-111111111111 "
  });

  assert.deepEqual(reference, {
    dataSource: "Sales",
    logicalName: "account",
    id: "11111111-1111-1111-1111-111111111111"
  });
});

test("extractDataverseRecordReference rejects invalid shapes", () => {
  assert.equal(extractDataverseRecordReference(null), undefined);
  assert.equal(extractDataverseRecordReference({}), undefined);
  assert.equal(extractDataverseRecordReference({ DataSource: "Sales", LogicalName: "account" }), undefined);
  assert.equal(extractDataverseRecordReference({ DataSource: "Sales", LogicalName: "account", Id: 123 }), undefined);
});

test("buildDataverseRecordUrl appends entity record query", () => {
  const url = buildDataverseRecordUrl("https://org.crm.dynamics.com/", {
    dataSource: "Sales",
    logicalName: "account",
    id: "11111111-1111-1111-1111-111111111111"
  });

  assert.equal(
    url,
    "https://org.crm.dynamics.com/main.aspx?etn=account&pagetype=entityrecord&id=11111111-1111-1111-1111-111111111111"
  );
});