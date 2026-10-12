import assert from "node:assert/strict";
import test from "node:test";
import { PendingRequests } from "../src/pendingRequests";

test("pending requests resolve once and ignore late notifications", async () => {
  const requests = new PendingRequests<number>();
  const completion = requests.wait("connection", 100, "timed out");
  assert.equal(requests.resolve("connection", 42), true);
  assert.equal(requests.resolve("connection", 99), false);
  assert.equal(await completion, 42);
});

test("a newer request rejects the previous request for the same key", async () => {
  const requests = new PendingRequests<number>();
  const first = requests.wait("connection", 100, "timed out");
  const firstRejected = assert.rejects(first, /Superseded by a newer request/);
  const second = requests.wait("connection", 100, "timed out");
  await firstRejected;
  requests.resolve("connection", 2);
  assert.equal(await second, 2);
});

test("pending requests reject with their timeout guidance", async () => {
  const requests = new PendingRequests<number>();
  await assert.rejects(requests.wait("connection", 10, "Connection timed out"), /Connection timed out/);
  assert.equal(requests.resolve("connection", 1), false);
});

test("rejectAll releases every in-flight operation", async () => {
  const requests = new PendingRequests<number>();
  const first = requests.wait("first", 100, "timed out");
  const second = requests.wait("second", 100, "timed out");
  const assertions = [assert.rejects(first, /service stopped/), assert.rejects(second, /service stopped/)];
  requests.rejectAll(new Error("service stopped"));
  await Promise.all(assertions);
});

test("request timeout covers a transport acknowledgement that never arrives", async () => {
  const requests = new PendingRequests<number>();
  await assert.rejects(requests.run("hung", 10, "timed out", () => new Promise(() => {})), /timed out/);
});

test("notification failure is observed while acknowledgement is pending", async () => {
  const requests = new PendingRequests<number>();
  await assert.rejects(requests.run("early", 100, "timed out", async () => {
    requests.reject("early", new Error("Authentication failed"));
    await new Promise(resolve => setTimeout(resolve, 20));
  }), /Authentication failed/);
});

test("dispatch failures reject the operation and clear its deadline", async () => {
  const requests = new PendingRequests<number>();
  await assert.rejects(requests.run("failed", 100, "timed out", async () => { throw new Error("Transport closed"); }), /Transport closed/);
  assert.equal(requests.resolve("failed", 1), false);
});

test("a late transport failure cannot reject a retry using the same key", async () => {
  const requests = new PendingRequests<number>();
  let fail!: (reason: Error) => void;
  await assert.rejects(requests.run("retry", 10, "timed out", () => new Promise((_, reject) => { fail = reject; })), /timed out/);
  const retry = requests.run("retry", 100, "timed out", async () => {});
  fail(new Error("Old transport failed"));
  await new Promise(resolve => setTimeout(resolve, 0));
  assert.equal(requests.resolve("retry", 42), true);
  assert.equal(await retry, 42);
});
