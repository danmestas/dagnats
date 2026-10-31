// Methodology: unit tests of the error mapping with literal bodies as the
// bridge writes them. The two 409 shapes are told apart by body, not
// status: bridge/resolve.go answers plain text "run cancelled" (http.Error
// appends a newline), bridge/journal.go answers {"code":"fenced"}. Positive
// space: each status/body pair maps to its class. Negative space: an
// unrecognised 409 or 410 is a plain BridgeError, a proxy's 503 is not
// NotProvisioned, and isTerminal is false for anything retryable.

import assert from "node:assert/strict";
import { test } from "node:test";
import {
  BridgeError,
  Committed,
  Fenced,
  NotProvisioned,
  RestartLimit,
  RunCancelled,
  Stale,
  StateTooLarge,
  BodyTooLarge,
  RecordTooLarge,
  errorFromResponse,
  isTerminal,
} from "../src/errors.ts";

const json = (code: string) => JSON.stringify({ error: `journal: ${code}`, code }) + "\n";

test("409 fenced and 409 run cancelled are different classes", () => {
  const fenced = errorFromResponse(409, json("fenced"));
  const cancelled = errorFromResponse(409, "run cancelled\n");
  assert.ok(fenced instanceof Fenced);
  assert.ok(cancelled instanceof RunCancelled);
  assert.ok(!(fenced instanceof RunCancelled));
  assert.ok(!(cancelled instanceof Fenced));
});

test("the 410 codes map to their classes", () => {
  assert.ok(errorFromResponse(410, json("stale")) instanceof Stale);
  assert.ok(errorFromResponse(410, json("committed")) instanceof Committed);
  assert.ok(errorFromResponse(410, json("restart_limit")) instanceof RestartLimit);
});

test("503 not_provisioned is NotProvisioned; a bare 503 is not", () => {
  assert.ok(errorFromResponse(503, json("not_provisioned")) instanceof NotProvisioned);
  const bare = errorFromResponse(503, "upstream unavailable");
  assert.ok(!(bare instanceof NotProvisioned));
  assert.equal(bare.status, 503);
});

test("anything else is a BridgeError carrying status, body and code", () => {
  const other409 = errorFromResponse(409, JSON.stringify({ code: "inputs_queued" }));
  assert.equal(other409.constructor, BridgeError);
  assert.equal(other409.status, 409);
  assert.equal(other409.code, "inputs_queued");
  const unknown410 = errorFromResponse(410, "gone\n");
  assert.equal(unknown410.constructor, BridgeError);
  assert.equal(unknown410.body, "gone\n");
  const plain409 = errorFromResponse(409, "worker_id is registered to another token\n");
  assert.equal(plain409.constructor, BridgeError);
  assert.equal(plain409.code, undefined);
});

test("every typed error is a BridgeError with its status", () => {
  const cases: Array<[BridgeError, number]> = [
    [errorFromResponse(409, json("fenced")), 409],
    [errorFromResponse(409, "run cancelled\n"), 409],
    [errorFromResponse(410, json("stale")), 410],
    [errorFromResponse(503, json("not_provisioned")), 503],
  ];
  for (const [err, status] of cases) {
    assert.ok(err instanceof BridgeError);
    assert.equal(err.status, status);
    assert.ok(err instanceof Error);
  }
});

test("isTerminal is true for the fenced/finished family and false for the retryable", () => {
  for (const err of [
    errorFromResponse(409, json("fenced")),
    errorFromResponse(409, "run cancelled\n"),
    errorFromResponse(410, json("stale")),
    errorFromResponse(410, json("committed")),
    errorFromResponse(410, json("restart_limit")),
  ]) {
    assert.equal(isTerminal(err), true, err.name);
  }
  assert.equal(isTerminal(errorFromResponse(500, "boom")), false);
  assert.equal(isTerminal(errorFromResponse(503, json("not_provisioned"))), false);
  assert.equal(isTerminal(errorFromResponse(409, JSON.stringify({ code: "inputs_queued" }))), false);
  assert.equal(isTerminal(new TypeError("fetch failed")), false);
  assert.equal(isTerminal(new DOMException("aborted", "AbortError")), false);
  assert.equal(isTerminal("string"), false);
});

test("the two 413 codes carry their sizes and are not terminal", () => {
  const state = errorFromResponse(413, '{"error":"x","code":"state_too_large","max_bytes":64,"bytes":65}\n');
  assert.ok(state instanceof StateTooLarge);
  assert.equal(state.maxBytes, 64);
  assert.equal(state.bytes, 65);
  const body = errorFromResponse(413, '{"error":"x","code":"body_too_large","max_bytes":8388608}');
  assert.ok(body instanceof BodyTooLarge);
  assert.equal(body.maxBytes, 8388608);
  const record = errorFromResponse(413, '{"error":"x","code":"record_too_large","max_bytes":1048576}');
  assert.ok(record instanceof RecordTooLarge);
  assert.equal(record.maxBytes, 1048576);
  assert.equal(isTerminal(record), false);
  assert.equal(isTerminal(state), false);
  assert.equal(isTerminal(body), false);
  assert.equal(errorFromResponse(413, "too big").constructor, BridgeError);
});
