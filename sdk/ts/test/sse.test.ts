// Methodology: unit tests of the hand-written SSE parser the connect
// heartbeat uses. Chunks are cut at awkward places (inside a field, between
// CR and LF) because a network read has no respect for event boundaries.

import assert from "node:assert/strict";
import { test } from "node:test";
import { SSEParser } from "../src/sse.ts";

test("parses the bridge heartbeat event", () => {
  const p = new SSEParser();
  assert.deepEqual(p.push("event: heartbeat\ndata: \n\n"), [{ event: "heartbeat", data: "" }]);
});

test("an event split across chunks is emitted once, when complete", () => {
  const p = new SSEParser();
  assert.deepEqual(p.push("event: hear"), []);
  assert.deepEqual(p.push("tbeat\ndata: x"), []);
  assert.deepEqual(p.push("\n\nevent: heartbeat\n\n"), [
    { event: "heartbeat", data: "x" },
    { event: "heartbeat", data: "" },
  ]);
});

test("CRLF line endings, comments and multi-line data", () => {
  const p = new SSEParser();
  assert.deepEqual(p.push(": keepalive\r\n\r\n"), []);
  assert.deepEqual(p.push("data: a\r\ndata: b\r\n\r\n"), [{ event: "message", data: "a\nb" }]);
});
