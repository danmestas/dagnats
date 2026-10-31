// Methodology: unit tests of the journal over an in-memory log, for the
// behaviour the fixtures do not state in terms of this API: outcomes,
// counters on open, canonical input, and the wrapper-facing shape. The
// rules themselves are proved by test/replay-fixtures.test.ts.

import assert from "node:assert/strict";
import { test } from "node:test";
import { CALLS_MAX, INTERRUPTED, Journal, canonical } from "../journal/journal.ts";
import { MemoryLog } from "../journal/memory.ts";
import { ReplayMismatch, type Task } from "../journal/types.ts";

/** The model output type is the caller's; a text reply stands in for it. */
interface Msg {
  content: string;
}
const faux = (content: string): Msg => ({ content });

const task = (delivered = 1): Task => ({
  run_id: "r1",
  step_id: "agent",
  iteration: 0,
  attempt: 1,
  dispatch_nonce: "n1",
  delivered,
  stream_seq: 7,
});

/** A redelivery of the same attempt on the same records. */
const redeliver = (log: MemoryLog) => new MemoryLog(task(2), log.records);

test("a model response is recorded once and reused at its position", async () => {
  const log = new MemoryLog(task());
  const first = await Journal.open<Msg>(log, task());
  let calls = 0;
  const a = await first.model(async () => {
    calls++;
    return faux("hello");
  });
  assert.equal(a.outcome, "requested");

  const second = await Journal.open<Msg>(redeliver(log), task(2));
  const b = await second.model(async () => {
    calls++;
    return faux("never");
  });
  assert.equal(b.outcome, "reused");
  assert.equal(calls, 1);
  assert.deepEqual(b.message, a.message);
  assert.deepEqual(second.recorded, { models: 1, intents: 0, results: 0 });
});

test("a finished tool result is reused, the tool is not run again", async () => {
  const log = new MemoryLog(task());
  const first = await Journal.open<Msg>(log, task());
  let runs = 0;
  await first.tool("echo", { s: "x" }, "unsafe", async () => {
    runs++;
    return { output: "x" };
  });

  const second = await Journal.open<Msg>(redeliver(log), task(2));
  const out = await second.tool("echo", { s: "x" }, "unsafe", async () => {
    runs++;
    return { output: "y" };
  });
  assert.equal(out.outcome, "reused");
  assert.equal(out.result.output, "x");
  assert.equal(runs, 1);
});

test("an unsafe intent with no result comes back interrupted", async () => {
  const log = new MemoryLog(task());
  await log.append({
    type: "tool.intent",
    data: { name: "bash", input: { cmd: "rm" }, call: 0, key: "r1.agent.0.0", replay: "unsafe" },
  });
  const second = await Journal.open<Msg>(redeliver(log), task(2));
  let runs = 0;
  const out = await second.tool("bash", { cmd: "rm" }, "unsafe", async () => {
    runs++;
    return { output: "ran" };
  });
  assert.equal(runs, 0);
  assert.equal(out.outcome, "interrupted");
  assert.equal(out.result.interrupted, true);
  assert.equal(out.result.error, INTERRUPTED);
});

test("the wrapper's tool call id is stored on the intent and ignored by replay", async () => {
  const log = new MemoryLog(task());
  const j = await Journal.open<Msg>(log, task());
  await j.tool("read", { path: "a" }, "safe", async () => ({ output: "c" }), "call_9");
  const intent = (await log.view())[0]!.data as { tool_call_id?: string };
  assert.equal(intent.tool_call_id, "call_9");
  const again = await Journal.open<Msg>(redeliver(log), task(2));
  const out = await again.tool("read", { path: "a" }, "safe", async () => ({ output: "d" }), "other");
  assert.equal(out.outcome, "reused");
});

test("a different call at a recorded position is a replay mismatch", async () => {
  const log = new MemoryLog(task());
  await log.append({
    type: "tool.intent",
    data: { name: "read", input: { path: "a" }, call: 0, key: "r1.agent.0.0", replay: "safe" },
  });
  const j = await Journal.open<Msg>(redeliver(log), task(2));
  await assert.rejects(
    j.tool("read", { path: "b" }, "safe", async () => ({ output: "" })),
    ReplayMismatch,
  );
});

test("canonical JSON ignores key order; an absent input equals only an absent input", () => {
  assert.equal(canonical({ b: 1, a: { d: 2, c: [3, { f: 1, e: 0 }] } }), canonical({ a: { c: [3, { e: 0, f: 1 }], d: 2 }, b: 1 }));
  assert.equal(canonical(undefined), "");
  assert.notEqual(canonical(undefined), canonical({}));
});

test("model and tool positions count separately", async () => {
  const log = new MemoryLog(task());
  const j = await Journal.open<Msg>(log, task());
  await j.model(async () => faux("m0"));
  await j.tool("t", {}, "safe", async () => ({ output: 0 }));
  await j.model(async () => faux("m1"));
  await j.tool("t", {}, "safe", async () => ({ output: 1 }));
  const calls = (await log.view()).map((r) => `${r.type}:${(r.data as { call: number }).call}`);
  assert.deepEqual(calls, [
    "model.response:0",
    "tool.intent:0",
    "tool.result:0",
    "model.response:1",
    "tool.intent:1",
    "tool.result:1",
  ]);
});

test("the newest intent recorded for a call wins, as in the Go SDK", async () => {
  const log = new MemoryLog(task());
  for (const name of ["read", "write"]) {
    await log.append({
      type: "tool.intent",
      data: { name, input: { p: 1 }, call: 0, key: "r1.agent.0.0", replay: "safe" },
    });
  }
  const j = await Journal.open<Msg>(redeliver(log), task(2));
  const out = await j.tool("write", { p: 1 }, "safe", async () => ({ output: "w" }));
  assert.equal(out.outcome, "ran");
  await assert.rejects(
    (await Journal.open<Msg>(redeliver(log), task(2))).tool("read", { p: 1 }, "safe", async () => ({})),
    ReplayMismatch,
  );
});

test("an iteration is bounded at CALLS_MAX tool calls and CALLS_MAX model calls", async () => {
  const tools = await Journal.open<Msg>(new MemoryLog(task()), task());
  for (let i = 0; i < CALLS_MAX; i++) await tools.tool("t", {}, "safe", async () => ({}));
  await assert.rejects(tools.tool("t", {}, "safe", async () => ({})), /more than 10000 tool calls/);

  const models = await Journal.open<Msg>(new MemoryLog(task()), task());
  for (let i = 0; i < CALLS_MAX; i++) await models.model(async () => faux("m"));
  let requested = false;
  await assert.rejects(
    models.model(async () => {
      requested = true;
      return faux("over");
    }),
    /more than 10000 model calls/,
  );
  assert.equal(requested, false, "the bound is checked before the request is made");
});
