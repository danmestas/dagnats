// Methodology: integration tests against a real in-process dagnats
// dev-mode server (test/devserver), one per test file, the way
// sdk/httpclient's tests run one. Every wait is bounded. Positive space:
// a polled task completes its run, a checkpoint round-trips, the journal
// view carries the claim and the records of an earlier delivery. Negative
// space: no checkpoint reads as undefined and never throws, a fenced
// worker and a cancelled run raise different typed errors from every
// action (journal.append and journal.view included, not only complete),
// and an abort ends a long poll without waiting out wait_ms.

import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { BridgeClient, BridgeLog, Fenced, Journal, RunCancelled, TaskContext, isTerminal } from "../src/index.ts";
import type { PollItem } from "../src/index.ts";
import {
  cancelRun,
  publishTask,
  startDevServer,
  startOneStepRun,
  waitForStatus,
  type DevServer,
} from "./harness.ts";

let srv: DevServer;
let client: BridgeClient;
let secondClient: BridgeClient;

before(async () => {
  srv = await startDevServer();
  client = new BridgeClient(srv.url, "");
  secondClient = new BridgeClient(srv.secondURL, "");
});
after(async () => {
  await srv.stop();
});

let typeSeq = 0;
/** A task type no other test polls, so tests do not take each other's tasks. */
const uniqueType = () => `ts-${++typeSeq}-${Date.now()}`;

async function pollOne(c: BridgeClient, taskType: string): Promise<PollItem> {
  const items = await c.poll({ worker: "w", task_types: [taskType], max: 1, wait_ms: 5000 });
  assert.equal(items.length, 1, "expected one task within 5s");
  return items[0]!;
}

test("poll then complete finishes the run", async () => {
  const type = uniqueType();
  const runId = await startOneStepRun(srv, type, { n: 1 });
  const item = await pollOne(client, type);
  assert.equal(item.run_id, runId);
  assert.equal(item.step_id, "step");
  assert.equal(item.task_type, type);
  assert.equal(item.task_id, `${runId}.step`);
  assert.deepEqual(item.input, { n: 1 });
  assert.equal(item.delivered, 1);
  assert.ok(item.stream_seq > 0);

  const ctx = new TaskContext(client, item);
  assert.equal(ctx.task.run_id, runId);
  await ctx.complete({ ok: true });
  await waitForStatus(srv, runId, "completed");
  // The task is gone from the bridge: a second resolve is a plain 404, not terminal.
  await assert.rejects(ctx.heartbeat(), (e: unknown) => {
    assert.equal((e as { status: number }).status, 404);
    assert.equal(isTerminal(e), false);
    return true;
  });
});

test("fail with a failure_type fails the run", async () => {
  const type = uniqueType();
  const runId = await startOneStepRun(srv, type);
  const ctx = new TaskContext(client, await pollOne(client, type));
  await ctx.fail({ error: "no good", failure_type: "non_retriable" });
  await waitForStatus(srv, runId, "failed");
});

test("checkpoint round-trips, and no checkpoint reads as undefined", async () => {
  const type = uniqueType();
  await startOneStepRun(srv, type);
  const ctx = new TaskContext(client, await pollOne(client, type));
  assert.equal(await ctx.loadCheckpoint(), undefined);
  await ctx.checkpoint({ step: 3, notes: ["a", "b"] });
  assert.deepEqual(await ctx.loadCheckpoint(), { step: 3, notes: ["a", "b"] });
  await ctx.heartbeat();
  await ctx.stream({ delta: "hi" });
  await ctx.complete(null);
});

test("logs are accepted for a claimed task", async () => {
  const type = uniqueType();
  await startOneStepRun(srv, type);
  const item = await pollOne(client, type);
  await client.logs(item.task_id, ["line one", { stream: "err", data: "line two" }]);
  await new TaskContext(client, item).complete(null);
});

test("a cancelled run raises RunCancelled, not Fenced", async () => {
  const type = uniqueType();
  const runId = await startOneStepRun(srv, type);
  const ctx = new TaskContext(client, await pollOne(client, type));
  await cancelRun(srv, runId);
  await waitForStatus(srv, runId, "cancelled");
  const err = await ctx.heartbeat().then(
    () => undefined,
    (e: unknown) => e,
  );
  assert.ok(err instanceof RunCancelled, String(err));
  assert.ok(!(err instanceof Fenced));
  assert.equal(isTerminal(err), true);
});

test("journal.view carries the claim; journal.append answers a seq", async () => {
  const type = uniqueType();
  const runId = await startOneStepRun(srv, type);
  const item = await pollOne(client, type);
  const ctx = new TaskContext(client, item);

  const view = await ctx.journal.view();
  assert.equal(view.claim.iteration, 0);
  assert.equal(view.claim.delivered, 1);
  assert.equal(view.claim.stream_seq, item.stream_seq);
  assert.ok(view.claim.dispatch_nonce.length > 0);
  assert.equal(item.dispatch_nonce, view.claim.dispatch_nonce, "poll and claim agree");
  assert.equal(ctx.task.dispatch_nonce, view.claim.dispatch_nonce);
  assert.deepEqual(view.records, []);

  const first = await ctx.journal.append("entry", { w: 1 });
  const second = await ctx.journal.append("entry", { w: 2 });
  assert.ok(second.seq > first.seq);
  await ctx.complete(null);
  await waitForStatus(srv, runId, "completed");
});

test("a redelivery fences the first worker from every journal action and sees its records", async () => {
  const type = uniqueType();
  const payload = { run_id: `fence-${type}`, step_id: "step", dispatch_nonce: "n1" };
  await publishTask(srv, type, payload);
  const first = new TaskContext(client, await pollOne(client, type));
  assert.equal(first.task.dispatch_nonce, "n1");
  await first.journal.append("entry", { w: 1 });

  await publishTask(srv, type, payload); // a dispatch resend: same identity, fresh message
  const second = new TaskContext(secondClient, await pollOne(secondClient, type));
  const view = await second.journal.view();
  assert.equal(view.claim.dispatch_nonce, "n1");
  const entries = view.records.filter((r) => r.type === "entry").map((r) => r.data);
  assert.deepEqual(entries, [{ w: 1 }], "the successor's view holds the first delivery's record");

  for (const attempt of [
    () => first.journal.append("entry", { w: "late" }),
    () => first.journal.view(),
    () => first.complete({ late: true }),
  ]) {
    const err = await attempt().then(
      () => undefined,
      (e: unknown) => e,
    );
    assert.ok(err instanceof Fenced, String(err));
    assert.equal(isTerminal(err), true);
  }
  await second.complete({ ok: 2 });
});

test("a Journal over BridgeLog records and replays through the bridge", async () => {
  const type = uniqueType();
  const payload = { run_id: `replay-${type}`, step_id: "step", dispatch_nonce: "n1" };
  await publishTask(srv, type, payload);
  const first = new TaskContext(client, await pollOne(client, type));
  const j1 = await Journal.open<{ text: string }>(new BridgeLog(first.journal), first.task);
  const m1 = await j1.model(async () => ({ text: "hello" }));
  assert.equal(m1.outcome, "requested");
  const t1 = await j1.tool("echo", { a: 1 }, "safe", async () => ({ output: "x" }));
  assert.equal(t1.outcome, "ran");

  await publishTask(srv, type, payload);
  const second = new TaskContext(secondClient, await pollOne(secondClient, type));
  const j2 = await Journal.open<{ text: string }>(new BridgeLog(second.journal), second.task);
  let reran = false;
  const m2 = await j2.model(async () => {
    reran = true;
    return { text: "never" };
  });
  const t2 = await j2.tool("echo", { a: 1 }, "safe", async () => {
    reran = true;
    return { output: "y" };
  });
  assert.equal(m2.outcome, "reused");
  assert.deepEqual(m2.message, { text: "hello" });
  assert.equal(t2.outcome, "reused");
  assert.equal(t2.result.output, "x");
  assert.equal(reran, false);
  await second.complete(null);
});

test("an abort ends a long poll without waiting out wait_ms", async () => {
  const controller = new AbortController();
  const started = Date.now();
  const pending = client.poll({ worker: "w", task_types: [uniqueType()], wait_ms: 30_000 }, { signal: controller.signal });
  setTimeout(() => controller.abort(), 200);
  await assert.rejects(pending, (e: unknown) => (e as Error).name === "AbortError");
  assert.ok(Date.now() - started < 5000, "the abort did not take effect promptly");
});

test("connect holds the heartbeat stream open until closed", async () => {
  const conn = client.connect({ worker: `w-${Date.now()}`, task_types: [uniqueType()] });
  await conn.ready;
  const settled = await Promise.race([
    conn.closed.then(
      () => "closed",
      () => "closed",
    ),
    new Promise((r) => setTimeout(() => r("open"), 500)),
  ]);
  assert.equal(settled, "open");
  conn.close();
  await assert.rejects(conn.closed, /closed/);
});
