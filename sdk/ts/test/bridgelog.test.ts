// Methodology: unit tests of BridgeLog's append serialization against a
// fake journal whose appends take a controllable time. Positive space:
// appends issued without awaiting land in call order with at most one in
// flight. Negative space: a failed append neither reorders nor drops the
// ones queued behind it, and each caller gets its own outcome.

import assert from "node:assert/strict";
import { test } from "node:test";
import { BridgeLog } from "../journal/index.ts";
import type { ViewRecord } from "../journal/index.ts";

function fakeJournal(failAt = -1) {
  const started: number[] = [];
  const finished: number[] = [];
  let inFlight = 0;
  let maxInFlight = 0;
  let n = 0;
  return {
    started,
    finished,
    maxInFlight: () => maxInFlight,
    calls: {
      async view() {
        return { records: [] as ViewRecord[] };
      },
      async append(_type: string, data: unknown) {
        const index = n++;
        started.push((data as { i: number }).i);
        maxInFlight = Math.max(maxInFlight, ++inFlight);
        // Earlier appends take longer, so any overlap would reorder them.
        await new Promise((r) => setTimeout(r, 20 - index * 5));
        inFlight--;
        finished.push((data as { i: number }).i);
        if (index === failAt) throw new Error("append failed");
        return { seq: index + 1 };
      },
    },
  };
}

test("unawaited appends run one at a time, in call order", async () => {
  const j = fakeJournal();
  const log = new BridgeLog(j.calls);
  await Promise.all([0, 1, 2].map((i) => log.append({ type: "entry", data: { i } })));
  assert.deepEqual(j.started, [0, 1, 2]);
  assert.deepEqual(j.finished, [0, 1, 2]);
  assert.equal(j.maxInFlight(), 1);
});

test("a failed append reaches its caller only, and the queue carries on", async () => {
  const j = fakeJournal(0);
  const log = new BridgeLog(j.calls);
  const results = await Promise.allSettled([0, 1, 2].map((i) => log.append({ type: "entry", data: { i } })));
  assert.deepEqual(
    results.map((r) => r.status),
    ["rejected", "fulfilled", "fulfilled"],
  );
  assert.deepEqual(j.finished, [0, 1, 2]);
});

test("view answers the records of the journal view", async () => {
  const log = new BridgeLog({
    async view() {
      return { records: [{ seq: 1 } as ViewRecord] };
    },
    async append() {},
  });
  assert.deepEqual(await log.view(), [{ seq: 1 }]);
});
