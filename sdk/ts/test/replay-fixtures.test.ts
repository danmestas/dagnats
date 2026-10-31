// Methodology: conformance with the Go SDK. Every fixture under
// journal/testdata/replay at the repository root (dagnats' own files, not
// copied here) is one
// case of the ADR-026 replay rules: a task, the view records, the
// handler's calls with what each tool or model would return, the
// expected outcome of each call, and the records that must be appended.
// Each is replayed through Journal and compared as canonical JSON.

import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { Journal, canonical } from "../journal/journal.ts";
import { MemoryLog } from "../journal/memory.ts";
import { ReplayMismatch, type Record, type Replay, type Task, type ViewRecord } from "../journal/types.ts";

interface Fixture {
  description: string;
  task: Task;
  records: ViewRecord[];
  calls: Array<
    | { kind: "tool"; name: string; input?: unknown; replay: Replay; output?: unknown; error?: string }
    | { kind: "model"; output?: unknown; error?: string }
  >;
  expect: Array<{ outcome: string; result?: unknown; output?: unknown }>;
  appended: Record[];
}

const dir = fileURLToPath(new URL("../../../journal/testdata/replay/", import.meta.url));
const files = readdirSync(dir).filter((f) => f.endsWith(".json")).sort();
assert.ok(files.length >= 15, "dagnats ships at least the 15 replay cases of #849");

for (const file of files) {
  const fx = JSON.parse(readFileSync(path.join(dir, file), "utf8")) as Fixture;
  test(`${file}: ${fx.description.split(".")[0]}`, async () => {
    const log = new MemoryLog(fx.task, fx.records);
    const base = fx.records.length;
    const j = await Journal.open<unknown>(log, fx.task);
    // A mismatch ends the case, so `expect` may be shorter than `calls`.
    assert.ok(fx.expect.length <= fx.calls.length);

    for (let i = 0; i < fx.expect.length; i++) {
      const call = fx.calls[i]!;
      const want = fx.expect[i]!;
      if (call.kind === "model") {
        if (want.outcome === "failed") {
          await assert.rejects(j.model(async () => Promise.reject(new Error(call.error))), { message: call.error });
          continue;
        }
        const got = await j.model(async () => call.output as unknown);
        assert.equal(got.outcome, want.outcome, `call ${i}`);
        assert.equal(canonical(got.message), canonical(want.output), `call ${i} output`);
        continue;
      }
      if (want.outcome === "mismatch") {
        await assert.rejects(
          j.tool(call.name, call.input, call.replay, async () => ({ output: call.output, error: call.error })),
          ReplayMismatch,
        );
        break;
      }
      const got = await j.tool(call.name, call.input, call.replay, async () => {
        const r: { output?: unknown; error?: string } = {};
        if (call.output !== undefined) r.output = call.output;
        if (call.error !== undefined) r.error = call.error;
        return r;
      });
      assert.equal(got.outcome, want.outcome, `call ${i}`);
      assert.equal(canonical(got.result), canonical(want.result), `call ${i} result`);
    }

    const appended = log.appended(base);
    assert.equal(
      canonical(appended),
      canonical(fx.appended),
      `appended records\n got: ${JSON.stringify(appended)}\nwant: ${JSON.stringify(fx.appended)}`,
    );
  });
}
