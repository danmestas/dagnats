// Journal applies ADR-026's replay rules to a Log, as dagnats'
// journal.Tool and journal.Model do for Go workers. The rules, numbered
// as in the wire-protocol reference ("Replay rules for clients"):
//
//   1. A view record is prior when its seq is at or before the newest
//      commit in the view, or its attempt is lower than the task's.
//   2. Model calls match by position within this attempt; a failed
//      request appends nothing and leaves the position free; prior
//      attempts' responses are never reused.
//   3. A tool call first looks for an unhonoured prior-attempt unsafe
//      intent with the same name and canonical input, and is answered
//      under its call and key: by the newest result for that call after
//      it, else by appending an interrupted result. It takes no position.
//   4. Otherwise it takes the next position not held by a prior-attempt
//      unsafe intent. A recorded intent there must match name and input
//      (else ReplayMismatch); a result after it is reused; without one,
//      it reruns only when both declarations are safe, else it is
//      answered interrupted.
//   5. An interrupted result carries the entries its intent's writer
//      appended after the intent as `partial`.
//   6. Inputs compare as canonical JSON.
//
// test/replay-fixtures.test.ts replays dagnats' fixtures through this
// file and fails where it disagrees with the Go SDK. M is the model
// output type: the journal stores it as given.

import {
  ReplayMismatch,
  type Log,
  type ModelResponseData,
  type Replay,
  type Task,
  type ToolIntentData,
  type ToolResultData,
  type ViewRecord,
} from "./types.ts";

export const INTERRUPTED = "interrupted";

/** Bounds the tool calls and the model calls of one iteration, as Go's callsMax does. */
export const CALLS_MAX = 10_000;

/** canonical JSON: keys sorted at every level, no whitespace. An absent input is only equal to an absent input. */
export function canonical(value: unknown): string {
  if (value === undefined) return "";
  return JSON.stringify(sortKeys(value));
}

function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value !== null && typeof value === "object") {
    const out: { [k: string]: unknown } = {};
    for (const k of Object.keys(value as object).sort()) {
      out[k] = sortKeys((value as { [k: string]: unknown })[k]);
    }
    return out;
  }
  return value;
}

export type ToolOutcome = "ran" | "reused" | "interrupted";
export type ModelOutcome = "requested" | "reused";

interface PriorUnsafe {
  rec: ViewRecord;
  data: ToolIntentData;
  honoured: boolean;
}

export class Journal<M = unknown> {
  private readonly log: Log;
  private readonly task: Task;
  private view: ViewRecord[] = [];
  /** This attempt's records by position. */
  private readonly models = new Map<number, ModelResponseData<M>>();
  private readonly intents = new Map<number, ViewRecord>();
  /** Prior-attempt unsafe intents, in view order (rule 3), and the positions they hold (rule 4). */
  private readonly priorUnsafe: PriorUnsafe[] = [];
  private readonly held = new Set<number>();
  private nextModel = 0;
  private nextTool = 0;
  /** How many records of each kind this attempt had on open; for tests and logs. */
  recorded = { models: 0, intents: 0, results: 0 };

  private constructor(log: Log, task: Task) {
    this.log = log;
    this.task = task;
  }

  /** open reads the view and classifies its records (rule 1). */
  static async open<M = unknown>(log: Log, task: Task): Promise<Journal<M>> {
    const j = new Journal<M>(log, task);
    j.view = await log.view();
    let lastCommit = 0;
    for (const r of j.view) if (r.type === "commit" && r.seq > lastCommit) lastCommit = r.seq;
    let results = 0;
    for (const r of j.view) {
      const prior = r.seq <= lastCommit || r.attempt < task.attempt;
      switch (r.type) {
        case "model.response":
          if (!prior) j.models.set((r.data as ModelResponseData<M>).call, r.data as ModelResponseData<M>);
          break;
        case "tool.intent": {
          const d = r.data as ToolIntentData;
          if (prior) {
            if (d.replay === "unsafe") {
              j.priorUnsafe.push({ rec: r, data: d, honoured: false });
              j.held.add(d.call);
            }
          } else {
            // The newest intent for a call wins, as Go's recordedToolIntent does.
            j.intents.set(d.call, r);
          }
          break;
        }
        case "tool.result":
          if (!prior) results++;
          break;
        default:
          break;
      }
    }
    j.recorded = { models: j.models.size, intents: j.intents.size, results };
    return j;
  }

  private key(call: number): string {
    return `${this.task.run_id}.${this.task.step_id}.${this.task.iteration}.${call}`;
  }

  /** The newest tool.result in the view for `call` after `after`, if any. */
  private resultAfter(call: number, after: number): ToolResultData | undefined {
    let found: ToolResultData | undefined;
    for (const r of this.view) {
      if (r.type !== "tool.result" || r.seq <= after) continue;
      const d = r.data as ToolResultData;
      if (d.call === call) found = d;
    }
    return found;
  }

  /** The `entry` bodies the intent's writer appended after it (rule 5). */
  private partialAfter(intent: ViewRecord): unknown[] {
    const out: unknown[] = [];
    for (const r of this.view) {
      if (r.type !== "entry" || r.seq <= intent.seq) continue;
      if (
        r.iteration === intent.iteration &&
        r.attempt === intent.attempt &&
        r.dispatch_nonce === intent.dispatch_nonce &&
        r.delivered === intent.delivered &&
        r.stream_seq === intent.stream_seq
      ) {
        out.push(r.data);
      }
    }
    return out;
  }

  private async interrupt(intent: ViewRecord, d: ToolIntentData): Promise<ToolResultData> {
    const result: ToolResultData = { call: d.call, key: d.key, error: INTERRUPTED, interrupted: true };
    const partial = this.partialAfter(intent);
    if (partial.length > 0) result.partial = partial;
    await this.log.append({ type: "tool.result", data: result });
    return result;
  }

  /**
   * model runs one model call at the next position (rule 2): reused from
   * this attempt's record when there is one, else requested and appended
   * before this returns. A request that throws appends nothing and leaves
   * the position free.
   */
  async model(run: () => Promise<M>): Promise<{ message: M; outcome: ModelOutcome }> {
    const call = this.nextModel;
    if (call >= CALLS_MAX) throw new Error(`journal: more than ${CALLS_MAX} model calls in one iteration`);
    const recorded = this.models.get(call);
    if (recorded) {
      this.nextModel++;
      return { message: recorded.output, outcome: "reused" };
    }
    const message = await run();
    const data: ModelResponseData<M> = { call, output: message };
    await this.log.append({ type: "model.response", data });
    this.models.set(call, data);
    this.nextModel++;
    return { message, outcome: "requested" };
  }

  /**
   * tool runs one tool call under rules 3 to 5. `run` receives the
   * idempotency key so a tool with side effects can dedupe on it.
   */
  async tool(
    name: string,
    input: unknown,
    replay: Replay,
    run: (key: string) => Promise<{ output?: unknown; error?: string }>,
    toolCallId?: string,
  ): Promise<{ result: ToolResultData; outcome: ToolOutcome }> {
    // The next position not held by a prior unsafe intent (rule 4), found first so the
    // bound applies to every call, as in Go where the check precedes rule 3.
    let call = this.nextTool;
    while (this.held.has(call)) call++;
    if (call >= CALLS_MAX) throw new Error(`journal: more than ${CALLS_MAX} tool calls in one iteration`);

    // Rule 3: an unhonoured prior unsafe intent with the same name and input.
    const want = canonical(input);
    const prior = this.priorUnsafe.find((p) => !p.honoured && p.data.name === name && canonical(p.data.input) === want);
    if (prior) {
      prior.honoured = true;
      const result = this.resultAfter(prior.data.call, prior.rec.seq);
      if (result) return { result, outcome: result.interrupted ? "interrupted" : "reused" };
      return { result: await this.interrupt(prior.rec, prior.data), outcome: "interrupted" };
    }

    // Rule 4: take that position.
    this.nextTool = call + 1;
    const key = this.key(call);

    const intent = this.intents.get(call);
    if (intent) {
      const d = intent.data as ToolIntentData;
      if (d.name !== name || canonical(d.input) !== want) {
        throw new ReplayMismatch(
          `tool call ${call}: journal has ${d.name} ${canonical(d.input)}, agent asked ${name} ${want}`,
        );
      }
      const result = this.resultAfter(call, intent.seq);
      if (result) return { result, outcome: result.interrupted ? "interrupted" : "reused" };
      if (d.replay !== "safe" || replay !== "safe") {
        return { result: await this.interrupt(intent, d), outcome: "interrupted" };
      }
      // both safe: rerun under the recorded intent, appending only the result
    } else {
      const data: ToolIntentData = { name, call, key, replay };
      if (input !== undefined) data.input = input;
      if (toolCallId !== undefined) data.tool_call_id = toolCallId;
      await this.log.append({ type: "tool.intent", data });
    }
    const out = await run(key);
    const result: ToolResultData = { call, key };
    if (out.output !== undefined) result.output = out.output;
    if (out.error !== undefined) result.error = out.error;
    await this.log.append({ type: "tool.result", data: result });
    return { result, outcome: "ran" };
  }
}
