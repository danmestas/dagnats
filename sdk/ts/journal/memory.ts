// A Log in memory, for tests. It stamps the view envelope from the task
// it is opened for, the way the bridge stamps it from the claimed task.

import type { Log, Record, Task, ViewRecord } from "./types.ts";

function stamp(task: Task, seq: number, index: number, r: Record): ViewRecord {
  return {
    seq,
    type: r.type,
    iteration: task.iteration,
    attempt: task.attempt,
    dispatch_nonce: task.dispatch_nonce,
    delivered: task.delivered,
    stream_seq: task.stream_seq,
    index,
    data: r.data,
  };
}

export class MemoryLog implements Log {
  readonly records: ViewRecord[] = [];
  private readonly task: Task;
  private index = 0;

  /** `prior` seeds the log with records already in the view, as a fixture does. */
  constructor(task: Task, prior: ViewRecord[] = []) {
    this.task = task;
    this.records.push(...structuredClone(prior));
  }
  async view(): Promise<ViewRecord[]> {
    return structuredClone(this.records);
  }
  async append(record: Record): Promise<void> {
    this.records.push(stamp(this.task, this.records.length + 1, this.index++, structuredClone(record)));
  }
  /** What was appended since construction, type and data only; for tests. */
  appended(from: number): Record[] {
    return this.records.slice(from).map((r) => ({ type: r.type, data: r.data }));
  }
}
