// BridgeLog is a Log over the bridge's journal actions: view reads
// journal.view, append writes journal.append. It takes the two calls
// structurally (a TaskContext's `journal` is one) so this module does not
// depend on the client.

import type { Log, Record, ViewRecord } from "./types.ts";

export interface JournalCalls {
  view(): Promise<{ records: ViewRecord[] }>;
  append(type: string, data: unknown): Promise<unknown>;
}

export class BridgeLog implements Log {
  private readonly calls: JournalCalls;
  /** The tail of the append chain: the next append starts when this settles. */
  private tail: Promise<unknown> = Promise.resolve();

  constructor(calls: JournalCalls) {
    this.calls = calls;
  }

  async view(): Promise<ViewRecord[]> {
    return (await this.calls.view()).records;
  }

  /**
   * One append is in flight at a time, in call order, so a caller that does
   * not await each one (a model streaming events, say) still writes records
   * in the order it produced them; the bridge assigns seq in arrival order.
   * A failed append does not stop the ones queued behind it, and each caller
   * sees its own append's outcome: they were issued, so they are answered
   * (a fenced worker gets Fenced from every one).
   */
  append(record: Record): Promise<void> {
    const run = this.tail.then(
      () => this.calls.append(record.type, record.data),
      () => this.calls.append(record.type, record.data),
    );
    this.tail = run.catch(() => {});
    return run.then(() => undefined);
  }
}
