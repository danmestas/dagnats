// Journal record shapes, matching what dagnats/journal writes (ADR-026)
// and what the bridge's journal.view answers, so a TypeScript worker's
// journal reads the same as a Go worker's to `dagnats journal check`, to
// the Go tools, and to the replay fixtures. The model output type is the
// caller's: the journal stores it verbatim and never looks inside.

/** Whether an unfinished tool call may be run again on replay. */
export type Replay = "safe" | "unsafe";

export interface ModelResponseData<M = unknown> {
  /** Position of this model call in the attempt, counting from 0. */
  call: number;
  output: M;
}

export interface ToolIntentData {
  name: string;
  /** Absent when the call had no input. */
  input?: unknown;
  /** Position of this tool call in the iteration, counting from 0. */
  call: number;
  /** `{run}.{step}.{iteration}.{call}`: the idempotency key a tool may use. */
  key: string;
  replay: Replay;
  /** The caller's tool call id, a cross-check on the position; stored as written and ignored by replay. */
  tool_call_id?: string;
}

export interface ToolResultData {
  call: number;
  key: string;
  output?: unknown;
  error?: string;
  /** Set when the tool was running in a worker that died and could not be run again. */
  interrupted?: boolean;
  /** The `entry` bodies the intent's writer appended after it, when any. */
  partial?: unknown[];
}

/** A record to append: its type and data body. */
export interface Record {
  type: string;
  data: unknown;
}

/** Identity of the task delivery this worker is: the poll item's identity. */
export interface Task {
  run_id: string;
  step_id: string;
  iteration: number;
  attempt: number;
  dispatch_nonce: string;
  delivered: number;
  stream_seq: number;
}

/** A record as journal.view answers it: the envelope the bridge wraps data in. */
export interface ViewRecord extends Record {
  seq: number;
  iteration: number;
  attempt: number;
  dispatch_nonce: string;
  delivered: number;
  stream_seq: number;
  index: number;
}

/**
 * Log is what a journal is built on: here an in-memory log; in a worker,
 * the dagnats journal over the HTTP bridge (BridgeLog). `view` answers
 * this iteration's records before this delivery's claim, in order, as
 * journal.view does. Appends are ordered; a Log never reorders or drops.
 */
export interface Log {
  view(): Promise<ViewRecord[]>;
  append(record: Record): Promise<void>;
}

export class ReplayMismatch extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ReplayMismatch";
  }
}
