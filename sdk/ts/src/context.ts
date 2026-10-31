// TaskContext is the per-task layer over BridgeClient, mirroring the Go
// SDK's TaskContext: it binds a polled task's id once and names each
// resolve action as a method. Every method takes an optional CallOptions
// last, so one slow call can be aborted without a worker-wide signal.

import type { BridgeClient, CallOptions, PollItem } from "./client.ts";
import type { Task, ViewRecord } from "../journal/types.ts";

export type FailureType = "retriable" | "non_retriable" | "retry_after";

export interface FailOptions {
  error: string;
  failure_type?: FailureType;
  /** With failure_type "retry_after": how long before the step retries. */
  retry_after_ms?: number;
}

/**
 * A durable wait for `continue` (ADR-027): the engine parks the step and no worker holds a
 * claim meanwhile. `delay_ms` and `until` are a time to wake at (`until`, an ISO-8601 time or a
 * Date, wins when both are set); `input` wakes the step as soon as an input is queued for it.
 * A time and `input` together wake on whichever comes first.
 */
export interface ContinueWait {
  delay_ms?: number;
  until?: string | Date;
  input?: boolean;
}

export interface ContinueOptions {
  /** This iteration's output. */
  output?: unknown;
  /** Loop state for the next iteration; absent keeps the checkpointed (or polled) state. */
  state?: unknown;
  wait?: ContinueWait;
}

/** The claim a journal.view answers: this delivery's identity. */
export type Claim = Omit<Task, "run_id" | "step_id">;

export interface JournalView {
  claim: Claim;
  /** This iteration's records written before the claim, in seq order; fixed for the claim's life. */
  records: ViewRecord[];
}

/** What loadCheckpoint answers: the state and where the bridge found it. */
export interface Checkpoint {
  /** The saved JSON, or bytes when the state is not JSON. */
  state: unknown;
  /** The bridge's own value: "journal", "checkpoint" (buffered or stored) or "payload" (as polled). */
  source: string;
}

/** The delivery's identity: what Journal.open takes, plus the type it was polled under. */
export interface TaskIdentity extends Task {
  task_type: string;
}

export class TaskContext {
  readonly task_id: string;
  readonly input: unknown;
  readonly item: PollItem;
  private readonly client: BridgeClient;
  /** The delivery's identity as polled; Journal.open takes it as is. */
  readonly task: TaskIdentity;

  constructor(client: BridgeClient, item: PollItem) {
    this.client = client;
    this.item = item;
    this.task_id = item.task_id;
    this.input = item.input;
    this.task = {
      task_type: item.task_type,
      run_id: item.run_id,
      step_id: item.step_id,
      iteration: item.iteration ?? 0,
      attempt: item.attempt ?? 0,
      dispatch_nonce: item.dispatch_nonce ?? "",
      delivered: item.delivered,
      stream_seq: item.stream_seq ?? 0,
    };
  }

  private resolve(action: string, body: Record<string, unknown>, call?: CallOptions): Promise<unknown> {
    return this.client.resolve(this.task_id, action, body, call);
  }

  async complete(output: unknown, call?: CallOptions): Promise<void> {
    await this.resolve("complete", { output }, call);
  }

  async fail(options: FailOptions, call?: CallOptions): Promise<void> {
    const { error, failure_type, retry_after_ms } = options;
    await this.resolve("fail", { error, failure_type, retry_after_ms }, call);
  }

  /** checkpoint saves plain JSON as the task's state and extends the claim. */
  async checkpoint(data: unknown, call?: CallOptions): Promise<void> {
    await this.resolve("checkpoint", { data }, call);
  }

  /**
   * loadCheckpoint answers the saved state and where the bridge found it,
   * or undefined when the task has none. State that is not JSON comes back
   * as bytes.
   */
  async loadCheckpoint(call?: CallOptions): Promise<Checkpoint | undefined> {
    const res = (await this.resolve("load_checkpoint", {}, call)) as
      | { source: string; state?: unknown; state_base64?: string }
      | undefined;
    if (res === undefined) return undefined;
    if (res.state_base64 !== undefined) {
      return { state: new Uint8Array(Buffer.from(res.state_base64, "base64")), source: res.source };
    }
    if (res.state === null || res.state === undefined) return undefined;
    return { state: res.state, source: res.source };
  }

  /**
   * continue ends this iteration and asks for the next, optionally after a durable `wait`.
   * `state` is checkpointed first, as `checkpoint` does. With `wait.input` and inputs already
   * queued it throws InputsQueued and the task stays claimed: place the inputs and continue
   * without the input wait.
   */
  async continue(options: ContinueOptions = {}, call?: CallOptions): Promise<void> {
    const { output, state, wait } = options;
    if (state !== undefined) await this.checkpoint(state, call);
    const until = wait?.until instanceof Date ? wait.until.toISOString() : wait?.until;
    const body = { output, wait: wait === undefined ? undefined : { ...wait, until } };
    await this.resolve("continue", body, call);
  }

  /** heartbeat extends the claim with nothing else. */
  async heartbeat(call?: CallOptions): Promise<void> {
    await this.resolve("heartbeat", {}, call);
  }

  /** stream publishes a JSON chunk to live consumers of the step and extends the claim. */
  async stream(chunk: unknown, call?: CallOptions): Promise<void> {
    await this.resolve("stream", { data: chunk }, call);
  }

  readonly journal = {
    /** The claim and the records the claim was opened with. A fenced task answers Fenced. */
    view: async (call?: CallOptions): Promise<JournalView> => {
      return (await this.resolve("journal.view", {}, call)) as JournalView;
    },
    /** Appends a record of the given type (bridge default "entry"); answers its seq. */
    append: async (type: string, data: unknown, call?: CallOptions): Promise<{ seq: number }> => {
      const body = { type, data: data === undefined ? null : data };
      return (await this.resolve("journal.append", body, call)) as { seq: number };
    },
  };
}
