// TaskContext is the per-task layer over BridgeClient, mirroring the Go
// SDK's TaskContext: it binds a polled task's id once and names each
// resolve action as a method. Every method takes an optional CallOptions
// last, so one slow call can be aborted without a worker-wide signal.
//
// The bridge's poll answer has no dispatch nonce; the claim in
// journal.view does. `task.dispatch_nonce` is "" until the first view.

import type { BridgeClient, CallOptions, PollItem } from "./client.ts";
import type { Task, ViewRecord } from "../journal/types.ts";

export type FailureType = "retriable" | "non_retriable" | "retry_after";

export interface FailOptions {
  error: string;
  failure_type?: FailureType;
  /** With failure_type "retry_after": how long before the step retries. */
  retry_after_ms?: number;
}

/** The claim a journal.view answers: this delivery's identity. */
export type Claim = Omit<Task, "run_id" | "step_id">;

export interface JournalView {
  claim: Claim;
  /** This iteration's records written before the claim, in seq order; fixed for the claim's life. */
  records: ViewRecord[];
}

export class TaskContext {
  readonly task_id: string;
  readonly input: unknown;
  readonly item: PollItem;
  private readonly client: BridgeClient;
  private identity: Task;

  constructor(client: BridgeClient, item: PollItem) {
    this.client = client;
    this.item = item;
    this.task_id = item.task_id;
    this.input = item.input;
    this.identity = {
      run_id: item.run_id,
      step_id: item.step_id,
      iteration: item.iteration ?? 0,
      attempt: item.attempt ?? 0,
      dispatch_nonce: "",
      delivered: item.delivered,
      stream_seq: item.stream_seq ?? 0,
    };
  }

  /** The delivery's identity; Journal.open takes it as is. */
  get task(): Task {
    return this.identity;
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
   * loadCheckpoint answers the saved state, or undefined when the task has
   * none. State that is not JSON comes back as bytes.
   */
  async loadCheckpoint(call?: CallOptions): Promise<unknown> {
    const res = (await this.resolve("load_checkpoint", {}, call)) as
      | { state?: unknown; state_base64?: string }
      | undefined;
    if (res?.state_base64 !== undefined) return new Uint8Array(Buffer.from(res.state_base64, "base64"));
    return res?.state ?? undefined;
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
      const view = (await this.resolve("journal.view", {}, call)) as JournalView;
      this.identity = { ...this.identity, dispatch_nonce: view.claim.dispatch_nonce };
      return view;
    },
    /** Appends a record of the given type (bridge default "entry"); answers its seq. */
    append: async (type: string, data: unknown, call?: CallOptions): Promise<{ seq: number }> => {
      const body = { type, data: data === undefined ? null : data };
      return (await this.resolve("journal.append", body, call)) as { seq: number };
    },
  };
}
