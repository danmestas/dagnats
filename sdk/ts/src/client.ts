// BridgeClient is the raw HTTP client for the dagnats bridge: connect,
// poll, resolve and logs, as docs/site/content/docs/workers/http-bridge.md
// documents them. It adds three things the wire does not give a caller:
// typed errors (errors.ts), an AbortSignal on every call, and a bounded
// default timeout on every call, so a bridge that stops answering can
// never hang a worker. TaskContext (context.ts) is the per-task layer on top.

import { Connection, type ConnectOptions } from "./connection.ts";
import { errorFromResponse } from "./errors.ts";

/** Per-call options: cancel it, or change how long it may take. */
export interface CallOptions {
  signal?: AbortSignal;
  timeout_ms?: number;
}

export interface PollOptions {
  /**
   * The worker's id. The poll wire carries none (the claim is by token),
   * so this is accepted for symmetry with connect and not sent.
   */
  worker: string;
  task_types: string[];
  worker_group?: string;
  /** Tasks to take at most; the bridge defaults to 1. */
  max?: number;
  /** How long the bridge holds the poll open when the queue is empty; default 5000, bridge max 60000. */
  wait_ms?: number;
}

/** One task as the bridge hands it out. Absent wire fields are absent here. */
export interface PollItem {
  task_id: string;
  /** The type this task was polled under; a poll may name several. */
  task_type: string;
  run_id: string;
  step_id: string;
  input?: unknown;
  iteration: number;
  attempt: number;
  /** The claim's dispatch nonce; absent for a task dispatched without one. */
  dispatch_nonce?: string;
  delivered: number;
  stream_seq: number;
  metadata?: Record<string, string>;
  traceparent?: string;
  tracestate?: string;
  /** Loop state of the last accepted `continue`, base64 (Go []byte). */
  state?: string;
  state_max_bytes?: number;
}

export type LogLine = string | { stream: "out" | "err"; data: string };

export const CALL_TIMEOUT_MS_DEFAULT = 5_000;
/** Slack added to a poll's wait_ms for the round trip. */
export const POLL_MARGIN_MS = 5_000;
const POLL_WAIT_MS_DEFAULT = 5_000;

export class BridgeClient {
  private readonly baseURL: string;
  private readonly token: string;

  /** `token` is the bridge bearer token; "" sends no Authorization header (dev mode). */
  constructor(baseURL: string, token: string) {
    this.baseURL = baseURL.replace(/\/+$/, "");
    this.token = token;
  }

  /** connect registers the worker and holds the heartbeat stream open, reconnecting when it drops. */
  connect(options: ConnectOptions): Connection {
    return new Connection({ baseURL: this.baseURL, headers: () => this.headers() }, options);
  }

  async poll(options: PollOptions, call: CallOptions = {}): Promise<PollItem[]> {
    const wait_ms = options.wait_ms ?? POLL_WAIT_MS_DEFAULT;
    const body = {
      task_types: options.task_types,
      worker_group: options.worker_group,
      max_tasks: options.max,
      timeout_ms: wait_ms,
    };
    const items = await this.post("/v1/tasks/poll", body, {
      signal: call.signal,
      timeout_ms: call.timeout_ms ?? wait_ms + POLL_MARGIN_MS,
    });
    return (items as PollItem[] | undefined) ?? [];
  }

  /** resolve is the raw call: POST /v1/tasks/{id}/resolve with `action` and its fields. */
  async resolve(task_id: string, action: string, body: Record<string, unknown> = {}, call: CallOptions = {}): Promise<unknown> {
    return this.post(`/v1/tasks/${encodeURIComponent(task_id)}/resolve`, { ...body, action }, call);
  }

  /** logs sends output lines for a claimed task; a bare string is a stdout line. */
  async logs(task_id: string, lines: LogLine[], call: CallOptions = {}): Promise<void> {
    const chunks = lines.map((line) => {
      const { stream, data } = typeof line === "string" ? { stream: "out", data: line } : line;
      return { stream, data: Buffer.from(data, "utf8").toString("base64") };
    });
    await this.post(`/v1/tasks/${encodeURIComponent(task_id)}/logs`, { chunks }, call);
  }

  private headers(): Record<string, string> {
    return this.token === "" ? {} : { authorization: `Bearer ${this.token}` };
  }

  private async post(path: string, body: unknown, call: CallOptions): Promise<unknown> {
    const signals = [AbortSignal.timeout(call.timeout_ms ?? CALL_TIMEOUT_MS_DEFAULT)];
    if (call.signal) signals.push(call.signal);
    const res = await fetch(this.baseURL + path, {
      method: "POST",
      headers: { "content-type": "application/json", ...this.headers() },
      body: JSON.stringify(body),
      signal: AbortSignal.any(signals),
    });
    const text = await res.text();
    if (!res.ok) throw errorFromResponse(res.status, text);
    return text === "" ? undefined : JSON.parse(text);
  }
}
