// Connection keeps a worker registered with the bridge. POST
// /v1/workers/connect answers with a Server-Sent Events stream that the
// bridge holds open, sending `event: heartbeat` every 25 seconds
// (bridge/connect.go); the worker stays in the directory for as long as
// the stream is open and is deregistered when it closes. So this holds the
// stream open and, when it drops, opens it again.
//
// Reconnects back off exponentially up to a cap, with jitter so a fleet
// restarted together does not retry in step. A 4xx is the bridge refusing
// this worker (bad body, a worker_id another token owns, a scope the token
// lacks): retrying cannot change the answer, so it ends the connection and
// `closed` rejects with the BridgeError. 408 and 429 are the exceptions:
// they say "not now", so they are retried, waiting at least as long as the
// Retry-After header asks (a proxy or rate limiter sets it) when it has one. A stream that goes silent for
// longer than idle_timeout_ms is dropped too, since a half-open TCP
// connection never reports an error by itself.

import { BridgeError, errorFromResponse } from "./errors.ts";
import { SSEParser } from "./sse.ts";

/** Three missed 25s heartbeats. */
export const IDLE_TIMEOUT_MS_DEFAULT = 75_000;
const BACKOFF_BASE_MS_DEFAULT = 500;
const BACKOFF_MAX_MS_DEFAULT = 15_000;
/** A Retry-After longer than this is not honoured in full; the worker would look dead. */
const RETRY_AFTER_MAX_MS = 300_000;

export interface ConnectOptions {
  /** The worker id the bridge registers (`worker_id` on the wire). */
  worker: string;
  task_types: string[];
  /** Concurrent tasks this worker advertises; the bridge defaults to 1. */
  max_tasks?: number;
  worker_group?: string;
  backoff_base_ms?: number;
  backoff_max_ms?: number;
  idle_timeout_ms?: number;
}

export interface ConnectionDeps {
  baseURL: string;
  headers: () => Record<string, string>;
}

export class Connection {
  /** Resolves when the stream first opens; rejects if the connection ends before that. */
  readonly ready: Promise<void>;
  /** Never resolves: rejects with the reason the connection ended. */
  readonly closed: Promise<never>;

  private readonly deps: ConnectionDeps;
  private readonly options: ConnectOptions;
  private readonly stop = new AbortController();
  private stopped = false;
  /** Set by a refused attempt that carried Retry-After; consumed by the next wait. */
  private retryAfterMs = 0;
  private resolveReady!: () => void;
  private rejectReady!: (reason: unknown) => void;
  private rejectClosed!: (reason: unknown) => void;

  constructor(deps: ConnectionDeps, options: ConnectOptions) {
    this.deps = deps;
    this.options = options;
    this.ready = new Promise<void>((resolve, reject) => {
      this.resolveReady = resolve;
      this.rejectReady = reject;
    });
    this.closed = new Promise<never>((_, reject) => {
      this.rejectClosed = reject;
    });
    // A caller that only awaits one of them must not trip unhandled-rejection on the other.
    this.ready.catch(() => {});
    this.closed.catch(() => {});
    void this.run();
  }

  /** close ends the connection for good; `closed` rejects with a "connection closed" error. */
  close(): void {
    this.end(new Error("connection closed"));
  }

  private end(reason: unknown): void {
    if (this.stopped) return;
    this.stopped = true;
    this.stop.abort();
    this.rejectReady(reason);
    this.rejectClosed(reason);
  }

  private async run(): Promise<void> {
    const baseMs = this.options.backoff_base_ms ?? BACKOFF_BASE_MS_DEFAULT;
    const maxMs = this.options.backoff_max_ms ?? BACKOFF_MAX_MS_DEFAULT;
    let delayMs = baseMs;
    while (!this.stopped) {
      try {
        if (await this.session()) delayMs = baseMs;
      } catch (err) {
        if (this.stopped) return;
        if (err instanceof BridgeError && err.status >= 400 && err.status < 500 && !isRetryable4xx(err.status)) {
          this.end(err);
          return;
        }
      }
      if (this.stopped) return;
      const waitMs = Math.max(delayMs * (0.5 + Math.random() / 2), this.retryAfterMs);
      this.retryAfterMs = 0;
      await sleep(waitMs, this.stop.signal);
      delayMs = Math.min(delayMs * 2, maxMs);
    }
  }

  /** One stream: true when it opened, however it ended. Throws on a refusal or a failed attempt. */
  private async session(): Promise<boolean> {
    const idleMs = this.options.idle_timeout_ms ?? IDLE_TIMEOUT_MS_DEFAULT;
    const attempt = new AbortController();
    const abortBoth = () => attempt.abort();
    this.stop.signal.addEventListener("abort", abortBoth, { once: true });
    let idle = setTimeout(abortBoth, idleMs);
    let established = false;
    try {
      const res = await fetch(`${this.deps.baseURL}/v1/workers/connect`, {
        method: "POST",
        headers: { "content-type": "application/json", ...this.deps.headers() },
        body: JSON.stringify(connectBody(this.options)),
        signal: attempt.signal,
      });
      if (!res.ok || res.body === null) {
        this.retryAfterMs = retryAfterMs(res.headers.get("retry-after"));
        throw errorFromResponse(res.status, await res.text());
      }
      established = true;
      this.resolveReady();
      const parser = new SSEParser();
      const decoder = new TextDecoder();
      const reader = res.body.getReader();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) return true;
        // Only a whole event proves the bridge is alive; a bare comment line does not.
        if (parser.push(decoder.decode(value, { stream: true })).length > 0) {
          clearTimeout(idle);
          idle = setTimeout(abortBoth, idleMs);
        }
      }
    } catch (err) {
      if (this.stopped) throw err;
      if (err instanceof BridgeError) throw err;
      // A dropped or silent stream is retried; one that had opened resets the backoff.
      return established;
    } finally {
      clearTimeout(idle);
      this.stop.signal.removeEventListener("abort", abortBoth);
    }
  }
}

function connectBody(o: ConnectOptions): Record<string, unknown> {
  return {
    worker_id: o.worker,
    task_types: o.task_types,
    max_tasks: o.max_tasks,
    worker_group: o.worker_group,
  };
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const timer = setTimeout(done, ms);
    function done() {
      clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    }
    signal.addEventListener("abort", done, { once: true });
  });
}

/** 408 and 429 ask the client to come back later rather than refusing it. */
function isRetryable4xx(status: number): boolean {
  return status === 408 || status === 429;
}

/** Retry-After as milliseconds: delta-seconds or an HTTP date; 0 when absent or unreadable. */
function retryAfterMs(header: string | null): number {
  if (header === null) return 0;
  const seconds = Number(header);
  const ms = Number.isFinite(seconds) ? seconds * 1000 : Date.parse(header) - Date.now();
  return Number.isFinite(ms) && ms > 0 ? Math.min(ms, RETRY_AFTER_MAX_MS) : 0;
}
