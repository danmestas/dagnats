// Typed errors for the bridge. The status alone does not say what
// happened: a 409 is either a fence (bridge/journal.go, a JSON body with
// code "fenced") or a cancelled run (bridge/resolve.go, plain text), and
// the caller must treat them differently in logs even though both mean
// stop. So the class is chosen from status AND body. The Go client maps
// every 409 to ErrRunCancelled; this one must not.

/** Any non-2xx answer from the bridge: the status and the raw body. */
export class BridgeError extends Error {
  readonly status: number;
  readonly body: string;
  /** The `code` of a JSON body, when it has one; for codes this client does not name yet. */
  readonly code: string | undefined;

  constructor(status: number, body: string, message?: string) {
    super(message ?? `bridge: ${status} ${body.trim()}`);
    this.name = new.target.name;
    this.status = status;
    this.body = body;
    this.code = jsonCode(body);
  }
}

/** 409 {"code":"fenced"}: another claim follows this worker's; stop and write nothing more. */
export class Fenced extends BridgeError {}
/** 409 "run cancelled": the run was cancelled; the bridge withdrew the task. */
export class RunCancelled extends BridgeError {}
/** 410 stale: a later iteration holds the step. */
export class Stale extends BridgeError {}
/** 410 committed: this attempt already committed. */
export class Committed extends BridgeError {}
/** 410 restart_limit: the claim found the restart limit's worth of claims with no progress. */
export class RestartLimit extends BridgeError {}
/** 413 state_too_large: loop state over the task's loop_state_max_bytes. Not terminal: send less. */
export class StateTooLarge extends BridgeError {
  readonly maxBytes: number | undefined;
  readonly bytes: number | undefined;

  constructor(status: number, body: string) {
    super(status, body);
    this.maxBytes = jsonNumber(body, "max_bytes");
    this.bytes = jsonNumber(body, "bytes");
  }
}
/** 413 body_too_large: a request body over the bridge's 8 MiB limit. Not terminal. */
export class BodyTooLarge extends BridgeError {
  readonly maxBytes: number | undefined;

  constructor(status: number, body: string) {
    super(status, body);
    this.maxBytes = jsonNumber(body, "max_bytes");
  }
}
/** 413 record_too_large: a journal record over the server's max_payload. Not terminal: append less. */
export class RecordTooLarge extends BridgeError {
  readonly maxBytes: number | undefined;

  constructor(status: number, body: string) {
    super(status, body);
    this.maxBytes = jsonNumber(body, "max_bytes");
  }
}
/**
 * 409 inputs_queued: a `continue` with `wait.input` was refused because inputs are already
 * queued for the step. Not terminal: the task is still claimed. Place the inputs (inputs.place)
 * and continue without the input wait.
 */
export class InputsQueued extends BridgeError {}
/** 400 invalid_wait: the `wait` of a `continue` was refused. Not terminal: fix the wait. */
export class InvalidWait extends BridgeError {}
/** 400 wait_requires_agent_loop: a `continue` with a `wait` on a step that is not an agent_loop step. Not terminal: continue without the wait. */
export class WaitRequiresAgentLoop extends BridgeError {}
/** 503 not_provisioned: the AGENT_JOURNAL stream is missing. */
export class NotProvisioned extends BridgeError {}

function jsonCode(body: string): string | undefined {
  if (!body.startsWith("{")) return undefined;
  try {
    const parsed: unknown = JSON.parse(body);
    if (parsed !== null && typeof parsed === "object" && "code" in parsed) {
      const code = (parsed as { code: unknown }).code;
      return typeof code === "string" ? code : undefined;
    }
  } catch {
    // not JSON after all; the body stays available as text
  }
  return undefined;
}

function jsonNumber(body: string, key: string): number | undefined {
  if (!body.startsWith("{")) return undefined;
  try {
    const value = (JSON.parse(body) as { [k: string]: unknown })[key];
    return typeof value === "number" ? value : undefined;
  } catch {
    return undefined;
  }
}

/** errorFromResponse picks the class for a non-2xx answer from its status and body. */
export function errorFromResponse(status: number, body: string): BridgeError {
  const code = jsonCode(body);
  if (status === 409) {
    if (code === "fenced") return new Fenced(status, body);
    if (code === "inputs_queued") return new InputsQueued(status, body);
    if (body.trim() === "run cancelled") return new RunCancelled(status, body);
  }
  if (status === 400 && code === "invalid_wait") return new InvalidWait(status, body);
  if (status === 400 && code === "wait_requires_agent_loop") return new WaitRequiresAgentLoop(status, body);
  if (status === 410) {
    if (code === "stale") return new Stale(status, body);
    if (code === "committed") return new Committed(status, body);
    if (code === "restart_limit") return new RestartLimit(status, body);
  }
  if (status === 413) {
    if (code === "state_too_large") return new StateTooLarge(status, body);
    if (code === "record_too_large") return new RecordTooLarge(status, body);
    if (code === "body_too_large") return new BodyTooLarge(status, body);
  }
  if (status === 503 && code === "not_provisioned") return new NotProvisioned(status, body);
  return new BridgeError(status, body);
}

/**
 * isTerminal reports whether err means the task is over for this worker:
 * stop, resolve nothing more. Network errors, aborts and 5xx are not
 * terminal; the same call may succeed later.
 */
export function isTerminal(err: unknown): boolean {
  return (
    err instanceof Fenced ||
    err instanceof Stale ||
    err instanceof Committed ||
    err instanceof RestartLimit ||
    err instanceof RunCancelled
  );
}
