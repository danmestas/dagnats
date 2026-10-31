// @dagnats/bridge: a TypeScript client for the dagnats HTTP bridge.
// The ADR-026 replay rules are in "@dagnats/bridge/journal"; BridgeLog and
// Journal are re-exported here for the common case of using both.
export { BridgeClient, CALL_TIMEOUT_MS_DEFAULT, POLL_MARGIN_MS } from "./client.ts";
export type { CallOptions, LogLine, PollItem, PollOptions } from "./client.ts";
export { Connection } from "./connection.ts";
export type { ConnectOptions } from "./connection.ts";
export { TaskContext } from "./context.ts";
export type { Checkpoint, Claim, FailOptions, FailureType, JournalView, TaskIdentity } from "./context.ts";
export {
  BodyTooLarge,
  BridgeError,
  Committed,
  Fenced,
  NotProvisioned,
  RestartLimit,
  RunCancelled,
  Stale,
  StateTooLarge,
  isTerminal,
} from "./errors.ts";
export { BridgeLog, INTERRUPTED, Journal, MemoryLog, ReplayMismatch, canonical } from "../journal/index.ts";
