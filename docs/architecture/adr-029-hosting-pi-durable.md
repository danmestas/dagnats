# ADR-029: Hosting pi-durable

**Status:** Proposed, deferred (2026-10-21)
**Deciders:** Dan Mestas
**Issue:** danmestas/dagnats-agents#5

## Context

pi-durable (`@earendil-works/pi-durable`, MIT) is the durable harness behind
the Pi coding agent. It keeps one agent session in a pluggable `Storage`:
17 methods and a conformance suite (24 cases in 1.3.0, 25 on main). Its own
backends are memory, SQLite, JSONL, IndexedDB and Cloudflare Durable Object
SQLite. None of them guards against two processes opening one session: its
README states that one process owns a storage at a time and there is no
cross-process locking.

ADRs 025 to 028 give dagnats agents pi-durable's guarantees natively. This
ADR covers the other case: an agent already written against pi-durable in
TypeScript that should run as a dagnats step.

A spike built a JetStream `Storage` and ran pi-durable's own conformance
suite against it.

## Decision

Deferred. When taken up, the adapter lives in `danmestas/dagnats-agents`,
not in dagnats.

### Storage adapter

- One subject per session, `pi.{session}`, in a stream with atomic publish
  enabled and no age limit.
- A commit is one message, or an atomic batch of chunks above `max_payload`,
  with `Nats-Expected-Last-Subject-Sequence` on its first message. A second
  writer is rejected and stores nothing.
- Reads are served by pi-durable's own in-memory backend, rebuilt by
  replaying the subject on open. The adapter reuses that backend's
  `prepareCommit`, as pi-durable's JSONL backend does, so the semantics are
  pi-durable's own.

Spike results: 24 of 24 conformance cases on 1.3.0 and 25 of 25 on main;
exactly one writer won in each of 50 simultaneous races; 3,400 to 6,500
commits per second locally; open replays at about 17 µs per commit (1.8 s
and 268 MB for 100,000 commits).

### Running a Pi agent as a step

A TypeScript bridge worker runs the agent as an `agent_loop` step and opens
the session on `pi.{run}.{step}`, so a redelivered task fences a stalled
one. pi-durable's `onWake` maps to `Continue` with `Until` (ADR-027), and a
run cancel maps to `Conversation.abort()`.

## Requirements

| ID | Requirement | Proven by |
|---|---|---|
| R1 | The adapter MUST pass pi-durable's storage conformance suite for the pinned version. | Adapter CI |
| R2 | A commit from a second writer MUST be rejected and store nothing. | Fencing tests |
| R3 | A session reopened after a crash MUST resume a real Harness run, including an interrupted tool. | Harness end-to-end test |

## Open points

- **Replay grows with the session.** pi-durable has no API to export or
  import its in-memory state, which a snapshot would need. Fine for sessions
  in the tens of thousands of commits; long sessions need that API upstream
  first.
- **A NATS JavaScript client bug.** `@nats-io/jetstream`'s batch `commit()`
  hides every server error behind one generic message. The adapter writes
  the batch headers itself. The fix belongs upstream.
- **pi-durable is experimental.** Nine releases in its first three weeks,
  several with breaking changes; pin exact versions.
- **Not yet proven against a real Harness.** The spike ran the storage
  conformance suite and fencing tests only (R3 above).

## Security considerations

A session subject holds the whole conversation, including tool results, and
is kept for the life of the session. Access is governed by NATS subject
permissions on `pi.>`.
