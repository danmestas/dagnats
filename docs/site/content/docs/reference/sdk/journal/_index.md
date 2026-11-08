---
title: journal
weight: 11
---

```
import "github.com/danmestas/dagnats/journal"
```

The per-step agent journal on the `AGENT_JOURNAL` stream (ADR-026). A handler claims a dispatch with `worker.TaskContext.Journal()`, appends records, and ends each iteration with one commit; the package owns the fencing that keeps a stalled worker from writing after its successor has claimed the step.

## Key Types and Functions

| Name | Description |
|------|-------------|
| `Open(ctx, js, pub, task)` | Claims a dispatch. Returns `ErrStale`, `ErrFenced` or a `*Journal` |
| `(*Journal).Append`, `Pause`, `Commit` | Fenced writes; a rejected write re-reads the tail from the leader |
| `(*Journal).State`, `Records`, `Committed`, `Resume` | What the claim found before it wrote |
| `Read(ctx, js, runID, stepID)` | Every record of a step's journal, in order |
| `Check(ctx, nc, runID)` | The invariant checker behind `dagnats journal check` |
| `ErrStale`, `ErrFenced`, `ErrCommitted` | The outcomes a handler must stop on |
| `ErrArchive`, `ErrDiscontinuous` | A read that needed purged records the archive could not supply, or found a hole |
| `CheckContinuity(records, from)` | Checks each record names the one before it on its subject |
| `ArchivedPrefix`, `ArchivedBefore` | The archive fallthrough the API and the engine relay use |

**Purged records.** An archiver may purge `AGENT_JOURNAL` below what it archived. Every record names the step's previous record (`Nats-Expected-Last-Subject-Sequence`), so a read knows exactly when its first record's predecessor is gone: only then does it read the purged part from the archive (`lode.stream.<domain>.AGENT_JOURNAL.range`, through `dagnatsext.ArchiveReader`), and the same chain proves no record is missing or repeated where the archive meets JetStream. `Open`, `Read` and every bridge and relay read fail with `ErrArchive` or `ErrDiscontinuous`, naming the run and step, rather than replay a journal with records missing. A journal JetStream holds in full never reaches the archive. A step JetStream holds nothing of is read from the archive when it can have had history (a later iteration, a retry, a fork's parent), never on a new step's first open, and a later iteration the archive holds nothing of fails with `ErrArchive`. Archived record times have microsecond precision. See the operations configuration page, "Reading purged journal records from the archive".

Non-Go workers reach the same journal through the HTTP bridge actions `journal.append`, `journal.read` and `load_checkpoint`, with the same outcomes mapped to HTTP statuses. See the [wire protocol](/docs/reference/wire-protocol).
