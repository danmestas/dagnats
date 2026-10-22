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

Non-Go workers reach the same journal through the HTTP bridge actions `journal.append`, `journal.read` and `load_checkpoint`, with the same outcomes mapped to HTTP statuses. See the [wire protocol](/docs/reference/wire-protocol).
