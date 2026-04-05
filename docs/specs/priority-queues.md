# Priority Queues

**Status:** Design
**Date:** 2026-04-04
**Depends on:** Nothing (independent, most useful with concurrency limits)

## Problem

All workflow runs are FIFO. There is no way to express "enterprise customers should
be processed before free-tier" or "this urgent run should jump the queue." When
concurrency limits create backlogs, all pending runs wait equally.

## Design

### 1. Concept

Priority is a **numeric adjustment in seconds** applied to a run's effective queue
position. Positive values advance the run, negative values delay it. Computed from
input data via dot-path + rules map.

Range: **-600 to +600 seconds**.

### 2. Type Changes

```go
type PriorityConfig struct {
    Key           string         `json:"key"`            // dot-path into input
    Rules         map[string]int `json:"rules"`          // value -> offset seconds
    DefaultOffset int            `json:"default_offset"` // when no rule matches
}

type WorkflowDef struct {
    // ... existing fields ...
    Priority *PriorityConfig `json:"priority,omitempty"`
}

type WorkflowRun struct {
    // ... existing fields ...
    PriorityOffset int `json:"priority_offset,omitempty"`
}

// Computed, not stored -- single source of truth.
func (r WorkflowRun) EffectiveTime() time.Time {
    return r.CreatedAt.Add(
        -time.Duration(r.PriorityOffset) * time.Second,
    )
}
```

### 3. How It Works

At run creation: extract value at `Key` from input, look up in `Rules` map,
clamp to [-600, +600], store as `PriorityOffset`.

In `findOldestPendingRun`: compare `EffectiveTime()` instead of `CreatedAt`.
This is the only change in the hot path.

### 4. Resolution Logic

```go
func ResolvePriority(cfg *PriorityConfig, input json.RawMessage) int {
    if cfg == nil { return 0 }
    val, err := ExtractDotPath(cfg.Key, input)
    if err != nil { return 0 }
    strVal := fmt.Sprintf("%v", val)
    if offset, ok := cfg.Rules[strVal]; ok {
        return clampOffset(offset)
    }
    return clampOffset(cfg.DefaultOffset)
}
```

### 5. Builder API

```go
wf := dag.NewWorkflow("process-request").
    WithConcurrency(5, 0).
    WithPriority(dag.PriorityConfig{
        Key: "data.account_type",
        Rules: map[string]int{
            "enterprise": 300,
            "pro":        60,
        },
    }).
    Build()
```

### 6. Validation

- `Priority.Key` must not be empty.
- `Priority.Rules` must have at least one entry. Max 20 rules.
- Each rule offset and `DefaultOffset` must be in [-600, +600].
- Priority without `Concurrency.MaxRuns` triggers a warning (no effect).

### 7. CLI

```bash
dagnats run start my-workflow '{"data":"..."}' --priority=120
```

`--priority` flag overrides expression evaluation for manual urgency.

### 8. Edge Cases

- **No rule matches:** `DefaultOffset` (0 if not set). Same as no priority.
- **Input missing key path:** Offset = 0. Warning logged.
- **Same EffectiveTime:** `CreatedAt` breaks ties (earlier wins).
- **Negative offset (de-prioritize):** Effective time moves future, selected last.
