---
title: Conditional Skipping
weight: 3
---

The `skip_if` field on a step definition evaluates a condition against a parent step's output, skipping the step entirely when the condition is true.

## ParentCond

The `ParentCond` struct defines the skip condition:

| Field | JSON Key | Type | Description |
|-------|----------|------|-------------|
| **StepID** | `step_id` | `string` | Parent step to evaluate (must be in `depends_on`) |
| **Field** | `field` | `string` | Top-level key in the parent's JSON output |
| **Op** | `op` | `string` | Comparison operator |
| **Value** | `value` | `any` | Value to compare against |

### Supported Operators

| Operator | Meaning |
|----------|---------|
| `==` | Equal |
| `!=` | Not equal |
| `<` | Less than |
| `>` | Greater than |
| `<=` | Less than or equal |
| `>=` | Greater than or equal |

## Type Coercion

The condition evaluator compares values using these rules:

- **Numbers:** Both sides parsed as `float64` (JSON default). All six operators supported.
- **Strings:** Lexicographic comparison. All six operators supported.
- **Booleans:** Only `==` and `!=` are supported. Other operators return false.
- **Mismatched types:** Condition evaluates to false (step runs normally).

If the parent step has no output or the field is missing, the condition evaluates to **false** and the step executes.

## Builder API

Use `SkipIf` with a `StepRef` for compile-time-safe references:

```go
wf := dag.NewWorkflow("review-pipeline")

lint := wf.Task("lint", "lint.run").
    WithTimeout(5 * time.Minute)

postResults := wf.Task("post-results", "github.comment").
    After(lint).
    WithTimeout(30 * time.Second).
    SkipIf(lint, "issues_found", "==", 0)
```

If the `lint` step's output contains `{"issues_found": 0}`, `post-results` is skipped. The `SkipIfOutput` helper constructs the `ParentCond` internally.

## JSON Schema

```json
{
  "id": "post-results",
  "task": "github.comment",
  "timeout": "30s",
  "type": "normal",
  "depends_on": ["lint"],
  "skip_if": {
    "step_id": "lint",
    "field": "issues_found",
    "op": "==",
    "value": 0
  }
}
```

## Skip Semantics

When a step is skipped:

- Its status is set to `skipped`
- **Downstream steps treat skipped as completed.** A step waiting on a skipped dependency proceeds normally. The skipped step produces no output, so any downstream `skip_if` referencing its fields evaluates to false.
- Skipped steps do not count against [concurrency limits](/docs/flow-control/concurrency-limits).
- Skipped steps do not appear in the [dead letter queue](/docs/reliability/dead-letter-queue).

## Validation

The workflow validator enforces:

1. `skip_if.step_id` must reference a step in the `depends_on` list (not any arbitrary step)
2. `skip_if.op` must be one of the six valid operators
3. The referenced step must exist in the workflow

Violations produce a clear error from `Build()` or `Validate()`.

## Related Pages

- [Normal Steps](/docs/step-types/normal-steps) -- the default step type that supports skip_if
- [Error Handling](/docs/reliability/error-handling) -- what happens when non-skipped steps fail
- [Priority](/docs/flow-control/priority) -- ordering runs when steps are skipped
