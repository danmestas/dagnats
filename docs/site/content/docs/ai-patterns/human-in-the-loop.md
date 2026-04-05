---
title: Human in the Loop
weight: 5
---

Human-in-the-loop patterns let you inject human judgment into automated LLM workflows for high-risk actions, quality review, or mid-execution guidance.

## Two Mechanisms

DagNats provides two complementary mechanisms for human interaction:

| Mechanism | When to Use | How It Works |
|-----------|------------|--------------|
| [Approval Gates](/docs/step-types/approval-gates) | Before a step executes | Engine-level pause; step does not start until approved |
| [Signals](/docs/coordination/signals) | During a step's execution | Handler-level pause; step blocks on `WaitForSignal` |

**Approval gates** are best for "should this proceed?" decisions -- deploy approvals, spend authorizations, destructive operations. The workflow pauses **between** steps.

**Signals** are best for "what should I do next?" decisions -- human feedback to a running agent, corrections to generated content, parameter adjustments. The workflow pauses **within** a step.

## Approval Gates for High-Risk Actions

When an LLM agent proposes a destructive action (deleting files, deploying code, sending emails), gate it behind an approval step:

```go
wf := dag.NewWorkflow("agent-with-approval")

agent := wf.AgentLoop("plan", "llm-agent").
    WithMaxIterations(10)

approve := wf.Approval("review-plan", dag.ApprovalConfig{
    Timeout:     4 * time.Hour,
    Subject:     "approval.agent.action",
    Description: "Review agent's proposed changes",
}).After(agent)

execute := wf.Task("execute", "apply-changes").
    After(approve)

def, _ := wf.Build()
```

The agent loop reasons and produces a plan. The approval gate pauses until a human reviews it. Only after approval does execution proceed. If the human rejects, the workflow fails (or routes to an `OnFailure` handler for re-planning).

The approval notification publishes to the NATS subject `approval.agent.action`. External integrations (Slack, email, dashboard) subscribe and present the decision to the reviewer.

## Signal-Based Review Mid-Execution

For interactive feedback during an agent loop, use signals. The agent pauses mid-iteration and waits for input:

```go
w.Handle("llm-agent", func(ctx worker.TaskContext) error {
    var state AgentState
    if saved, _ := ctx.LoadCheckpoint(); saved != nil {
        json.Unmarshal(saved, &state)
    }

    result, _ := callLLM(state.Messages)

    if result.Confidence < 0.7 {
        // Low confidence -- ask for human guidance
        ctx.PutStream([]byte("Low confidence. Requesting review..."))
        review, err := ctx.WaitForSignal("human-review", 1*time.Hour)
        if err != nil {
            return ctx.Fail(err)
        }
        state.Messages = append(state.Messages,
            Message{Role: "user", Content: string(review)},
        )
        data, _ := json.Marshal(state)
        ctx.Checkpoint(data)
        return ctx.Continue(nil)
    }

    if result.Done {
        return ctx.Complete([]byte(result.Answer))
    }
    return ctx.Continue(nil)
})
```

An external system sends the human's feedback:

```bash
# Via CLI
dagnats signal send <run-id> human-review '{"guidance": "Focus on auth module"}'
```

## Combining Approval with Agent Loops

For maximum safety, use both: signals for in-loop guidance and approval gates before irreversible actions.

```go
wf := dag.NewWorkflow("safe-agent")

// Agent reasons and produces a plan (signals for mid-loop feedback)
agent := wf.AgentLoop("reason", "llm-reason").
    WithMaxIterations(15).
    WithMaxDuration(20 * time.Minute)

// Human reviews the plan before execution
gate := wf.Approval("approve", dag.ApprovalConfig{
    Timeout: 24 * time.Hour,
    Subject: "approval.safe-agent",
}).After(agent)

// Execute the approved plan
execute := wf.Planner("execute", "run-plan", dag.PlannerConfig{
    MaxSteps:     10,
    AllowedTasks: []string{"code-edit", "test-run"},
}).After(gate)

def, _ := wf.Build()
```

This three-phase pattern (reason, approve, execute) is the safest way to run LLM agents that take real-world actions. The agent can iterate freely during the reasoning phase. The approval gate is the checkpoint before anything irreversible happens.

## Timeout Design

| Component | Recommended Timeout | Rationale |
|-----------|-------------------|-----------|
| `WaitForSignal` in agent loop | 30-60 minutes | Human may be away; heartbeat to prevent redelivery |
| Approval gate | 4-24 hours | Async review; auto-reject on expiry |
| Per-iteration timeout | 60-120 seconds | Prevent hung LLM calls |

For `WaitForSignal`, remember to call `Heartbeat()` periodically if the wait exceeds the NATS AckWait period (typically 30 seconds). Or use `Pause()` to NAK with delay and resume when the human responds.

## Related

- [Approval Gates](/docs/step-types/approval-gates) -- step type mechanics and token validation
- [Signals](/docs/coordination/signals) -- WaitForSignal and SendSignal API
- [Agent Loop Pattern](/docs/ai-patterns/agent-loop-pattern) -- the reasoning cycle that gates protect
- [Cost and Safety Controls](/docs/ai-patterns/cost-and-safety-controls) -- complementary safety mechanisms
