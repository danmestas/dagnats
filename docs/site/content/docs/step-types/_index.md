---
title: "Step Types"
weight: 3
---

{{< cards cols="3" >}}
  {{< card link="agent-loops" title="Agent Loops" >}}
  {{< card link="approval-gates" title="Approval Gates" >}}
  {{< card link="map-steps" title="Map Steps" >}}
  {{< card link="normal-steps" title="Normal Steps" >}}
  {{< card link="planner-steps" title="Planner Steps" >}}
  {{< card link="respond-steps" title="Respond Steps" >}}
  {{< card link="sleep-and-timers" title="Sleep and Timers" >}}
  {{< card link="sub-workflows" title="Sub-Workflows" >}}
  {{< card link="wait-for-event" title="Wait for Event" >}}
{{< /cards >}}

A step's `type` field in workflow JSON is one of ten values. Five of them (`sleep`, `wait_for_event`, `approval`, `sub_workflow`, `respond`) are run by the engine with no worker. The others go to a worker as tasks.

| `type` | Runs on | Page |
|--------|---------|------|
| `normal` (default) | Worker | [Normal Steps](/docs/step-types/normal-steps) |
| `agent_loop` | Worker, repeated | [Agent Loops](/docs/step-types/agent-loops) |
| `agent` | Worker, like `normal`. The step's `metadata` carries agent settings for an agent SDK to read. | [Normal Steps](/docs/step-types/normal-steps) |
| `map` | Worker, once per array item | [Map Steps](/docs/step-types/map-steps) |
| `planner` | Worker, then the engine adds the generated steps | [Planner Steps](/docs/step-types/planner-steps) |
| `sleep` | Engine | [Sleep and Timers](/docs/step-types/sleep-and-timers) |
| `wait_for_event` | Engine | [Wait for Event](/docs/step-types/wait-for-event) |
| `approval` | Engine, completed by an API call | [Approval Gates](/docs/step-types/approval-gates) |
| `sub_workflow` | Engine, which starts a child run | [Sub-Workflows](/docs/step-types/sub-workflows) |
| `respond` | Engine | [Respond Steps](/docs/step-types/respond-steps) |

An unknown `type` is an error: `workflow validate` and `workflow register` report `unknown step type`.
