// protocol/run_event.go
// RunEvent is the payload for the run-lifecycle consumer contract
// published to the EVENTS stream (event.run.{workflow}.{runID}.{status}),
// distinct from protocol.Event on the WORKFLOW_HISTORY stream
// (history.{runID}). history.* carries the full per-step timeline;
// event.run.* carries ONLY the reliable "this run reached a terminal
// state, and it is durably persisted" signal a forge/poller needs
// instead of GET /runs/{id} polling. See docs/wire-protocol.md
// "Consumer contract: run lifecycle events".
package protocol

import "time"

// RunEventType discriminates the three terminal outcomes a run can
// reach. Coarser than dag.RunStatus (which also has Compensated /
// CompensateFailed) — compensation outcomes are reported as
// RunEventFailed since compensation only runs after the workflow
// itself failed; the exact status still rides the Status field.
type RunEventType string

const (
	RunEventCompleted RunEventType = "run.completed"
	RunEventFailed    RunEventType = "run.failed"
	RunEventCancelled RunEventType = "run.cancelled"
)

// RunEvent is the wire payload published to event.run.*. Status
// carries the precise dag.RunStatus string (e.g. "compensated") even
// when Type has coalesced to a coarser bucket. Labels is copied from
// the run's dag.WorkflowRun.Labels at finalization time (see
// copyLabels in internal/engine/run_event.go) — omitted from the
// wire payload when the run has none.
//
// Status is a plain string, not dag.RunStatus: protocol is the wire
// schema package and stays free of a dependency on dag's internal
// types, matching Event's existing payload-as-raw-JSON discipline.
type RunEvent struct {
	Type        RunEventType      `json:"type"`
	RunID       string            `json:"run_id"`
	WorkflowID  string            `json:"workflow_id"`
	Status      string            `json:"status"`
	CreatedAt   time.Time         `json:"created_at"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	TraceParent string            `json:"trace_parent,omitempty"`
	// TriggerDepth is the source run's dag.WorkflowRun.TriggerDepth,
	// copied here at finalization time (#634) so a run_terminal
	// trigger consuming this event can compute the depth of the run
	// it is about to start (source depth + 1) WITHOUT a second lookup
	// of the source run. Additive: omitempty, older events decode to
	// 0 which is also the correct depth for every manual/HTTP/cron
	// -started run.
	TriggerDepth int `json:"trigger_depth,omitempty"`
}
