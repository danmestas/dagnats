// internal/engine/fork.go
// Forked runs (#810): the API decides which parent steps a fork reuses
// and puts them in the start payload's "fork" section, so the event in
// history says exactly what the run started from and a redelivered
// start seeds the same steps.
package engine

import (
	"encoding/json"

	"github.com/danmestas/dagnats/dag"
)

// applyForkStart seeds a forked run from its start payload. A payload
// without a fork section, or one that does not decode, is a normal
// start: the run is left as built.
func applyForkStart(run *dag.WorkflowRun, payload []byte) {
	if run == nil {
		panic("applyForkStart: run must not be nil")
	}
	if run.ForkedFrom != nil {
		panic("applyForkStart: run is already forked")
	}
	var start struct {
		Fork *dag.ForkStart `json:"fork"`
	}
	if err := json.Unmarshal(payload, &start); err != nil || start.Fork == nil {
		return
	}
	if start.Fork.Origin.RunID == "" || start.Fork.Origin.StepID == "" {
		return
	}
	dag.ApplyFork(run, *start.Fork)
}
