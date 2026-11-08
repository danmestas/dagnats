// dag/fork.go
// Forked runs (#810). A fork is a new run of the parent's workflow, at
// the def version the parent was pinned to, with the parent's input.
// Every step the parent finished (completed or skipped) that is not
// the forked step, does not depend on it and is not an auxiliary
// handler is reused: its status and output are seeded into the new
// run, so it is not executed again. The forked step and everything
// downstream of it run afresh; the forked step's journal replays the
// parent's records up to the fork point (journal.Fork).
package dag

import "time"

// ForkOrigin names where a forked run came from: the parent run, the
// forked step and the parent journal sequence the fork replays to.
type ForkOrigin struct {
	RunID      string `json:"run_id"`
	StepID     string `json:"step_id"`
	JournalSeq uint64 `json:"journal_seq"`
}

// ForkStart is the "fork" section of a forked run's workflow.started
// payload: the origin and the parent step states the run reuses.
type ForkStart struct {
	Origin ForkOrigin           `json:"origin"`
	Reused map[string]StepState `json:"reused"`
}

// ForkReuse returns the parent's step states a fork at stepID reuses.
func ForkReuse(def WorkflowDef, parent WorkflowRun, stepID string) map[string]StepState {
	if stepID == "" {
		panic("ForkReuse: stepID must not be empty")
	}
	if len(def.Steps) == 0 {
		panic("ForkReuse: def must have steps")
	}
	downstream := dependentsOf(def, stepID)
	reused := map[string]StepState{}
	for _, step := range def.Steps {
		if step.ID == stepID || downstream[step.ID] || def.AuxSteps[step.ID] {
			continue
		}
		state, ok := parent.Steps[step.ID]
		if !ok {
			continue
		}
		switch state.Status {
		case StepStatusCompleted, StepStatusSkipped:
			reused[step.ID] = StepState{
				Status: state.Status, Output: state.Output, CompletedAt: state.CompletedAt,
			}
		}
	}
	return reused
}

// dependentsOf is every step that depends on stepID, directly or not.
// Iterative over an explicit queue; each step enters it at most once.
func dependentsOf(def WorkflowDef, stepID string) map[string]bool {
	if stepID == "" {
		panic("dependentsOf: stepID must not be empty")
	}
	if len(def.Steps) == 0 {
		panic("dependentsOf: def must have steps")
	}
	found := map[string]bool{}
	queue := []string{stepID}
	for i := 0; i < len(queue) && i <= len(def.Steps); i++ {
		for _, step := range def.Steps {
			if found[step.ID] || !dependsOn(step, queue[i]) {
				continue
			}
			found[step.ID] = true
			queue = append(queue, step.ID)
		}
	}
	return found
}

func dependsOn(step StepDef, id string) bool {
	for _, dep := range step.DependsOn {
		if dep == id {
			return true
		}
	}
	return false
}

// ApplyFork records the origin on a new run and seeds the reused step
// states. A reused state for a step the run does not have, or for the
// forked step itself, is ignored.
func ApplyFork(run *WorkflowRun, start ForkStart) {
	if run == nil {
		panic("ApplyFork: run must not be nil")
	}
	if start.Origin.RunID == "" || start.Origin.StepID == "" {
		panic("ApplyFork: origin must name a run and a step")
	}
	origin := start.Origin
	run.ForkedFrom = &origin
	for id, state := range start.Reused {
		if _, ok := run.Steps[id]; !ok || id == origin.StepID {
			continue
		}
		if state.Status != StepStatusCompleted && state.Status != StepStatusSkipped {
			continue
		}
		if state.CompletedAt == nil {
			now := time.Now().UTC()
			state.CompletedAt = &now
		}
		run.Steps[id] = StepState{
			Status: state.Status, Output: state.Output, CompletedAt: state.CompletedAt,
		}
	}
}
