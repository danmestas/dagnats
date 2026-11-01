package api

import (
	"fmt"
	"strings"

	"github.com/danmestas/dagnats/dag"
)

// confineDef refuses a definition a program registers through
// api.p.<name> that reaches outside "<name>.": its name, any step's task
// type (so a step cannot dispatch to another program's worker and read
// the output back through runs.get), a sub-workflow target, the tasks a
// planner may emit, and the event types it waits on or is cancelled by
// (event subjects are global). Prefix-only for now: declared protocols
// are not yet a way out.
func confineDef(def dag.WorkflowDef, prefix string) error {
	if prefix == "" || !strings.HasSuffix(prefix, ".") {
		panic("confineDef: prefix must be \"<name>.\"")
	}
	if !underPrefix(def.Name, prefix) {
		return fmt.Errorf("workflow %q is not under %q", def.Name, prefix)
	}
	for _, step := range def.Steps {
		if err := confineStep(step, prefix); err != nil {
			return fmt.Errorf("step %q: %w", step.ID, err)
		}
	}
	for _, cancel := range def.CancelOn {
		if !underPrefix(cancel.Event, prefix) {
			return fmt.Errorf("cancel_on event %q is not under %q", cancel.Event, prefix)
		}
	}
	return nil
}

// confineStep checks one step's task and the names its config reaches.
func confineStep(step dag.StepDef, prefix string) error {
	if prefix == "" {
		panic("confineStep: prefix must not be empty")
	}
	if step.Task != "" && !underPrefix(step.Task, prefix) {
		return fmt.Errorf("task %q is not under %q", step.Task, prefix)
	}
	switch step.Type {
	case dag.StepTypeSubWorkflow:
		cfg, err := dag.ParseSubWorkflowConfig(step)
		if err != nil {
			return err
		}
		if !underPrefix(cfg.Workflow, prefix) {
			return fmt.Errorf("starts workflow %q, which is not under %q", cfg.Workflow, prefix)
		}
	case dag.StepTypePlanner:
		// The engine checks a planner's run-time fragment only against
		// allowed_tasks, not the fragment steps' types or configs, so a
		// fragment could still start another program's workflow or wait
		// on its events. Refused until the engine confines fragments.
		return fmt.Errorf("planner steps are not available to programs yet: " +
			"their run-time fragments are not confined to the program's prefix")
	case dag.StepTypeWaitForEvent:
		cfg, err := dag.ParseWaitForEventConfig(step)
		if err != nil {
			return err
		}
		if !underPrefix(cfg.Event, prefix) {
			return fmt.Errorf("waits for event %q, which is not under %q", cfg.Event, prefix)
		}
	}
	return nil
}

// underPrefix reports whether name is "<prefix><something>".
func underPrefix(name, prefix string) bool {
	return len(name) > len(prefix) && strings.HasPrefix(name, prefix)
}
