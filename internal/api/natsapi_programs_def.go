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
		cfg, err := dag.ParsePlannerConfig(step)
		if err != nil {
			return err
		}
		return confinePlanner(cfg, prefix)
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

// confinePlanner requires an explicit task list under the prefix: with
// none, the planner's fragments may name any task type at run time.
func confinePlanner(cfg dag.PlannerConfig, prefix string) error {
	if len(cfg.AllowedTasks) == 0 {
		return fmt.Errorf("planner needs allowed_tasks, each under %q", prefix)
	}
	for _, task := range cfg.AllowedTasks {
		if !underPrefix(task, prefix) {
			return fmt.Errorf("planner allows task %q, which is not under %q", task, prefix)
		}
	}
	return nil
}

// underPrefix reports whether name is "<prefix><something>".
func underPrefix(name, prefix string) bool {
	return len(name) > len(prefix) && strings.HasPrefix(name, prefix)
}
