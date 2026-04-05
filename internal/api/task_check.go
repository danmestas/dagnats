// api/task_check.go
// Validates that workflow task types have active JetStream consumers on
// the TASK_QUEUES stream. Returns unmatched task types as warnings --
// registration still succeeds because workers may appear later.
package api

import (
	"context"
	"sort"
	"strings"

	"github.com/danmestas/dagnats/dag"
	"github.com/nats-io/nats.go/jetstream"
)

// CheckTaskConsumers returns task types from the workflow definition
// that have no active consumer on the TASK_QUEUES stream. Used as a
// warning at registration time -- missing consumers may appear later.
func CheckTaskConsumers(
	js jetstream.JetStream, def dag.WorkflowDef,
) []string {
	if js == nil {
		panic("CheckTaskConsumers: js must not be nil")
	}
	if len(def.Steps) == 0 {
		panic("CheckTaskConsumers: def must have steps")
	}

	taskTypes := collectTaskTypes(def)
	activeTypes := listActiveTaskTypes(js)
	return findMissingTypes(taskTypes, activeTypes)
}

// collectTaskTypes extracts unique task type strings from workflow
// steps. Returns a sorted slice for deterministic output.
func collectTaskTypes(def dag.WorkflowDef) []string {
	if len(def.Steps) == 0 {
		panic("collectTaskTypes: def must have steps")
	}
	if len(def.Steps) > 10000 {
		panic("collectTaskTypes: step count exceeds max bound")
	}

	seen := make(map[string]struct{}, len(def.Steps))
	for _, step := range def.Steps {
		seen[step.Task] = struct{}{}
	}

	types := make([]string, 0, len(seen))
	for t := range seen {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

// listActiveTaskTypes enumerates consumers on TASK_QUEUES and
// extracts the task type from each consumer's FilterSubject.
// Returns a set (map) for O(1) lookup.
func listActiveTaskTypes(
	js jetstream.JetStream,
) map[string]struct{} {
	if js == nil {
		panic("listActiveTaskTypes: js must not be nil")
	}

	active := make(map[string]struct{})
	const maxConsumers = 10000

	ctx := context.Background()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		return active
	}

	lister := stream.ListConsumers(ctx)
	count := 0
	for info := range lister.Info() {
		if count >= maxConsumers {
			break
		}
		count++
		subject := info.Config.FilterSubject
		taskType := extractTaskType(subject)
		if taskType != "" {
			active[taskType] = struct{}{}
		}
	}
	return active
}

// extractTaskType parses a filter subject like "task.greet.>" or
// "task.greet.*" and returns the task type ("greet"). Returns ""
// for subjects that do not match the expected pattern.
func extractTaskType(subject string) string {
	if len(subject) < 6 {
		return ""
	}
	if subject[:5] != "task." {
		return ""
	}

	// Strip the "task." prefix and the trailing ".>" or ".*"
	rest := subject[5:]
	if idx := strings.LastIndex(rest, "."); idx > 0 {
		return rest[:idx]
	}
	// Consumer might filter on "task.greet" without wildcard
	return rest
}

// findMissingTypes returns task types from wanted that are not
// present in the active set. Output is sorted for determinism.
func findMissingTypes(
	wanted []string, active map[string]struct{},
) []string {
	if len(wanted) > 10000 {
		panic("findMissingTypes: wanted exceeds max bound")
	}
	if active == nil {
		panic("findMissingTypes: active must not be nil")
	}

	missing := make([]string, 0, len(wanted))
	for _, t := range wanted {
		if _, ok := active[t]; !ok {
			missing = append(missing, t)
		}
	}
	return missing
}
