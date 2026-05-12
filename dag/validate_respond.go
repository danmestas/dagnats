package dag

// Warning is the structured result of a graph-level validation check
// per ADR-013 Layer 1. Warnings are surfaced through the workflow
// registration response — they do NOT fail the registration, because
// legitimate branch-per-outcome patterns can produce false positives
// and a hard rejection would block real use cases. Fatal field rules
// live in the per-trigger Validate() methods.
type Warning struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

// Warning kinds. Stable strings — consumers (registration response,
// CLI surfaces) may switch on these.
const (
	WarnMissingRespond   = "missing_respond"
	WarnDuplicateRespond = "duplicate_respond"
)

// validateRespondMaxSteps caps the input size to keep the validator's
// loops bounded. Workflows beyond this size are vanishingly rare and
// would be a separate validation failure anyway; the cap exists per
// TigerStyle "all loops bounded".
const validateRespondMaxSteps = 10000

// ValidateRespondReachability returns warnings (not errors) for the
// two ADR-013 graph problems unique to HTTP-triggered workflows:
//
//   - missing_respond — workflow declares an HTTP trigger but no
//     reachable terminal path contains a respond step. At runtime the
//     caller hangs until the 504 timeout fires.
//   - duplicate_respond — two respond steps are simultaneously
//     reachable on the same execution. The second publish has no
//     subscriber (subject already unsubscribed) and silently drops.
//
// hasHTTPTrigger gates the missing_respond check — a workflow without
// an HTTP trigger has no caller to leave hanging. duplicate_respond is
// always emitted, since two responds on the same run is wrong
// regardless of trigger kind (the second publish always drops).
//
// Returns a nil slice when there are no problems. Callers should
// distinguish nil from non-empty rather than relying on len().
func ValidateRespondReachability(
	def WorkflowDef, hasHTTPTrigger bool,
) []Warning {
	if len(def.Steps) > validateRespondMaxSteps {
		panic("ValidateRespondReachability: step count exceeds cap")
	}

	responds := findRespondSteps(def)

	if hasHTTPTrigger && len(responds) == 0 {
		return []Warning{{
			Kind: WarnMissingRespond,
			Message: "workflow has an HTTP trigger but no respond " +
				"step is reachable; calls will hang until timeout",
		}}
	}

	if len(responds) < 2 {
		return nil
	}

	if anyPairSimultaneous(def, responds) {
		return []Warning{{
			Kind: WarnDuplicateRespond,
			Message: "two or more respond steps are simultaneously " +
				"reachable; only the first publish is delivered",
		}}
	}
	return nil
}

// findRespondSteps returns the IDs of every StepTypeRespond in the
// workflow definition.
func findRespondSteps(def WorkflowDef) []string {
	out := make([]string, 0, len(def.Steps))
	for _, s := range def.Steps {
		if s.Type == StepTypeRespond {
			out = append(out, s.ID)
		}
	}
	return out
}

// anyPairSimultaneous returns true when at least one pair of respond
// step IDs is reachable on the same execution (i.e., not gated by
// mutually-exclusive SkipIf conditions on a shared ancestor).
func anyPairSimultaneous(def WorkflowDef, responds []string) bool {
	stepsByID := indexSteps(def)
	closures := make(map[string]map[string]bool, len(responds))
	for _, r := range responds {
		closures[r] = ancestorClosure(stepsByID, r)
	}
	for i := 0; i < len(responds); i++ {
		for j := i + 1; j < len(responds); j++ {
			if !areMutuallyExclusive(
				stepsByID, closures[responds[i]],
				closures[responds[j]],
			) {
				return true
			}
		}
	}
	return false
}

// indexSteps builds a stepID → StepDef lookup table.
func indexSteps(def WorkflowDef) map[string]StepDef {
	out := make(map[string]StepDef, len(def.Steps))
	for _, s := range def.Steps {
		out[s.ID] = s
	}
	return out
}

// ancestorClosure returns the set of step IDs that the given step
// transitively depends on, including itself. Iterative BFS upward
// through DependsOn — TigerStyle bans recursion. Bounded by step
// count.
func ancestorClosure(
	steps map[string]StepDef, startID string,
) map[string]bool {
	visited := make(map[string]bool, len(steps))
	queue := []string{startID}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if visited[id] {
			continue
		}
		visited[id] = true
		step, ok := steps[id]
		if !ok {
			continue
		}
		for _, dep := range step.DependsOn {
			if !visited[dep] {
				queue = append(queue, dep)
			}
		}
	}
	return visited
}

// areMutuallyExclusive returns true when at least one step in closure
// A has a SkipIf whose StepID + Field also appears as a SkipIf StepID
// + Field on a step in closure B, with a different (Op, Value)
// combination. This is the heuristic for "happy vs error" branching:
// each branch is gated on opposite predicates over the same parent
// step's output, so one of the two will be skipped on every run.
func areMutuallyExclusive(
	steps map[string]StepDef, a, b map[string]bool,
) bool {
	skipIfsA := collectSkipIfs(steps, a)
	skipIfsB := collectSkipIfs(steps, b)
	for _, sa := range skipIfsA {
		for _, sb := range skipIfsB {
			if sa.StepID != sb.StepID {
				continue
			}
			if sa.Field != sb.Field {
				continue
			}
			if sa.Op != sb.Op || sa.Value != sb.Value {
				return true
			}
		}
	}
	return false
}

// collectSkipIfs gathers every non-nil SkipIf on steps inside the
// given closure. The respond step itself rarely carries a SkipIf —
// authors gate the *path* to a respond via SkipIfs on upstream steps.
func collectSkipIfs(
	steps map[string]StepDef, closure map[string]bool,
) []ParentCond {
	out := make([]ParentCond, 0, len(closure))
	for id := range closure {
		step, ok := steps[id]
		if !ok {
			continue
		}
		if step.SkipIf == nil {
			continue
		}
		out = append(out, *step.SkipIf)
	}
	return out
}
