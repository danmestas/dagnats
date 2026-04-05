package dag

import (
	"fmt"
	"strings"
	"time"
)

// MatchOp defines comparison operators for event matching.
type MatchOp string

const MatchOpEq MatchOp = "eq"

// Match is the builder-time type. Both sides are dot-path strings.
type Match struct {
	Left  string  `json:"left"`
	Op    MatchOp `json:"op"`
	Right string  `json:"right"`
}

// ResolvedMatch is the runtime type stored in KV waiter entries.
// Right is resolved to a concrete value when the waiter is created.
type ResolvedMatch struct {
	Left  string  `json:"left"`
	Op    MatchOp `json:"op"`
	Right any     `json:"right"`
}

// Evaluate checks if the match condition holds against event JSON data.
func (m ResolvedMatch) Evaluate(eventData []byte) (bool, error) {
	if m.Left == "" {
		panic("ResolvedMatch.Evaluate: Left must not be empty")
	}
	if m.Op == "" {
		panic("ResolvedMatch.Evaluate: Op must not be empty")
	}
	leftVal, err := ExtractDotPath(m.Left, eventData)
	if err != nil {
		return false, nil
	}
	switch m.Op {
	case MatchOpEq:
		return fmt.Sprintf("%v", leftVal) == fmt.Sprintf("%v", m.Right), nil
	default:
		return false, fmt.Errorf("unknown match op: %s", m.Op)
	}
}

// Resolve converts a builder-time Match to a runtime ResolvedMatch.
func (m Match) Resolve(
	stepOutputs map[string][]byte,
	workflowInput []byte,
) (ResolvedMatch, error) {
	if m.Right == "" {
		panic("Match.Resolve: Right must not be empty")
	}
	if m.Left == "" {
		panic("Match.Resolve: Left must not be empty")
	}

	var val any
	var err error

	if strings.HasPrefix(m.Right, "step.") {
		val, err = resolveStepPath(m.Right, stepOutputs)
		if err != nil {
			return ResolvedMatch{}, err
		}
	} else if strings.HasPrefix(m.Right, "input.") {
		val, err = resolveInputPath(m.Right, workflowInput)
		if err != nil {
			return ResolvedMatch{}, err
		}
	} else {
		return ResolvedMatch{}, fmt.Errorf("unknown path prefix in %s", m.Right)
	}

	return ResolvedMatch{Left: m.Left, Op: m.Op, Right: val}, nil
}

func resolveStepPath(path string, stepOutputs map[string][]byte) (any, error) {
	if !strings.HasPrefix(path, "step.") {
		panic("resolveStepPath: path must start with step.")
	}
	if stepOutputs == nil {
		panic("resolveStepPath: stepOutputs must not be nil")
	}

	parts := strings.SplitN(path, ".", 4)
	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid step path: %s", path)
	}
	data, ok := stepOutputs[parts[1]]
	if !ok {
		return nil, fmt.Errorf("step %s has no output", parts[1])
	}
	val, err := ExtractDotPath(parts[3], data)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", path, err)
	}
	return val, nil
}

func resolveInputPath(path string, workflowInput []byte) (any, error) {
	if !strings.HasPrefix(path, "input.") {
		panic("resolveInputPath: path must start with input.")
	}
	if workflowInput == nil {
		panic("resolveInputPath: workflowInput must not be nil")
	}

	pathStr := strings.TrimPrefix(path, "input.")
	val, err := ExtractDotPath(pathStr, workflowInput)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", path, err)
	}
	return val, nil
}

// WaitForEventOpts configures a wait-for-event step.
type WaitForEventOpts struct {
	Event   string        `json:"event"`
	Match   Match         `json:"match"`
	Timeout time.Duration `json:"timeout"`
}

func validateWaitForEventStep(step StepDef, ids map[string]bool) error {
	if step.Type != StepTypeWaitForEvent {
		return nil
	}
	if ids == nil {
		panic("validateWaitForEventStep: ids must not be nil")
	}
	if step.ID == "" {
		panic("validateWaitForEventStep: step ID must not be empty")
	}

	opts, err := ParseWaitForEventConfig(step)
	if err != nil {
		return fmt.Errorf(
			"step %q: WaitForEvent config is nil", step.ID)
	}
	if opts.Event == "" {
		return fmt.Errorf(
			"step %q: WaitForEvent.Event must not be empty",
			step.ID)
	}
	if opts.Match.Left == "" {
		return fmt.Errorf(
			"step %q: WaitForEvent.Match.Left must not be empty",
			step.ID)
	}
	if opts.Match.Op == "" {
		return fmt.Errorf(
			"step %q: WaitForEvent.Match.Op must not be empty",
			step.ID)
	}
	if opts.Match.Right == "" {
		return fmt.Errorf(
			"step %q: WaitForEvent.Match.Right must not be empty",
			step.ID)
	}
	if opts.Timeout <= 0 {
		return fmt.Errorf(
			"step %q: WaitForEvent.Timeout must be positive",
			step.ID)
	}

	if err := validateMatchDotPaths(
		step.ID, opts.Match, ids,
	); err != nil {
		return err
	}

	return nil
}

func validateMatchDotPaths(stepID string, match Match, ids map[string]bool) error {
	if stepID == "" {
		panic("validateMatchDotPaths: stepID must not be empty")
	}
	if ids == nil {
		panic("validateMatchDotPaths: ids must not be nil")
	}

	if strings.HasPrefix(match.Right, "step.") {
		parts := strings.SplitN(match.Right, ".", 4)
		if len(parts) < 4 {
			return fmt.Errorf(
				"step %q: invalid step path %q (expected step.{id}.output.{field})",
				stepID, match.Right)
		}
		refID := parts[1]
		if !ids[refID] {
			return fmt.Errorf(
				"step %q: Match.Right references step %q which does not exist",
				stepID, refID)
		}
	} else if !strings.HasPrefix(match.Right, "input.") {
		return fmt.Errorf(
			"step %q: Match.Right must start with step. or input., got %q",
			stepID, match.Right)
	}

	return nil
}
