package dag

import "encoding/json"

// ParentCond evaluates a simple comparison on a parent step's JSON output.
// Field is a top-level key in the output JSON object. Op is one of the six
// comparison operators: ==, !=, <, >, <=, >=. Value is the comparison target.
// Evaluated purely — no I/O, no side effects.
type ParentCond struct {
	StepID string      `json:"step_id"`
	Field  string      `json:"field"`
	Op     string      `json:"op"`
	Value  interface{} `json:"value"`
}

// validOps is the closed set of comparison operators. Validate rejects
// any ParentCond with an operator outside this set.
var validOps = map[string]bool{
	"==": true, "!=": true,
	"<": true, ">": true,
	"<=": true, ">=": true,
}

// Evaluate returns true when the condition is satisfied against the given
// step states. Returns false if the parent step has no output, the field
// is missing, or the types are not comparable.
func (c *ParentCond) Evaluate(steps map[string]StepState) bool {
	if c == nil {
		return false
	}
	state, ok := steps[c.StepID]
	if !ok || state.Output == nil {
		return false
	}
	var data map[string]interface{}
	if err := json.Unmarshal(state.Output, &data); err != nil {
		return false
	}
	fieldVal, ok := data[c.Field]
	if !ok {
		return false
	}
	return compareValues(fieldVal, c.Op, c.Value)
}

// compareValues performs the comparison. Both operands are compared as
// float64 for numeric types (JSON numbers unmarshal as float64) and as
// strings for string types. Mismatched types return false.
func compareValues(a interface{}, op string, b interface{}) bool {
	// Try numeric comparison first — JSON numbers are float64.
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if aOk && bOk {
		return compareFloat64(aNum, op, bNum)
	}

	// Fall back to string comparison.
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	if aOk && bOk {
		return compareString(aStr, op, bStr)
	}

	// Bool equality only.
	aBool, aOk := a.(bool)
	bBool, bOk := b.(bool)
	if aOk && bOk {
		switch op {
		case "==":
			return aBool == bBool
		case "!=":
			return aBool != bBool
		}
	}

	return false
}

func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

func compareFloat64(a float64, op string, b float64) bool {
	switch op {
	case "==":
		return a == b
	case "!=":
		return a != b
	case "<":
		return a < b
	case ">":
		return a > b
	case "<=":
		return a <= b
	case ">=":
		return a >= b
	}
	return false
}

func compareString(a, op, b string) bool {
	switch op {
	case "==":
		return a == b
	case "!=":
		return a != b
	case "<":
		return a < b
	case ">":
		return a > b
	case "<=":
		return a <= b
	case ">=":
		return a >= b
	}
	return false
}

// SkipIfOutput creates a ParentCond for use with StepRef.SkipIf.
// The parent must be in the step's DependsOn list (enforced by Validate).
func SkipIfOutput(parent StepRef, field, op string, value interface{}) *ParentCond {
	return &ParentCond{
		StepID: parent.ID(),
		Field:  field,
		Op:     op,
		Value:  value,
	}
}
