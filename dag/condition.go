package dag

import (
	"cmp"
	"encoding/json"
)

// ParentCond evaluates a simple comparison on a parent step's JSON
// output. Field is a top-level key. Op is one of: ==, !=, <, >, <=,
// >=. Evaluated purely — no I/O, no side effects.
type ParentCond struct {
	StepID string      `json:"step_id"`
	Field  string      `json:"field"`
	Op     string      `json:"op"`
	Value  interface{} `json:"value"`
}

// validOps is the closed set of comparison operators.
var validOps = map[string]bool{
	"==": true, "!=": true,
	"<": true, ">": true,
	"<=": true, ">=": true,
}

// Evaluate returns true when the condition is satisfied. Returns false
// if the parent step has no output, the field is missing, or the types
// are not comparable.
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

func compareValues(a interface{}, op string, b interface{}) bool {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if aOk && bOk {
		return compareOrdered(aNum, op, bNum)
	}
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	if aOk && bOk {
		return compareOrdered(aStr, op, bStr)
	}
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

// compareOrdered handles all ordered types (float64, string) with a
// single generic function, eliminating the duplicated switch logic.
func compareOrdered[T cmp.Ordered](a T, op string, b T) bool {
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
// Panics on invalid inputs — catch errors at construction time.
func SkipIfOutput(
	parent StepRef, field, op string, value interface{},
) *ParentCond {
	if field == "" {
		panic("SkipIfOutput: field must not be empty")
	}
	if !validOps[op] {
		panic("SkipIfOutput: invalid operator: " + op)
	}
	return &ParentCond{
		StepID: parent.ID(),
		Field:  field,
		Op:     op,
		Value:  value,
	}
}
