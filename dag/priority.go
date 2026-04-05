// dag/priority.go
// Priority resolution for workflow run ordering.
package dag

import (
	"encoding/json"
	"fmt"
)

// PriorityConfig controls run ordering when concurrency limits
// create backlogs. Key is a dot-path into input data.
type PriorityConfig struct {
	Key           string         `json:"key"`
	Rules         map[string]int `json:"rules"`
	DefaultOffset int            `json:"default_offset"`
}

// ResolvePriority computes the priority offset from input data.
func ResolvePriority(
	cfg *PriorityConfig, input json.RawMessage,
) int {
	if cfg == nil {
		return 0
	}
	if cfg.Key == "" {
		return clampOffset(cfg.DefaultOffset)
	}
	val, err := ExtractDotPath(cfg.Key, input)
	if err != nil {
		return 0
	}
	strVal := fmt.Sprintf("%v", val)
	if offset, ok := cfg.Rules[strVal]; ok {
		return clampOffset(offset)
	}
	return clampOffset(cfg.DefaultOffset)
}

func clampOffset(offset int) int {
	if offset > 600 {
		return 600
	}
	if offset < -600 {
		return -600
	}
	return offset
}
