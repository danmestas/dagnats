// dag/duration_json.go
// Duration fields decode from either integer nanoseconds (what
// encoding/json writes for time.Duration, so every stored definition)
// or a Go duration string such as "30s" (what people write in workflow
// JSON); see internal/durationjson. A type that embeds one of these
// inherits its UnmarshalJSON and needs its own to decode extra fields.
package dag

import "github.com/danmestas/dagnats/internal/durationjson"

// UnmarshalJSON accepts duration fields as Go duration strings ("30s")
// or integer nanoseconds.
func (def *WorkflowDef) UnmarshalJSON(data []byte) error {
	type plain WorkflowDef
	return durationjson.Unmarshal(data, (*plain)(def))
}

// UnmarshalJSON accepts duration fields as Go duration strings ("30s")
// or integer nanoseconds.
func (step *StepDef) UnmarshalJSON(data []byte) error {
	type plain StepDef
	return durationjson.Unmarshal(data, (*plain)(step))
}

// UnmarshalJSON accepts duration fields as Go duration strings ("30s")
// or integer nanoseconds.
func (policy *RetryPolicy) UnmarshalJSON(data []byte) error {
	type plain RetryPolicy
	return durationjson.Unmarshal(data, (*plain)(policy))
}

// UnmarshalJSON accepts duration fields as Go duration strings ("30s")
// or integer nanoseconds.
func (cfg *AgentLoopConfig) UnmarshalJSON(data []byte) error {
	type plain AgentLoopConfig
	return durationjson.Unmarshal(data, (*plain)(cfg))
}

// UnmarshalJSON accepts duration fields as Go duration strings ("30s")
// or integer nanoseconds.
func (cfg *SleepConfig) UnmarshalJSON(data []byte) error {
	type plain SleepConfig
	return durationjson.Unmarshal(data, (*plain)(cfg))
}

// UnmarshalJSON accepts duration fields as Go duration strings ("30s")
// or integer nanoseconds.
func (cfg *ApprovalConfig) UnmarshalJSON(data []byte) error {
	type plain ApprovalConfig
	return durationjson.Unmarshal(data, (*plain)(cfg))
}

// UnmarshalJSON accepts duration fields as Go duration strings ("30s")
// or integer nanoseconds.
func (opts *WaitForEventOpts) UnmarshalJSON(data []byte) error {
	type plain WaitForEventOpts
	return durationjson.Unmarshal(data, (*plain)(opts))
}

// UnmarshalJSON accepts duration fields as Go duration strings ("30s")
// or integer nanoseconds.
func (cancel *CancelOn) UnmarshalJSON(data []byte) error {
	type plain CancelOn
	return durationjson.Unmarshal(data, (*plain)(cancel))
}

// UnmarshalJSON accepts duration fields as Go duration strings ("30s")
// or integer nanoseconds.
func (limit *RateLimit) UnmarshalJSON(data []byte) error {
	type plain RateLimit
	return durationjson.Unmarshal(data, (*plain)(limit))
}

// UnmarshalJSON accepts duration fields as Go duration strings ("30s")
// or integer nanoseconds.
func (limit *KeyedRateLimit) UnmarshalJSON(data []byte) error {
	type plain KeyedRateLimit
	return durationjson.Unmarshal(data, (*plain)(limit))
}
