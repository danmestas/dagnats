package dag

import (
	"encoding/json"
	"fmt"
)

// RespondConfig is the configuration for a StepTypeRespond step
// per ADR-013. A respond step publishes the workflow's outward-facing
// HTTP response on dagnats.http.response.<run_id>. The step is a side
// effect, not a return: subsequent steps in the DAG continue to run
// after the response has been dispatched.
//
// Status defaults to 200 and ContentType defaults to "application/json"
// via Defaulted() — the engine applies defaults at execution time so
// authors can omit the boilerplate. BodyFrom is a dotpath into prior
// step output; empty means "use the immediate upstream step's output".
type RespondConfig struct {
	Status      int               `json:"status,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	BodyFrom    string            `json:"body_from,omitempty"`
	ContentType string            `json:"content_type,omitempty"`
}

// respondDefaultStatus is the HTTP status applied when RespondConfig
// leaves Status zero. 200 matches the most common synchronous endpoint
// shape; non-2xx is uncommon enough to require an explicit choice.
const respondDefaultStatus = 200

// respondDefaultContentType matches the default content-type used by
// dagnats's NATS payloads. Workflow authors return JSON-shaped data
// 99% of the time; making this the default removes a routine line of
// boilerplate.
const respondDefaultContentType = "application/json"

// Defaulted returns a copy of c with zero-valued fields filled from
// the documented defaults. Callers should use this exactly once at
// execute time; storing the defaulted copy back into the workflow
// definition would obscure the author's intent and lose the "default"
// signal on subsequent re-reads.
func (c RespondConfig) Defaulted() RespondConfig {
	// Status fed in must be either zero (use default) or a sane HTTP
	// code. Negative status leaks through ParseRespondConfig untouched
	// — assert it cannot happen so a malformed Config caught upstream
	// crashes loud at the boundary rather than reaching the wire.
	if c.Status < 0 {
		panic("RespondConfig.Defaulted: Status must not be negative")
	}
	if c.Status == 0 {
		c.Status = respondDefaultStatus
	}
	if c.ContentType == "" {
		c.ContentType = respondDefaultContentType
	}
	// Post-default invariant: every consumer of Defaulted() relies on
	// Status > 0 and ContentType != "" to publish a syntactically valid
	// HTTP response. Restate the guarantee explicitly so any future
	// change that re-introduces a "" content-type fails here, not on
	// the wire.
	if c.Status <= 0 {
		panic("RespondConfig.Defaulted: post-default Status must be positive")
	}
	if c.ContentType == "" {
		panic("RespondConfig.Defaulted: post-default ContentType must not be empty")
	}
	return c
}

// ParseRespondConfig extracts RespondConfig from a StepDef's Config
// field. Mirrors the other Parse*Config helpers in this package.
// Programmer-error invariants (empty step ID, mismatched type) trigger
// a panic — the workflow validator catches these one frame up; reaching
// this function with a malformed step means a registration path
// skipped Validate(), which is itself a bug to fix.
func ParseRespondConfig(step StepDef) (RespondConfig, error) {
	if step.ID == "" {
		panic("ParseRespondConfig: step.ID must not be empty")
	}
	if step.Type != StepTypeRespond {
		return RespondConfig{}, fmt.Errorf(
			"step %q: expected Respond, got %s",
			step.ID, step.Type,
		)
	}
	if step.Config == nil {
		return RespondConfig{}, fmt.Errorf(
			"step %q: Config is nil for Respond", step.ID,
		)
	}
	if len(step.Config) == 0 {
		// step.Config != nil but len 0 — this is an empty json.RawMessage,
		// not the "user omitted the field" shape (which is caught by the
		// nil check above). Reaching here means an upstream marshalling
		// path produced `{"config":""}` or `{"config":null}` literal,
		// which is a wire-shape bug rather than a workflow author choice.
		panic("ParseRespondConfig: step.Config has zero length but is non-nil")
	}
	var cfg RespondConfig
	if err := json.Unmarshal(step.Config, &cfg); err != nil {
		return RespondConfig{}, fmt.Errorf(
			"step %q: unmarshal RespondConfig: %w",
			step.ID, err,
		)
	}
	return cfg, nil
}
