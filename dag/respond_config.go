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
	if c.Status == 0 {
		c.Status = respondDefaultStatus
	}
	if c.ContentType == "" {
		c.ContentType = respondDefaultContentType
	}
	return c
}

// ParseRespondConfig extracts RespondConfig from a StepDef's Config
// field. Mirrors the other Parse*Config helpers in this package.
func ParseRespondConfig(step StepDef) (RespondConfig, error) {
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
	var cfg RespondConfig
	if err := json.Unmarshal(step.Config, &cfg); err != nil {
		return RespondConfig{}, fmt.Errorf(
			"step %q: unmarshal RespondConfig: %w",
			step.ID, err,
		)
	}
	return cfg, nil
}
