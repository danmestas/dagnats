package journal

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
)

// errNoResponse is what ScriptedModel returns past its script.
var errNoResponse = errors.New("scripted model: no response left")

// ScriptedModel is a fake model for tests: it answers Model calls
// with its scripted responses in order and counts the requests it
// served, so a test can tell a reused response from a re-requested
// one. Safe for concurrent use.
type ScriptedModel struct {
	mu        sync.Mutex
	responses []json.RawMessage
	requests  int
}

// NewScriptedModel scripts the responses, in order.
func NewScriptedModel(responses ...[]byte) *ScriptedModel {
	if len(responses) == 0 {
		panic("NewScriptedModel: at least one response is required")
	}
	if len(responses) > callsMax {
		panic("NewScriptedModel: too many responses")
	}
	m := &ScriptedModel{responses: make([]json.RawMessage, 0, len(responses))}
	for _, r := range responses {
		m.responses = append(m.responses, json.RawMessage(r))
	}
	return m
}

// Respond is the ModelFunc: the next scripted response, or an error
// once the script is exhausted.
func (m *ScriptedModel) Respond(ctx context.Context) ([]byte, error) {
	if ctx == nil {
		panic("ScriptedModel.Respond: ctx must not be nil")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.requests < 0 {
		panic("ScriptedModel.Respond: requests must not be negative")
	}
	if m.requests >= len(m.responses) {
		return nil, errNoResponse
	}
	response := m.responses[m.requests]
	m.requests++
	return response, nil
}

// Requests is how many responses the model has served.
func (m *ScriptedModel) Requests() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}
