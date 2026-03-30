// agent/runner_test.go
// Tests for the agent Runner using mock LLM and tool executors.
// Methodology: Each test verifies positive behavior and negative space.

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/danmestas/dagnats/agent/llm"
	"github.com/danmestas/dagnats/observe"
)

// mockLLM is a test double for llm.Client that returns preconfigured
// responses in sequence.
type mockLLM struct {
	responses []*llm.Response
	callCount int
}

func (m *mockLLM) SendMessage(
	ctx context.Context,
	req *llm.Request,
) (*llm.Response, error) {
	if m.callCount >= len(m.responses) {
		return nil, fmt.Errorf("no more mock responses")
	}
	resp := m.responses[m.callCount]
	m.callCount++
	// Call stream func with text content for coverage.
	if req.StreamFunc != nil {
		for _, block := range resp.Content {
			if block.Type == "text" {
				req.StreamFunc(block.Text)
			}
		}
	}
	return resp, nil
}

// mockTools is a test double for ToolExecutor that records calls
// and returns preconfigured results.
type mockTools struct {
	calls   []toolCall
	results map[string]json.RawMessage
	defs    []llm.ToolDef
}

type toolCall struct {
	Name  string
	Input json.RawMessage
}

func (m *mockTools) Execute(
	ctx context.Context,
	name string,
	input json.RawMessage,
) (json.RawMessage, error) {
	m.calls = append(m.calls, toolCall{Name: name, Input: input})
	if result, ok := m.results[name]; ok {
		return result, nil
	}
	return json.RawMessage(`"ok"`), nil
}

func (m *mockTools) ListToolDefs(names []string) []llm.ToolDef {
	return m.defs
}

func testConfig() AgentConfig {
	return AgentConfig{
		Name:         "test",
		SystemPrompt: "You are a test agent.",
		Model:        "test-model",
		Provider:     "test",
		MaxTurns:     10,
		Tools:        []string{"read_file"},
	}
}

func TestRunner_SimpleTextResponse(t *testing.T) {
	llm := &mockLLM{
		responses: []*llm.Response{{
			Content: []llm.ContentBlock{
				{Type: "text", Text: "Hello world!"},
			},
			StopReason: "end_turn",
			Usage:      llm.Usage{InputTokens: 10, OutputTokens: 5},
		}},
	}
	tools := &mockTools{}
	tel := observe.NewNoopTelemetry()
	runner := NewRunner(llm, tools, tel)

	state := NewConversationState("test", "Say hello")
	input, err := state.Marshal()
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}

	result, err := runner.Run(
		context.Background(), testConfig(), input, nil,
	)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if !result.Done {
		t.Fatal("expected Done=true for end_turn response")
	}
	if result.FinalOutput == nil {
		t.Fatal("expected non-nil FinalOutput")
	}
	if result.TotalUsage.InputTokens != 10 {
		t.Fatalf(
			"expected 10 input tokens, got %d",
			result.TotalUsage.InputTokens,
		)
	}
}

func TestRunner_ToolUseLoop(t *testing.T) {
	llm := &mockLLM{
		responses: []*llm.Response{
			{
				// First response: request a tool call
				Content: []llm.ContentBlock{
					{
						Type:  "tool_use",
						ID:    "call_1",
						Name:  "read_file",
						Input: json.RawMessage(`{"path":"main.go"}`),
					},
				},
				StopReason: "tool_use",
				Usage:      llm.Usage{InputTokens: 10, OutputTokens: 20},
			},
			{
				// Second response: final text
				Content: []llm.ContentBlock{
					{Type: "text", Text: "The file contains a main function."},
				},
				StopReason: "end_turn",
				Usage:      llm.Usage{InputTokens: 30, OutputTokens: 15},
			},
		},
	}
	tools := &mockTools{
		results: map[string]json.RawMessage{
			"read_file": json.RawMessage(`"package main\nfunc main() {}"`),
		},
	}
	tel := observe.NewNoopTelemetry()
	runner := NewRunner(llm, tools, tel)

	state := NewConversationState("test", "Read main.go")
	input, err := state.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	result, err := runner.Run(
		context.Background(), testConfig(), input, nil,
	)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if !result.Done {
		t.Fatal("expected Done=true after tool loop completes")
	}
	// Verify the tool was called exactly once.
	if len(tools.calls) != 1 {
		t.Fatalf("expected 1 tool call, got %d", len(tools.calls))
	}
	if tools.calls[0].Name != "read_file" {
		t.Fatalf("expected 'read_file', got %q", tools.calls[0].Name)
	}
	// Verify accumulated usage.
	if result.TotalUsage.InputTokens != 40 {
		t.Fatalf(
			"expected 40 input tokens, got %d",
			result.TotalUsage.InputTokens,
		)
	}
}

func TestRunner_MaxTurnsProducesContinue(t *testing.T) {
	// LLM always requests tool use, never produces end_turn.
	responses := make([]*llm.Response, 5)
	for i := range responses {
		responses[i] = &llm.Response{
			Content: []llm.ContentBlock{{
				Type:  "tool_use",
				ID:    fmt.Sprintf("call_%d", i),
				Name:  "read_file",
				Input: json.RawMessage(`{"path":"f.go"}`),
			}},
			StopReason: "tool_use",
			Usage:      llm.Usage{InputTokens: 5, OutputTokens: 5},
		}
	}
	llm := &mockLLM{responses: responses}
	tools := &mockTools{}
	tel := observe.NewNoopTelemetry()
	runner := NewRunner(llm, tools, tel)

	cfg := testConfig()
	cfg.MaxTurns = 3 // Only allow 3 turns.

	state := NewConversationState("test", "keep going")
	input, err := state.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	result, err := runner.Run(
		context.Background(), cfg, input, nil,
	)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if result.Done {
		t.Fatal("expected Done=false when max turns reached")
	}
	if result.FinalOutput != nil {
		t.Fatal("expected nil FinalOutput for continue")
	}
	// Verify the LLM was called exactly 3 times.
	if llm.callCount != 3 {
		t.Fatalf("expected 3 LLM calls, got %d", llm.callCount)
	}
}

func TestRunner_StreamFuncCalled(t *testing.T) {
	llm := &mockLLM{
		responses: []*llm.Response{{
			Content: []llm.ContentBlock{
				{Type: "text", Text: "streamed output"},
			},
			StopReason: "end_turn",
		}},
	}
	tools := &mockTools{}
	tel := observe.NewNoopTelemetry()
	runner := NewRunner(llm, tools, tel)

	state := NewConversationState("test", "stream test")
	input, err := state.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var streamed []string
	_, err = runner.Run(
		context.Background(), testConfig(), input,
		func(token string) { streamed = append(streamed, token) },
	)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if len(streamed) != 1 {
		t.Fatalf("expected 1 streamed token, got %d", len(streamed))
	}
	if streamed[0] != "streamed output" {
		t.Fatalf("expected 'streamed output', got %q", streamed[0])
	}
}

func TestRunner_ConversationStatePreserved(t *testing.T) {
	llm := &mockLLM{
		responses: []*llm.Response{{
			Content: []llm.ContentBlock{
				{Type: "text", Text: "done"},
			},
			StopReason: "end_turn",
		}},
	}
	tools := &mockTools{}
	tel := observe.NewNoopTelemetry()
	runner := NewRunner(llm, tools, tel)

	state := NewConversationState("test", "preserve state")
	input, err := state.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	result, err := runner.Run(
		context.Background(), testConfig(), input, nil,
	)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}

	// Verify the output can be deserialized back.
	got, err := UnmarshalConversationState(result.Output)
	if err != nil {
		t.Fatalf("unmarshal result state: %v", err)
	}
	// Should have: original user message + assistant response.
	if len(got.Messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(got.Messages))
	}
	if got.TurnCount != 1 {
		t.Fatalf("expected turn count 1, got %d", got.TurnCount)
	}
}

func TestNewRunner_PanicsOnNilArgs(t *testing.T) {
	tel := observe.NewNoopTelemetry()
	cases := []struct {
		name string
		fn   func()
	}{
		{"nil llm", func() { NewRunner(nil, &mockTools{}, tel) }},
		{"nil tools", func() {
			NewRunner(&mockLLM{}, nil, tel)
		}},
		{"nil tel", func() {
			NewRunner(&mockLLM{}, &mockTools{}, nil)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic")
				}
			}()
			tc.fn()
		})
	}
}
