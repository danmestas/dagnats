// agent/handler_test.go
// Tests for the agent Handler using mock worker context and LLM.
// Methodology: Each test verifies positive behavior and negative space.

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/danmestas/dagnats/agent/llm"
	"github.com/danmestas/dagnats/agent/tools"
	"github.com/danmestas/dagnats/observe"
)

// mockWorkerCtx implements WorkerContext for testing.
type mockWorkerCtx struct {
	input     []byte
	completed []byte
	continued []byte
	failed    error
	streamed  [][]byte
}

func (m *mockWorkerCtx) Input() []byte       { return m.input }
func (m *mockWorkerCtx) RunID() string       { return "test-run" }
func (m *mockWorkerCtx) StepID() string      { return "test-step" }
func (m *mockWorkerCtx) RetryCount() int     { return 0 }
func (m *mockWorkerCtx) Complete(output []byte) error {
	m.completed = output
	return nil
}
func (m *mockWorkerCtx) Fail(err error) error {
	m.failed = err
	return nil
}
func (m *mockWorkerCtx) Continue(output []byte) error {
	m.continued = output
	return nil
}
func (m *mockWorkerCtx) PutStream(data []byte) error {
	m.streamed = append(m.streamed, data)
	return nil
}

// mockLLMForHandler is a minimal LLM client for handler tests.
type mockLLMForHandler struct {
	responses []*llm.Response
	callIdx   int
}

func (m *mockLLMForHandler) SendMessage(
	ctx context.Context, req *llm.Request,
) (*llm.Response, error) {
	if m.callIdx >= len(m.responses) {
		return nil, fmt.Errorf("no more mock responses")
	}
	resp := m.responses[m.callIdx]
	m.callIdx++
	if req.StreamFunc != nil {
		for _, block := range resp.Content {
			if block.Type == "text" {
				req.StreamFunc(block.Text)
			}
		}
	}
	return resp, nil
}

func TestHandler_InitialPromptComplete(t *testing.T) {
	mockLLM := &mockLLMForHandler{
		responses: []*llm.Response{{
			Content: []llm.ContentBlock{
				{Type: "text", Text: "Analysis complete."},
			},
			StopReason: "end_turn",
		}},
	}
	llmReg := llm.NewProviderRegistry()
	llmReg.Register("test", func(apiKey string) llm.Client {
		return mockLLM
	})
	toolReg := tools.NewRegistry()
	configs := &MapConfigLoader{
		Configs: map[string]AgentConfig{
			"explorer": {
				Name:         "explorer",
				SystemPrompt: "Explore the codebase.",
				Model:        "test-model",
				Provider:     "test",
			},
		},
	}
	tel := observe.NewNoopTelemetry()
	handler := NewHandler(llmReg, toolReg, configs, tel)

	input, _ := json.Marshal(AgentTaskInput{
		AgentConfigName: "explorer",
		Prompt:          "Find all Go files",
	})
	wCtx := &mockWorkerCtx{input: input}

	err := handler.Handle(wCtx)
	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}
	if wCtx.completed == nil {
		t.Fatal("expected Complete to be called")
	}
	if wCtx.continued != nil {
		t.Fatal("expected Continue NOT to be called")
	}
}

func TestHandler_ContinueOnMaxTurns(t *testing.T) {
	// LLM always requests tool use, never completes.
	responses := make([]*llm.Response, 5)
	for i := range responses {
		responses[i] = &llm.Response{
			Content: []llm.ContentBlock{{
				Type:  "tool_use",
				ID:    fmt.Sprintf("call_%d", i),
				Name:  "echo",
				Input: json.RawMessage(`{}`),
			}},
			StopReason: "tool_use",
		}
	}
	mockLLM := &mockLLMForHandler{responses: responses}
	llmReg := llm.NewProviderRegistry()
	llmReg.Register("test", func(apiKey string) llm.Client {
		return mockLLM
	})
	toolReg := tools.NewRegistry()
	toolReg.Register(&echoTool{})
	configs := &MapConfigLoader{
		Configs: map[string]AgentConfig{
			"agent": {
				Name:         "agent",
				SystemPrompt: "Test.",
				Model:        "m",
				Provider:     "test",
				MaxTurns:     3,
				Tools:        []string{"echo"},
			},
		},
	}
	tel := observe.NewNoopTelemetry()
	handler := NewHandler(llmReg, toolReg, configs, tel)

	input, _ := json.Marshal(AgentTaskInput{
		AgentConfigName: "agent",
		Prompt:          "Keep going",
	})
	wCtx := &mockWorkerCtx{input: input}

	err := handler.Handle(wCtx)
	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}
	if wCtx.continued == nil {
		t.Fatal("expected Continue to be called")
	}
	if wCtx.completed != nil {
		t.Fatal("expected Complete NOT to be called")
	}
}

func TestHandler_StreamsTokens(t *testing.T) {
	mockLLM := &mockLLMForHandler{
		responses: []*llm.Response{{
			Content: []llm.ContentBlock{
				{Type: "text", Text: "streamed"},
			},
			StopReason: "end_turn",
		}},
	}
	llmReg := llm.NewProviderRegistry()
	llmReg.Register("test", func(apiKey string) llm.Client {
		return mockLLM
	})
	toolReg := tools.NewRegistry()
	configs := &MapConfigLoader{
		Configs: map[string]AgentConfig{
			"agent": {
				Name: "agent", SystemPrompt: "s",
				Model: "m", Provider: "test",
			},
		},
	}
	tel := observe.NewNoopTelemetry()
	handler := NewHandler(llmReg, toolReg, configs, tel)

	input, _ := json.Marshal(AgentTaskInput{
		AgentConfigName: "agent",
		Prompt:          "stream test",
	})
	wCtx := &mockWorkerCtx{input: input}
	err := handler.Handle(wCtx)
	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}
	if len(wCtx.streamed) == 0 {
		t.Fatal("expected streamed tokens")
	}
}

func TestHandler_ConfigNotFound(t *testing.T) {
	llmReg := llm.NewProviderRegistry()
	toolReg := tools.NewRegistry()
	configs := &MapConfigLoader{Configs: map[string]AgentConfig{}}
	tel := observe.NewNoopTelemetry()
	handler := NewHandler(llmReg, toolReg, configs, tel)

	input, _ := json.Marshal(AgentTaskInput{
		AgentConfigName: "nonexistent",
		Prompt:          "hello",
	})
	wCtx := &mockWorkerCtx{input: input}
	err := handler.Handle(wCtx)
	if err == nil {
		t.Fatal("expected error for missing config")
	}
}

// echoTool is a simple tool for testing.
type echoTool struct{}

func (e *echoTool) Name() string        { return "echo" }
func (e *echoTool) Description() string { return "echo" }
func (e *echoTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object"}`)
}
func (e *echoTool) Execute(
	ctx context.Context, input json.RawMessage,
) (json.RawMessage, error) {
	return json.RawMessage(`"echoed"`), nil
}
