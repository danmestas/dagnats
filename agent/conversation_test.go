// agent/conversation_test.go
// Tests for ConversationState construction, mutation, and serialization.
// Methodology: Each test verifies positive behavior and negative space.

package agent

import (
	"encoding/json"
	"testing"
)

func TestNewConversationState(t *testing.T) {
	state := NewConversationState("explorer", "find all Go files")
	if state.AgentConfig != "explorer" {
		t.Fatalf("expected agent config 'explorer', got %q",
			state.AgentConfig)
	}
	if len(state.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(state.Messages))
	}
	if state.Messages[0].Role != "user" {
		t.Fatalf("expected role 'user', got %q",
			state.Messages[0].Role)
	}
	if state.TurnCount != 0 {
		t.Fatalf("expected turn count 0, got %d", state.TurnCount)
	}
}

func TestNewConversationState_PanicsOnEmpty(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty config name")
		}
	}()
	NewConversationState("", "prompt")
}

func TestNewConversationState_PanicsOnEmptyPrompt(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty prompt")
		}
	}()
	NewConversationState("agent", "")
}

func TestAppendAssistant(t *testing.T) {
	state := NewConversationState("agent", "hello")
	blocks := []ContentBlock{
		{Type: "text", Text: "Hi there!"},
	}
	state.AppendAssistant(blocks)
	if len(state.Messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(state.Messages))
	}
	if state.Messages[1].Role != "assistant" {
		t.Fatalf("expected role 'assistant', got %q",
			state.Messages[1].Role)
	}
	if state.TurnCount != 1 {
		t.Fatalf("expected turn count 1, got %d", state.TurnCount)
	}
}

func TestAppendAssistant_PanicsOnEmpty(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty content")
		}
	}()
	state := NewConversationState("agent", "hello")
	state.AppendAssistant(nil)
}

func TestAppendToolResults(t *testing.T) {
	state := NewConversationState("agent", "hello")
	results := []ToolResult{
		{ToolUseID: "t1", Content: "file contents"},
		{ToolUseID: "t2", Content: "Error: not found", IsError: true},
	}
	state.AppendToolResults(results)
	if len(state.Messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(state.Messages))
	}
	if state.Messages[1].Role != "user" {
		t.Fatalf("expected role 'user' for tool results, got %q",
			state.Messages[1].Role)
	}
	// Verify the results round-trip through JSON.
	var decoded []ToolResult
	if err := json.Unmarshal(state.Messages[1].Content, &decoded); err != nil {
		t.Fatalf("unmarshal tool results: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("expected 2 results, got %d", len(decoded))
	}
	if !decoded[1].IsError {
		t.Fatal("expected second result to be an error")
	}
}

func TestAddArtifact(t *testing.T) {
	state := NewConversationState("agent", "hello")
	state.AddArtifact("src/main.go")
	state.AddArtifact("src/test.go")
	if len(state.Artifacts) != 2 {
		t.Fatalf("expected 2 artifacts, got %d", len(state.Artifacts))
	}
	if state.Artifacts[0] != "src/main.go" {
		t.Fatalf("expected 'src/main.go', got %q", state.Artifacts[0])
	}
}

func TestAddArtifact_PanicsOnEmpty(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty ref")
		}
	}()
	state := NewConversationState("agent", "hello")
	state.AddArtifact("")
}

func TestMarshalUnmarshalConversationState(t *testing.T) {
	state := NewConversationState("round-trip", "test prompt")
	state.AppendAssistant([]ContentBlock{
		{Type: "text", Text: "response"},
	})
	state.AddArtifact("file.go")

	data, err := state.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	got, err := UnmarshalConversationState(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if got.AgentConfig != "round-trip" {
		t.Fatalf("config name mismatch: %q", got.AgentConfig)
	}
	if len(got.Messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(got.Messages))
	}
	if len(got.Artifacts) != 1 {
		t.Fatalf("expected 1 artifact, got %d", len(got.Artifacts))
	}
	if got.TurnCount != 1 {
		t.Fatalf("expected turn count 1, got %d", got.TurnCount)
	}
}

func TestIsLargePayload(t *testing.T) {
	small := make([]byte, PayloadSizeThreshold-1)
	if IsLargePayload(small) {
		t.Fatal("expected small payload to not be large")
	}
	large := make([]byte, PayloadSizeThreshold+1)
	if !IsLargePayload(large) {
		t.Fatal("expected large payload to be large")
	}
}
