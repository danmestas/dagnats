// agent/conversation.go

// ConversationState is the serializable state passed between agent loop
// iterations via worker.TaskContext.Continue(). It holds the full message
// history, turn count, and references to artifacts produced by the agent.
package agent

import (
	"encoding/json"
	"fmt"
)

// ConversationState captures everything needed to resume an agent loop
// iteration. Serialized to JSON and passed as the Continue() payload.
// When the serialized size exceeds PayloadSizeThreshold, the caller
// stores it in Object Store and passes a PayloadRef instead.
type ConversationState struct {
	Messages    []Message `json:"messages"`
	TurnCount   int       `json:"turn_count"`
	Artifacts   []string  `json:"artifacts,omitempty"`
	AgentConfig string    `json:"agent_config"`
}

// Message is a single entry in the conversation history. Role is one
// of "user", "assistant", or "tool". Content is the raw JSON matching
// the LLM provider's format (text blocks, tool_use, tool_result, etc).
type Message struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

// ContentBlock represents a typed block within an assistant message.
// The LLM may return text and/or tool_use blocks in a single response.
type ContentBlock struct {
	Type  string          `json:"type"`
	Text  string          `json:"text,omitempty"`
	ID    string          `json:"id,omitempty"`
	Name  string          `json:"name,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`
}

// ToolResult is the content of a "tool" role message — the result
// of executing a tool call requested by the assistant.
type ToolResult struct {
	ToolUseID string `json:"tool_use_id"`
	Content   string `json:"content"`
	IsError   bool   `json:"is_error,omitempty"`
}

// PayloadSizeThreshold is the maximum size (bytes) for inline payloads
// in NATS messages. Payloads larger than this are stored in Object Store
// and referenced by key.
const PayloadSizeThreshold = 768 * 1024 // 768KB (under 1MB NATS limit)

// PayloadRef is a reference to a payload that may be inline or stored
// in NATS Object Store. Used for conversation states that exceed the
// NATS message size limit.
type PayloadRef struct {
	Inline bool   `json:"inline"`
	Data   []byte `json:"data,omitempty"`
	Bucket string `json:"bucket,omitempty"`
	Key    string `json:"key,omitempty"`
}

// NewConversationState creates an initial ConversationState with the
// user's prompt as the first message. agentConfigName identifies which
// AgentConfig to load for this conversation.
func NewConversationState(
	agentConfigName string, prompt string,
) ConversationState {
	if agentConfigName == "" {
		panic("NewConversationState: agentConfigName must not be empty")
	}
	if prompt == "" {
		panic("NewConversationState: prompt must not be empty")
	}
	content, err := json.Marshal(prompt)
	if err != nil {
		panic("NewConversationState: marshal prompt: " + err.Error())
	}
	return ConversationState{
		Messages: []Message{
			{Role: "user", Content: content},
		},
		TurnCount:   0,
		AgentConfig: agentConfigName,
	}
}

// AppendAssistant adds an assistant message to the conversation.
func (s *ConversationState) AppendAssistant(content []ContentBlock) {
	if len(content) == 0 {
		panic("AppendAssistant: content must not be empty")
	}
	data, err := json.Marshal(content)
	if err != nil {
		panic("AppendAssistant: marshal content: " + err.Error())
	}
	s.Messages = append(s.Messages, Message{
		Role: "assistant", Content: data,
	})
	s.TurnCount++
}

// AppendToolResults adds tool result messages to the conversation.
// Each result corresponds to a tool_use block from the assistant.
func (s *ConversationState) AppendToolResults(results []ToolResult) {
	if len(results) == 0 {
		panic("AppendToolResults: results must not be empty")
	}
	data, err := json.Marshal(results)
	if err != nil {
		panic("AppendToolResults: marshal results: " + err.Error())
	}
	s.Messages = append(s.Messages, Message{
		Role: "user", Content: data,
	})
}

// AddArtifact records a reference to an artifact produced during
// this agent's execution (e.g., a file path or object store key).
func (s *ConversationState) AddArtifact(ref string) {
	if ref == "" {
		panic("AddArtifact: ref must not be empty")
	}
	s.Artifacts = append(s.Artifacts, ref)
}

// Marshal serializes the ConversationState to JSON.
func (s *ConversationState) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

// UnmarshalConversationState deserializes from JSON.
func UnmarshalConversationState(
	data []byte,
) (ConversationState, error) {
	var state ConversationState
	if err := json.Unmarshal(data, &state); err != nil {
		return ConversationState{},
			fmt.Errorf("unmarshal conversation state: %w", err)
	}
	return state, nil
}

// IsLargePayload returns true when the serialized state exceeds
// the threshold for inline NATS messages.
func IsLargePayload(data []byte) bool {
	return len(data) > PayloadSizeThreshold
}
