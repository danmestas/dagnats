// agent/llm/client.go

// Package llm defines the provider-agnostic interface for calling LLM APIs.
// Concrete implementations for Anthropic (Claude) and OpenAI-compatible APIs
// live alongside this file. The ProviderRegistry maps provider names from
// AgentConfig to Client constructors.
package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// Client is the interface for calling LLM APIs. Implementations must
// support streaming via StreamFunc if provided in the Request.
type Client interface {
	// SendMessage sends a conversation to the LLM and returns the response.
	// If req.StreamFunc is non-nil, tokens are streamed as they arrive.
	SendMessage(ctx context.Context, req *Request) (*Response, error)
}

// Request is sent to an LLM provider. StreamFunc is optional — when
// set, the implementation calls it with each text token for real-time
// output (wired to PutStream in the worker).
type Request struct {
	Model        string    `json:"model"`
	SystemPrompt string    `json:"system_prompt"`
	Messages     []Message `json:"messages"`
	Tools        []ToolDef `json:"tools,omitempty"`
	MaxTokens    int       `json:"max_tokens"`
	Temperature  float64   `json:"temperature"`
	StreamFunc   func(token string)
}

// Message is a single entry in the conversation.
type Message struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

// ToolDef is the schema definition sent to the LLM so it knows which
// tools are available. InputSchema is a JSON Schema object.
type ToolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

// Response is returned from an LLM provider.
type Response struct {
	Content    []ContentBlock `json:"content"`
	StopReason string         `json:"stop_reason"`
	Usage      Usage          `json:"usage"`
}

// ContentBlock represents a typed block within an assistant response.
type ContentBlock struct {
	Type  string          `json:"type"`
	Text  string          `json:"text,omitempty"`
	ID    string          `json:"id,omitempty"`
	Name  string          `json:"name,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`
}

// Usage tracks token consumption for a single LLM call.
type Usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

// ProviderFactory creates a Client for a given API key.
type ProviderFactory func(apiKey string) Client

// ProviderRegistry maps provider names (e.g. "anthropic", "openai")
// to Client constructors. Thread-safe via sync.RWMutex.
type ProviderRegistry struct {
	mu        sync.RWMutex
	providers map[string]ProviderFactory
}

// NewProviderRegistry creates an empty registry.
func NewProviderRegistry() *ProviderRegistry {
	return &ProviderRegistry{
		providers: make(map[string]ProviderFactory),
	}
}

// Register adds a provider factory under the given name. Panics
// if the name is empty or the factory is nil.
func (r *ProviderRegistry) Register(
	name string, factory ProviderFactory,
) {
	if name == "" {
		panic("ProviderRegistry.Register: name must not be empty")
	}
	if factory == nil {
		panic("ProviderRegistry.Register: factory must not be nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers[name] = factory
}

// Get returns a Client for the named provider with the given API key.
// Returns an error if the provider is not registered.
func (r *ProviderRegistry) Get(
	name string, apiKey string,
) (Client, error) {
	r.mu.RLock()
	factory, ok := r.providers[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown LLM provider: %q", name)
	}
	return factory(apiKey), nil
}

// Names returns the list of registered provider names.
func (r *ProviderRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	return names
}
