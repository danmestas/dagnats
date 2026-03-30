// agent/llm/client_test.go
// Tests for the LLM client interface, provider registry, and HTTP
// clients using httptest servers. No real API calls.
// Methodology: Each test verifies positive behavior and negative space.

package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProviderRegistry_RegisterAndGet(t *testing.T) {
	reg := NewProviderRegistry()
	called := false
	reg.Register("test", func(apiKey string) Client {
		called = true
		return &mockClient{}
	})
	client, err := reg.Get("test", "key123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if !called {
		t.Fatal("expected factory to be called")
	}
}

func TestProviderRegistry_GetUnknown(t *testing.T) {
	reg := NewProviderRegistry()
	_, err := reg.Get("unknown", "key")
	if err == nil {
		t.Fatal("expected error for unknown provider")
	}
}

func TestProviderRegistry_Names(t *testing.T) {
	reg := NewProviderRegistry()
	reg.Register("anthropic", func(k string) Client {
		return &mockClient{}
	})
	reg.Register("openai", func(k string) Client {
		return &mockClient{}
	})
	names := reg.Names()
	if len(names) != 2 {
		t.Fatalf("expected 2 names, got %d", len(names))
	}
}

func TestProviderRegistry_PanicsOnEmpty(t *testing.T) {
	reg := NewProviderRegistry()
	t.Run("empty name", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic")
			}
		}()
		reg.Register("", func(k string) Client {
			return &mockClient{}
		})
	})
	t.Run("nil factory", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic")
			}
		}()
		reg.Register("test", nil)
	})
}

// mockClient is a test double for Client.
type mockClient struct {
	response *Response
	err      error
}

func (m *mockClient) SendMessage(
	ctx context.Context, req *Request,
) (*Response, error) {
	return m.response, m.err
}

func TestAnthropicClient_NonStreaming(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			// Verify headers.
			if r.Header.Get("x-api-key") != "test-key" {
				t.Error("missing api key header")
			}
			if r.Header.Get("anthropic-version") == "" {
				t.Error("missing anthropic-version header")
			}
			resp := anthropicResponse{
				ID: "msg_123",
				Content: []ContentBlock{
					{Type: "text", Text: "Hello!"},
				},
				StopReason: "end_turn",
			}
			resp.Usage.InputTokens = 10
			resp.Usage.OutputTokens = 5
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		},
	))
	defer server.Close()

	client := NewAnthropicClient("test-key")
	client.baseURL = server.URL

	prompt, _ := json.Marshal("Hello")
	resp, err := client.SendMessage(
		context.Background(),
		&Request{
			Model:        "claude-sonnet-4-6",
			SystemPrompt: "Be helpful.",
			Messages: []Message{
				{Role: "user", Content: prompt},
			},
			MaxTokens: 100,
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StopReason != "end_turn" {
		t.Fatalf("expected end_turn, got %q", resp.StopReason)
	}
	if len(resp.Content) != 1 {
		t.Fatalf("expected 1 content block, got %d",
			len(resp.Content))
	}
	if resp.Content[0].Text != "Hello!" {
		t.Fatalf("expected 'Hello!', got %q",
			resp.Content[0].Text)
	}
	if resp.Usage.InputTokens != 10 {
		t.Fatalf("expected 10 input tokens, got %d",
			resp.Usage.InputTokens)
	}
}

func TestAnthropicClient_ToolUse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			resp := anthropicResponse{
				ID: "msg_456",
				Content: []ContentBlock{
					{
						Type:  "tool_use",
						ID:    "call_1",
						Name:  "read_file",
						Input: json.RawMessage(`{"path":"main.go"}`),
					},
				},
				StopReason: "tool_use",
			}
			json.NewEncoder(w).Encode(resp)
		},
	))
	defer server.Close()

	client := NewAnthropicClient("test-key")
	client.baseURL = server.URL

	prompt, _ := json.Marshal("Read main.go")
	resp, err := client.SendMessage(
		context.Background(),
		&Request{
			Model:     "claude-sonnet-4-6",
			Messages:  []Message{{Role: "user", Content: prompt}},
			MaxTokens: 100,
			Tools: []ToolDef{{
				Name:        "read_file",
				Description: "Read a file",
				InputSchema: json.RawMessage(
					`{"type":"object","properties":{"path":{"type":"string"}}}`,
				),
			}},
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StopReason != "tool_use" {
		t.Fatalf("expected tool_use, got %q", resp.StopReason)
	}
	if resp.Content[0].Name != "read_file" {
		t.Fatalf("expected tool name 'read_file', got %q",
			resp.Content[0].Name)
	}
}

func TestAnthropicClient_APIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error":"rate limited"}`))
		},
	))
	defer server.Close()

	client := NewAnthropicClient("test-key")
	client.baseURL = server.URL

	prompt, _ := json.Marshal("Hello")
	_, err := client.SendMessage(
		context.Background(),
		&Request{
			Model:     "claude-sonnet-4-6",
			Messages:  []Message{{Role: "user", Content: prompt}},
			MaxTokens: 100,
		},
	)
	if err == nil {
		t.Fatal("expected error for 429 response")
	}
}

func TestOpenAIClient_NonStreaming(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer test-key" {
				t.Error("missing auth header")
			}
			content := "Hello from OpenAI!"
			resp := map[string]interface{}{
				"choices": []map[string]interface{}{{
					"message": map[string]interface{}{
						"role":    "assistant",
						"content": content,
					},
					"finish_reason": "stop",
				}},
				"usage": map[string]int{
					"prompt_tokens":     8,
					"completion_tokens": 4,
				},
			}
			json.NewEncoder(w).Encode(resp)
		},
	))
	defer server.Close()

	client := NewOpenAIClient("test-key",
		WithBaseURL(server.URL))

	prompt, _ := json.Marshal("Hello")
	resp, err := client.SendMessage(
		context.Background(),
		&Request{
			Model:        "gpt-4",
			SystemPrompt: "Be helpful.",
			Messages: []Message{
				{Role: "user", Content: prompt},
			},
			MaxTokens: 100,
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StopReason != "end_turn" {
		t.Fatalf("expected end_turn, got %q", resp.StopReason)
	}
	if resp.Content[0].Text != "Hello from OpenAI!" {
		t.Fatalf("expected text, got %q", resp.Content[0].Text)
	}
	if resp.Usage.InputTokens != 8 {
		t.Fatalf("expected 8, got %d", resp.Usage.InputTokens)
	}
}

func TestOpenAIClient_ToolCalls(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			resp := map[string]interface{}{
				"choices": []map[string]interface{}{{
					"message": map[string]interface{}{
						"role": "assistant",
						"tool_calls": []map[string]interface{}{{
							"id":   "call_1",
							"type": "function",
							"function": map[string]string{
								"name":      "read_file",
								"arguments": `{"path":"main.go"}`,
							},
						}},
					},
					"finish_reason": "tool_calls",
				}},
				"usage": map[string]int{
					"prompt_tokens":     10,
					"completion_tokens": 15,
				},
			}
			json.NewEncoder(w).Encode(resp)
		},
	))
	defer server.Close()

	client := NewOpenAIClient("test-key",
		WithBaseURL(server.URL))

	prompt, _ := json.Marshal("Read main.go")
	resp, err := client.SendMessage(
		context.Background(),
		&Request{
			Model:     "gpt-4",
			Messages:  []Message{{Role: "user", Content: prompt}},
			MaxTokens: 100,
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StopReason != "tool_use" {
		t.Fatalf("expected tool_use, got %q", resp.StopReason)
	}
	if len(resp.Content) != 1 {
		t.Fatalf("expected 1 block, got %d", len(resp.Content))
	}
	if resp.Content[0].Name != "read_file" {
		t.Fatalf("expected read_file, got %q",
			resp.Content[0].Name)
	}
}

func TestConvertFinishReason(t *testing.T) {
	cases := map[string]string{
		"stop":       "end_turn",
		"tool_calls": "tool_use",
		"length":     "max_tokens",
		"other":      "other",
	}
	for input, expected := range cases {
		got := convertFinishReason(input)
		if got != expected {
			t.Errorf("convertFinishReason(%q) = %q, want %q",
				input, got, expected)
		}
	}
}
