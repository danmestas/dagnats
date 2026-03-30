// agent/llm/openai.go

// OpenAI-compatible LLM client. Uses the Chat Completions API with
// function calling. Covers OpenAI, Groq, Together, local vLLM, and
// any API that implements the OpenAI chat completions format.
// No external SDK — raw HTTP for zero dependencies.
package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// openaiDefaultURL is the default base URL for the OpenAI API.
const openaiDefaultURL = "https://api.openai.com/v1/chat/completions"

// openaiHTTPTimeout is the maximum duration for a non-streaming request.
const openaiHTTPTimeout = 120 * time.Second

// OpenAIClient implements Client for OpenAI-compatible APIs.
type OpenAIClient struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

// OpenAIOption configures the OpenAI client.
type OpenAIOption func(*OpenAIClient)

// WithBaseURL sets a custom base URL (for Groq, Together, vLLM, etc).
func WithBaseURL(url string) OpenAIOption {
	return func(c *OpenAIClient) { c.baseURL = url }
}

// NewOpenAIClient creates an OpenAI-compatible API client.
func NewOpenAIClient(
	apiKey string, opts ...OpenAIOption,
) *OpenAIClient {
	if apiKey == "" {
		panic("NewOpenAIClient: apiKey must not be empty")
	}
	c := &OpenAIClient{
		apiKey:  apiKey,
		baseURL: openaiDefaultURL,
		httpClient: &http.Client{
			Timeout: openaiHTTPTimeout,
		},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// openaiRequest is the request body for Chat Completions.
type openaiRequest struct {
	Model       string           `json:"model"`
	Messages    []openaiMessage  `json:"messages"`
	Tools       []openaiTool     `json:"tools,omitempty"`
	MaxTokens   int              `json:"max_tokens,omitempty"`
	Temperature *float64         `json:"temperature,omitempty"`
	Stream      bool             `json:"stream,omitempty"`
}

type openaiMessage struct {
	Role       string           `json:"role"`
	Content    json.RawMessage  `json:"content,omitempty"`
	ToolCalls  []openaiToolCall `json:"tool_calls,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
}

type openaiTool struct {
	Type     string             `json:"type"`
	Function openaiToolFunction `json:"function"`
}

type openaiToolFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

type openaiToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// openaiResponse is the response body for non-streaming calls.
type openaiResponse struct {
	Choices []struct {
		Message struct {
			Role      string           `json:"role"`
			Content   *string          `json:"content"`
			ToolCalls []openaiToolCall `json:"tool_calls"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

// SendMessage calls the OpenAI-compatible Chat Completions API.
func (c *OpenAIClient) SendMessage(
	ctx context.Context,
	req *Request,
) (*Response, error) {
	if req == nil {
		panic("OpenAIClient.SendMessage: req must not be nil")
	}
	body := c.buildRequest(req)
	// Streaming deferred — non-streaming only for now.
	return c.sendNonStreaming(ctx, body, req.StreamFunc)
}

func (c *OpenAIClient) buildRequest(
	req *Request,
) openaiRequest {
	messages := c.convertMessages(req)
	tools := make([]openaiTool, len(req.Tools))
	for i, t := range req.Tools {
		tools[i] = openaiTool{
			Type: "function",
			Function: openaiToolFunction{
				Name:        t.Name,
				Description: t.Description,
				Parameters:  t.InputSchema,
			},
		}
	}
	or := openaiRequest{
		Model:     req.Model,
		Messages:  messages,
		MaxTokens: req.MaxTokens,
	}
	if len(tools) > 0 {
		or.Tools = tools
	}
	if req.Temperature > 0 {
		temp := req.Temperature
		or.Temperature = &temp
	}
	return or
}

// convertMessages transforms the generic Messages into OpenAI format.
// The key difference: OpenAI uses "tool" role with tool_call_id for
// tool results, and tool_calls on the assistant message.
func (c *OpenAIClient) convertMessages(
	req *Request,
) []openaiMessage {
	var messages []openaiMessage
	// Add system message.
	if req.SystemPrompt != "" {
		sysContent, _ := json.Marshal(req.SystemPrompt)
		messages = append(messages, openaiMessage{
			Role: "system", Content: sysContent,
		})
	}
	for _, m := range req.Messages {
		messages = append(messages, openaiMessage{
			Role: m.Role, Content: m.Content,
		})
	}
	return messages
}

func (c *OpenAIClient) sendNonStreaming(
	ctx context.Context,
	body openaiRequest,
	streamFunc func(string),
) (*Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(
		ctx, http.MethodPost, c.baseURL, bytes.NewReader(data),
	)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	c.setHeaders(httpReq)
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf(
			"openai API error (status %d): %s",
			resp.StatusCode, string(respBody),
		)
	}
	return c.parseResponse(resp.Body, streamFunc)
}

func (c *OpenAIClient) setHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
}

func (c *OpenAIClient) parseResponse(
	body io.Reader,
	streamFunc func(string),
) (*Response, error) {
	var or openaiResponse
	if err := json.NewDecoder(body).Decode(&or); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if len(or.Choices) == 0 {
		return nil, fmt.Errorf("openai: no choices in response")
	}
	choice := or.Choices[0]
	blocks := c.convertResponseBlocks(choice)
	stopReason := convertFinishReason(choice.FinishReason)

	// Call stream func with final text if provided.
	if streamFunc != nil {
		for _, b := range blocks {
			if b.Type == "text" && b.Text != "" {
				streamFunc(b.Text)
			}
		}
	}

	return &Response{
		Content:    blocks,
		StopReason: stopReason,
		Usage: Usage{
			InputTokens:  or.Usage.PromptTokens,
			OutputTokens: or.Usage.CompletionTokens,
		},
	}, nil
}

func (c *OpenAIClient) convertResponseBlocks(
	choice struct {
		Message struct {
			Role      string           `json:"role"`
			Content   *string          `json:"content"`
			ToolCalls []openaiToolCall `json:"tool_calls"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	},
) []ContentBlock {
	var blocks []ContentBlock
	if choice.Message.Content != nil && *choice.Message.Content != "" {
		blocks = append(blocks, ContentBlock{
			Type: "text", Text: *choice.Message.Content,
		})
	}
	for _, tc := range choice.Message.ToolCalls {
		blocks = append(blocks, ContentBlock{
			Type:  "tool_use",
			ID:    tc.ID,
			Name:  tc.Function.Name,
			Input: json.RawMessage(tc.Function.Arguments),
		})
	}
	if len(blocks) == 0 {
		blocks = append(blocks, ContentBlock{
			Type: "text", Text: "",
		})
	}
	return blocks
}

// convertFinishReason maps OpenAI finish reasons to our format.
func convertFinishReason(reason string) string {
	switch reason {
	case "stop":
		return "end_turn"
	case "tool_calls":
		return "tool_use"
	case "length":
		return "max_tokens"
	default:
		return reason
	}
}

// OpenAIFactory is a ProviderFactory for the OpenAI client.
func OpenAIFactory(apiKey string) Client {
	return NewOpenAIClient(apiKey)
}

// OpenAICompatibleFactory returns a ProviderFactory for a custom
// OpenAI-compatible endpoint (Groq, Together, vLLM, etc).
func OpenAICompatibleFactory(baseURL string) ProviderFactory {
	return func(apiKey string) Client {
		return NewOpenAIClient(apiKey, WithBaseURL(baseURL))
	}
}
