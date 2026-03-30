// agent/llm/anthropic.go

// Anthropic Claude API client. Uses the Messages API with streaming
// support. Tool use responses map directly to ContentBlock with
// Type="tool_use". No external SDK — raw HTTP for zero dependencies.
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

// anthropicAPIURL is the base URL for the Anthropic Messages API.
const anthropicAPIURL = "https://api.anthropic.com/v1/messages"

// anthropicAPIVersion is the API version header value.
const anthropicAPIVersion = "2023-06-01"

// anthropicHTTPTimeout is the maximum duration for a non-streaming
// request. Streaming requests use the context deadline instead.
const anthropicHTTPTimeout = 120 * time.Second

// AnthropicClient implements Client for the Anthropic/Claude API.
type AnthropicClient struct {
	apiKey     string
	httpClient *http.Client
	baseURL    string
}

// NewAnthropicClient creates a Claude API client. The apiKey is sent
// as the x-api-key header on every request.
func NewAnthropicClient(apiKey string) *AnthropicClient {
	if apiKey == "" {
		panic("NewAnthropicClient: apiKey must not be empty")
	}
	return &AnthropicClient{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: anthropicHTTPTimeout,
		},
		baseURL: anthropicAPIURL,
	}
}

// anthropicRequest is the request body for the Messages API.
type anthropicRequest struct {
	Model       string              `json:"model"`
	MaxTokens   int                 `json:"max_tokens"`
	System      string              `json:"system,omitempty"`
	Messages    []anthropicMessage  `json:"messages"`
	Tools       []anthropicTool     `json:"tools,omitempty"`
	Temperature *float64            `json:"temperature,omitempty"`
	Stream      bool                `json:"stream,omitempty"`
}

type anthropicMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type anthropicTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

// anthropicResponse is the response body for non-streaming calls.
type anthropicResponse struct {
	ID         string         `json:"id"`
	Content    []ContentBlock `json:"content"`
	StopReason string         `json:"stop_reason"`
	Usage      struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

// SendMessage calls the Anthropic Messages API.
func (c *AnthropicClient) SendMessage(
	ctx context.Context,
	req *Request,
) (*Response, error) {
	if req == nil {
		panic("AnthropicClient.SendMessage: req must not be nil")
	}
	body := c.buildRequest(req)
	if req.StreamFunc != nil {
		return c.sendStreaming(ctx, body, req.StreamFunc)
	}
	return c.sendNonStreaming(ctx, body)
}

func (c *AnthropicClient) buildRequest(
	req *Request,
) anthropicRequest {
	messages := make([]anthropicMessage, len(req.Messages))
	for i, m := range req.Messages {
		messages[i] = anthropicMessage{
			Role: m.Role, Content: m.Content,
		}
	}
	tools := make([]anthropicTool, len(req.Tools))
	for i, t := range req.Tools {
		tools[i] = anthropicTool{
			Name:        t.Name,
			Description: t.Description,
			InputSchema: t.InputSchema,
		}
	}
	ar := anthropicRequest{
		Model:     req.Model,
		MaxTokens: req.MaxTokens,
		System:    req.SystemPrompt,
		Messages:  messages,
	}
	if len(tools) > 0 {
		ar.Tools = tools
	}
	if req.Temperature > 0 {
		temp := req.Temperature
		ar.Temperature = &temp
	}
	return ar
}

func (c *AnthropicClient) sendNonStreaming(
	ctx context.Context,
	body anthropicRequest,
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
	return c.parseResponse(resp)
}

// sendStreaming uses SSE to stream the response. Each content_block_delta
// event with type "text_delta" triggers the StreamFunc callback.
func (c *AnthropicClient) sendStreaming(
	ctx context.Context,
	body anthropicRequest,
	streamFunc func(string),
) (*Response, error) {
	body.Stream = true
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
	// Override timeout — streaming requests use context deadline.
	client := &http.Client{}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return c.parseErrorResponse(resp)
	}
	return c.processSSEStream(resp.Body, streamFunc)
}

func (c *AnthropicClient) setHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", c.apiKey)
	req.Header.Set("anthropic-version", anthropicAPIVersion)
}

func (c *AnthropicClient) parseResponse(
	resp *http.Response,
) (*Response, error) {
	if resp.StatusCode != http.StatusOK {
		return c.parseErrorResponse(resp)
	}
	var ar anthropicResponse
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &Response{
		Content:    ar.Content,
		StopReason: ar.StopReason,
		Usage: Usage{
			InputTokens:  ar.Usage.InputTokens,
			OutputTokens: ar.Usage.OutputTokens,
		},
	}, nil
}

func (c *AnthropicClient) parseErrorResponse(
	resp *http.Response,
) (*Response, error) {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return nil, fmt.Errorf(
		"anthropic API error (status %d): %s",
		resp.StatusCode, string(body),
	)
}

// sseEvent represents a single Server-Sent Event.
type sseEvent struct {
	Event string
	Data  string
}

// processSSEStream parses the SSE stream and accumulates the response.
func (c *AnthropicClient) processSSEStream(
	body io.Reader,
	streamFunc func(string),
) (*Response, error) {
	var result Response
	var currentBlocks []ContentBlock
	decoder := newSSEDecoder(body)

	const maxSSEEvents = 100000 // Safety bound
	for i := 0; i < maxSSEEvents; i++ {
		evt, err := decoder.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read SSE event: %w", err)
		}
		switch evt.Event {
		case "content_block_start":
			var block struct {
				ContentBlock ContentBlock `json:"content_block"`
			}
			if err := json.Unmarshal(
				[]byte(evt.Data), &block,
			); err == nil {
				currentBlocks = append(
					currentBlocks, block.ContentBlock,
				)
			}
		case "content_block_delta":
			c.handleBlockDelta(
				evt.Data, currentBlocks, streamFunc,
			)
		case "message_delta":
			var delta struct {
				Delta struct {
					StopReason string `json:"stop_reason"`
				} `json:"delta"`
				Usage struct {
					OutputTokens int `json:"output_tokens"`
				} `json:"usage"`
			}
			if err := json.Unmarshal(
				[]byte(evt.Data), &delta,
			); err == nil {
				result.StopReason = delta.Delta.StopReason
				result.Usage.OutputTokens = delta.Usage.OutputTokens
			}
		case "message_start":
			var msg struct {
				Message struct {
					Usage struct {
						InputTokens int `json:"input_tokens"`
					} `json:"usage"`
				} `json:"message"`
			}
			if err := json.Unmarshal(
				[]byte(evt.Data), &msg,
			); err == nil {
				result.Usage.InputTokens = msg.Message.Usage.InputTokens
			}
		case "message_stop":
			break
		}
	}
	result.Content = currentBlocks
	return &result, nil
}

// handleBlockDelta processes a content_block_delta SSE event.
func (c *AnthropicClient) handleBlockDelta(
	data string,
	blocks []ContentBlock,
	streamFunc func(string),
) {
	var delta struct {
		Index int `json:"index"`
		Delta struct {
			Type        string          `json:"type"`
			Text        string          `json:"text"`
			PartialJSON json.RawMessage `json:"partial_json"`
		} `json:"delta"`
	}
	if err := json.Unmarshal([]byte(data), &delta); err != nil {
		return
	}
	if delta.Index >= len(blocks) {
		return
	}
	if delta.Delta.Type == "text_delta" {
		blocks[delta.Index].Text += delta.Delta.Text
		if streamFunc != nil {
			streamFunc(delta.Delta.Text)
		}
	}
	if delta.Delta.Type == "input_json_delta" {
		existing := blocks[delta.Index].Input
		if existing == nil {
			blocks[delta.Index].Input = delta.Delta.PartialJSON
		} else {
			blocks[delta.Index].Input = append(
				existing, delta.Delta.PartialJSON...,
			)
		}
	}
}

// sseDecoder reads Server-Sent Events from an io.Reader.
type sseDecoder struct {
	reader *bytes.Reader
	buf    []byte
}

// newSSEDecoder creates an SSE decoder. Reads entire body into memory
// (bounded by HTTP response limits) for simple line-based parsing.
func newSSEDecoder(r io.Reader) *sseDecoder {
	data, _ := io.ReadAll(io.LimitReader(r, 50*1024*1024)) // 50MB cap
	return &sseDecoder{
		reader: bytes.NewReader(data),
		buf:    data,
	}
}

// Next returns the next SSE event. Returns io.EOF when done.
func (d *sseDecoder) Next() (*sseEvent, error) {
	var event sseEvent
	foundEvent := false
	const maxLineLength = 1024 * 1024 // 1MB per line

	for {
		line, err := d.readLine(maxLineLength)
		if err != nil && !foundEvent {
			return nil, io.EOF
		}
		if len(line) == 0 {
			if foundEvent {
				return &event, nil
			}
			if err != nil {
				return nil, io.EOF
			}
			continue
		}
		if bytes.HasPrefix(line, []byte("event: ")) {
			event.Event = string(line[7:])
			foundEvent = true
		}
		if bytes.HasPrefix(line, []byte("data: ")) {
			event.Data = string(line[6:])
		}
	}
}

func (d *sseDecoder) readLine(maxLen int) ([]byte, error) {
	var line []byte
	for i := 0; i < maxLen; i++ {
		b, err := d.reader.ReadByte()
		if err != nil {
			return line, err
		}
		if b == '\n' {
			return bytes.TrimRight(line, "\r"), nil
		}
		line = append(line, b)
	}
	return line, nil
}

// AnthropicFactory is a ProviderFactory for the Anthropic client.
func AnthropicFactory(apiKey string) Client {
	return NewAnthropicClient(apiKey)
}
