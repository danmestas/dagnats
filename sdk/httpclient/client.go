// Package httpclient implements the DagNats worker protocol over HTTP.
// This is the reference client for validating the bridge wire protocol
// and serves as a template for other language SDKs.
package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/danmestas/dagnats/protocol"
)

const (
	maxResponseBytes  = 10 << 20 // 10 MiB safety cap on response bodies
	defaultTimeoutMs  = 30_000
	connectTimeoutSec = 5
)

// Client implements the DagNats worker protocol over HTTP.
// This is the reference implementation for other language SDKs.
type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
	workerID   string
	cancelConn context.CancelFunc
}

// Option configures a Client.
type Option func(*Client)

// WithToken sets the bearer token for bridge authentication.
func WithToken(token string) Option {
	return func(c *Client) { c.token = token }
}

// New creates an HTTP client targeting the given base URL.
// Panics if baseURL is empty.
func New(baseURL string, opts ...Option) *Client {
	if baseURL == "" {
		panic("httpclient.New: baseURL must not be empty")
	}
	c := &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: time.Duration(defaultTimeoutMs) * time.Millisecond,
		},
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.httpClient == nil {
		panic("httpclient.New: httpClient is nil after options")
	}
	return c
}

// connectRequest mirrors bridge.connectRequest for the wire format.
type connectRequest struct {
	WorkerID  string   `json:"worker_id"`
	TaskTypes []string `json:"task_types"`
	MaxTasks  int      `json:"max_tasks"`
}

// Connect registers a worker with the bridge and starts a background
// SSE heartbeat reader. The SSE connection stays open until Disconnect
// is called or ctx is cancelled.
func (c *Client) Connect(
	ctx context.Context, workerID string,
	taskTypes []string, maxTasks int,
) error {
	if c == nil {
		panic("Connect: c is nil")
	}
	if workerID == "" {
		panic("Connect: workerID must not be empty")
	}

	c.workerID = workerID
	req := connectRequest{
		WorkerID:  workerID,
		TaskTypes: taskTypes,
		MaxTasks:  maxTasks,
	}
	sseCtx, cancel := context.WithCancel(ctx)
	c.cancelConn = cancel

	resp, err := c.doPost(sseCtx, "/v1/workers/connect", req)
	if err != nil {
		cancel()
		return fmt.Errorf("connect: %w", err)
	}

	// Start background SSE reader to keep connection alive.
	// The response body stays open until context cancellation.
	go drainSSE(sseCtx, resp.Body)
	return nil
}

// drainSSE reads and discards SSE heartbeats until ctx is done.
func drainSSE(ctx context.Context, body io.ReadCloser) {
	if body == nil {
		panic("drainSSE: body must not be nil")
	}
	defer body.Close()
	buf := make([]byte, 512)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		_, err := body.Read(buf)
		if err != nil {
			return
		}
	}
}

// Disconnect cancels the SSE connection.
func (c *Client) Disconnect() {
	if c == nil {
		panic("Disconnect: c is nil")
	}
	if c.cancelConn != nil {
		c.cancelConn()
		c.cancelConn = nil
	}
}

// pollRequest mirrors bridge.pollRequest for the wire format.
type pollRequest struct {
	TaskTypes []string `json:"task_types"`
	MaxTasks  int      `json:"max_tasks"`
	TimeoutMs int64    `json:"timeout_ms"`
}

// Poll long-polls for available tasks. Returns an empty slice on timeout.
func (c *Client) Poll(
	ctx context.Context,
	taskTypes []string,
	maxTasks int,
	timeout time.Duration,
) ([]protocol.TaskPayload, error) {
	if c == nil {
		panic("Poll: c is nil")
	}
	if len(taskTypes) == 0 {
		panic("Poll: taskTypes must not be empty")
	}

	req := pollRequest{
		TaskTypes: taskTypes,
		MaxTasks:  maxTasks,
		TimeoutMs: timeout.Milliseconds(),
	}

	// Use a longer HTTP timeout than the poll timeout to avoid
	// the HTTP client timing out before the server responds.
	pollCtx, cancel := context.WithTimeout(
		ctx, timeout+5*time.Second,
	)
	defer cancel()

	resp, err := c.doPost(pollCtx, "/v1/tasks/poll", req)
	if err != nil {
		return nil, fmt.Errorf("poll: %w", err)
	}
	defer resp.Body.Close()

	return decodePollResponse(resp)
}

// decodePollResponse reads and unmarshals the poll response body.
func decodePollResponse(
	resp *http.Response,
) ([]protocol.TaskPayload, error) {
	if resp == nil {
		panic("decodePollResponse: resp must not be nil")
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(
			io.LimitReader(resp.Body, 1024),
		)
		return nil, fmt.Errorf(
			"poll: status %d: %s",
			resp.StatusCode, string(body),
		)
	}
	limited := io.LimitReader(resp.Body, maxResponseBytes)
	var tasks []protocol.TaskPayload
	if err := json.NewDecoder(limited).Decode(&tasks); err != nil {
		return nil, fmt.Errorf("poll: decode: %w", err)
	}
	return tasks, nil
}

// Complete resolves a task as successfully completed.
func (c *Client) Complete(
	ctx context.Context,
	taskID string,
	output json.RawMessage,
) error {
	if c == nil {
		panic("Complete: c is nil")
	}
	if taskID == "" {
		panic("Complete: taskID must not be empty")
	}
	res := protocol.TaskResolution{
		Action: "complete",
		Output: output,
	}
	return c.resolveExpectOK(ctx, taskID, res)
}

// Fail resolves a task as failed with an error message.
func (c *Client) Fail(
	ctx context.Context,
	taskID string,
	errMsg string,
) error {
	if c == nil {
		panic("Fail: c is nil")
	}
	if taskID == "" {
		panic("Fail: taskID must not be empty")
	}
	res := protocol.TaskResolution{
		Action: "fail",
		Error:  errMsg,
	}
	return c.resolveExpectOK(ctx, taskID, res)
}

// Pause pauses a task with optional checkpoint, to be resumed after
// the given duration.
func (c *Client) Pause(
	ctx context.Context,
	taskID string,
	name string,
	duration time.Duration,
	checkpoint json.RawMessage,
) error {
	if c == nil {
		panic("Pause: c is nil")
	}
	if taskID == "" {
		panic("Pause: taskID must not be empty")
	}
	res := protocol.TaskResolution{
		Action:     "pause",
		Name:       name,
		DurationMs: duration.Milliseconds(),
		Checkpoint: checkpoint,
	}
	return c.resolveExpectOK(ctx, taskID, res)
}

// Checkpoint saves intermediate state for a task.
func (c *Client) Checkpoint(
	ctx context.Context,
	taskID string,
	data json.RawMessage,
) error {
	if c == nil {
		panic("Checkpoint: c is nil")
	}
	if taskID == "" {
		panic("Checkpoint: taskID must not be empty")
	}
	res := protocol.TaskResolution{
		Action: "checkpoint",
		Data:   data,
	}
	return c.resolveExpectOK(ctx, taskID, res)
}

// resolveExpectOK posts a resolve request and checks for 200 OK.
func (c *Client) resolveExpectOK(
	ctx context.Context,
	taskID string,
	res protocol.TaskResolution,
) error {
	if taskID == "" {
		panic("resolveExpectOK: taskID must not be empty")
	}
	if res.Action == "" {
		panic("resolveExpectOK: Action must not be empty")
	}
	resp, err := c.resolve(ctx, taskID, res)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(
			io.LimitReader(resp.Body, 1024),
		)
		return fmt.Errorf(
			"resolve %s: status %d: %s",
			res.Action, resp.StatusCode, string(body),
		)
	}
	return nil
}

// resolve POSTs a TaskResolution to the bridge resolve endpoint.
func (c *Client) resolve(
	ctx context.Context,
	taskID string,
	res protocol.TaskResolution,
) (*http.Response, error) {
	if taskID == "" {
		panic("resolve: taskID must not be empty")
	}
	if res.Action == "" {
		panic("resolve: Action must not be empty")
	}
	path := "/v1/tasks/" + taskID + "/resolve"
	return c.doPost(ctx, path, res)
}

// doPost marshals body to JSON and POSTs to baseURL+path.
func (c *Client) doPost(
	ctx context.Context,
	path string,
	body any,
) (*http.Response, error) {
	if c == nil {
		panic("doPost: c is nil")
	}
	if path == "" {
		panic("doPost: path must not be empty")
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	url := c.baseURL + path
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, url, bytes.NewReader(data),
	)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set(
			"Authorization", "Bearer "+c.token,
		)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	return resp, nil
}
