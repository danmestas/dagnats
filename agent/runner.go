// agent/runner.go

// AgentRunner executes the LLM tool-use loop: call LLM, parse tool-use
// blocks, dispatch tool calls, feed results back, repeat until the LLM
// produces a final text response (end_turn) or the turn limit is reached.
// Stateless between calls — all conversation state arrives via input and
// leaves via output.
package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/danmestas/dagnats/agent/llm"
	"github.com/danmestas/dagnats/observe"
)

// ToolExecutor executes a tool call and returns the result.
// Implementations live in agent/tools/.
type ToolExecutor interface {
	Execute(
		ctx context.Context,
		name string,
		input json.RawMessage,
	) (json.RawMessage, error)
}

// Result is the output of one agent loop iteration.
type Result struct {
	// Output is the serialized ConversationState for the next iteration.
	// Always set (even when Done=true, for history).
	Output []byte

	// Done is true when the LLM produced a final response (end_turn)
	// and the agent loop step should call Complete instead of Continue.
	Done bool

	// FinalOutput is the extracted final text output. Non-nil only
	// when Done=true. This becomes the step's output in the DAG.
	FinalOutput []byte

	// TotalUsage accumulates token usage across all LLM calls in
	// this iteration.
	TotalUsage llm.Usage
}

// Runner executes one iteration of the agent loop. An iteration may
// contain multiple LLM round-trips (call → tool_use → result → call)
// bounded by AgentConfig.MaxTurns.
type Runner struct {
	llm   llm.Client
	tools ToolExecutor
	tel   *observe.Telemetry
}

// NewRunner creates an agent Runner. Panics on nil arguments —
// these are programmer errors caught at construction time.
func NewRunner(
	llmClient llm.Client,
	tools ToolExecutor,
	tel *observe.Telemetry,
) *Runner {
	if llmClient == nil {
		panic("NewRunner: llm must not be nil")
	}
	if tools == nil {
		panic("NewRunner: tools must not be nil")
	}
	if tel == nil {
		panic("NewRunner: tel must not be nil")
	}
	return &Runner{llm: llmClient, tools: tools, tel: tel}
}

// Run executes one agent loop iteration. The input is a serialized
// ConversationState. The config determines model, tools, and bounds.
// Returns a Result indicating whether to Continue or Complete.
func (r *Runner) Run(
	ctx context.Context,
	config AgentConfig,
	input []byte,
	streamFunc func(token string),
) (*Result, error) {
	if len(input) == 0 {
		panic("Runner.Run: input must not be empty")
	}
	ctx, span := r.tel.Tracer.Start(ctx, "agent.run",
		observe.WithAttributes(
			observe.StringAttr("agent_config", config.Name),
			observe.StringAttr("model", config.Model),
		),
	)
	defer span.End()

	state, err := UnmarshalConversationState(input)
	if err != nil {
		return nil, fmt.Errorf("unmarshal state: %w", err)
	}

	toolDefs := r.buildToolDefs(ctx, config)
	totalUsage := llm.Usage{}
	maxTurns := config.EffectiveMaxTurns()

	for turn := 0; turn < maxTurns; turn++ {
		resp, err := r.callLLM(
			ctx, config, state, toolDefs, streamFunc,
		)
		if err != nil {
			return nil, fmt.Errorf("llm call (turn %d): %w", turn, err)
		}
		totalUsage.InputTokens += resp.Usage.InputTokens
		totalUsage.OutputTokens += resp.Usage.OutputTokens

		// Convert llm.ContentBlock to agent.ContentBlock for storage.
		agentBlocks := convertToAgentBlocks(resp.Content)
		state.AppendAssistant(agentBlocks)

		if resp.StopReason != "tool_use" {
			return r.buildFinalResult(state, totalUsage)
		}

		toolResults, err := r.executeToolCalls(ctx, resp.Content)
		if err != nil {
			return nil, fmt.Errorf(
				"tool execution (turn %d): %w", turn, err,
			)
		}
		state.AppendToolResults(toolResults)
	}

	// Turn limit reached — return what we have as a Continue signal
	// so the orchestrator can re-enqueue for the next iteration.
	return r.buildContinueResult(state, totalUsage)
}

// callLLM sends the current conversation state to the LLM provider.
func (r *Runner) callLLM(
	ctx context.Context,
	config AgentConfig,
	state ConversationState,
	toolDefs []llm.ToolDef,
	streamFunc func(token string),
) (*llm.Response, error) {
	_, span := r.tel.Tracer.Start(ctx, "agent.llm_call",
		observe.WithSpanKind(observe.SpanKindClient),
	)
	defer span.End()

	llmMessages := make([]llm.Message, len(state.Messages))
	for i, m := range state.Messages {
		llmMessages[i] = llm.Message{Role: m.Role, Content: m.Content}
	}
	req := &llm.Request{
		Model:        config.Model,
		SystemPrompt: config.SystemPrompt,
		Messages:     llmMessages,
		Tools:        toolDefs,
		MaxTokens:    config.EffectiveMaxTokens(),
		Temperature:  config.Temperature,
		StreamFunc:   streamFunc,
	}
	resp, err := r.llm.SendMessage(ctx, req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
		return nil, err
	}
	span.SetAttributes(
		observe.Int64Attr(
			"input_tokens", int64(resp.Usage.InputTokens),
		),
		observe.Int64Attr(
			"output_tokens", int64(resp.Usage.OutputTokens),
		),
		observe.StringAttr("stop_reason", resp.StopReason),
	)
	return resp, nil
}

// convertToAgentBlocks maps llm.ContentBlock to agent.ContentBlock.
func convertToAgentBlocks(blocks []llm.ContentBlock) []ContentBlock {
	result := make([]ContentBlock, len(blocks))
	for i, b := range blocks {
		result[i] = ContentBlock{
			Type: b.Type, Text: b.Text,
			ID: b.ID, Name: b.Name, Input: b.Input,
		}
	}
	return result
}

// executeToolCalls dispatches each tool_use block to the tool executor
// and collects results. Executes tools sequentially — parallel
// execution is a future optimization.
func (r *Runner) executeToolCalls(
	ctx context.Context,
	content []llm.ContentBlock,
) ([]ToolResult, error) {
	var results []ToolResult
	for _, block := range content {
		if block.Type != "tool_use" {
			continue
		}
		_, span := r.tel.Tracer.Start(ctx, "agent.tool_call",
			observe.WithAttributes(
				observe.StringAttr("tool_name", block.Name),
				observe.StringAttr("tool_use_id", block.ID),
			),
		)
		output, err := r.tools.Execute(ctx, block.Name, block.Input)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(observe.StatusError, err.Error())
			span.End()
			results = append(results, ToolResult{
				ToolUseID: block.ID,
				Content:   fmt.Sprintf("Error: %s", err.Error()),
				IsError:   true,
			})
			continue
		}
		span.End()
		results = append(results, ToolResult{
			ToolUseID: block.ID,
			Content:   string(output),
		})
	}
	if len(results) == 0 {
		panic("executeToolCalls: no tool_use blocks found in content")
	}
	return results, nil
}

// buildToolDefs converts the config's tool names into ToolDef schemas
// for the LLM. Tools not found in the executor are skipped with a log.
func (r *Runner) buildToolDefs(
	ctx context.Context,
	config AgentConfig,
) []llm.ToolDef {
	// The tool executor owns the schema lookup. This method delegates
	// to it. For now, we use a type assertion to check if the executor
	// supports listing tool definitions. If not, return empty.
	type toolLister interface {
		ListToolDefs(names []string) []llm.ToolDef
	}
	if lister, ok := r.tools.(toolLister); ok {
		return lister.ListToolDefs(config.Tools)
	}
	r.tel.Logger.Info("tool executor does not support ListToolDefs",
		observe.String("agent_config", config.Name),
	)
	return nil
}

// buildFinalResult creates a Result for a completed agent iteration.
func (r *Runner) buildFinalResult(
	state ConversationState,
	usage llm.Usage,
) (*Result, error) {
	stateData, err := state.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal final state: %w", err)
	}
	finalOutput := extractFinalText(state)
	return &Result{
		Output:      stateData,
		Done:        true,
		FinalOutput: finalOutput,
		TotalUsage:  usage,
	}, nil
}

// buildContinueResult creates a Result for a continuing agent loop.
func (r *Runner) buildContinueResult(
	state ConversationState,
	usage llm.Usage,
) (*Result, error) {
	stateData, err := state.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal continue state: %w", err)
	}
	return &Result{
		Output:     stateData,
		Done:       false,
		TotalUsage: usage,
	}, nil
}

// extractFinalText pulls the last assistant text block from the
// conversation as the step's output. Returns JSON-encoded string.
func extractFinalText(state ConversationState) []byte {
	messagesLen := len(state.Messages)
	if messagesLen == 0 {
		return []byte(`""`)
	}
	last := state.Messages[messagesLen-1]
	if last.Role != "assistant" {
		return []byte(`""`)
	}
	var blocks []ContentBlock
	if err := json.Unmarshal(last.Content, &blocks); err != nil {
		return last.Content
	}
	for i := len(blocks) - 1; i >= 0; i-- {
		if blocks[i].Type == "text" && blocks[i].Text != "" {
			data, err := json.Marshal(blocks[i].Text)
			if err != nil {
				return []byte(`""`)
			}
			return data
		}
	}
	return []byte(`""`)
}
