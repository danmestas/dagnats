// agent/handler.go

// AgentHandler bridges the DagNats worker framework to the agent
// runtime. It deserializes AgentConfig + ConversationState from the
// task input, runs one agent loop iteration, and calls Continue or
// Complete on the worker TaskContext.
//
// This file depends on the worker package. When compiled as part of
// the full DagNats binary, it imports worker types. The core agent
// logic (Runner, ConversationState) has no worker dependency.
package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/danmestas/dagnats/agent/llm"
	"github.com/danmestas/dagnats/agent/tools"
	"github.com/danmestas/dagnats/observe"
)

// AgentTaskInput is the JSON payload sent to the "agent-run" task.
// On the first iteration, it contains the initial prompt and config
// name. On subsequent iterations, it contains the serialized
// ConversationState from the previous Continue() call.
type AgentTaskInput struct {
	// AgentConfigName selects the AgentConfig from the KV store.
	AgentConfigName string `json:"agent_config_name"`

	// Prompt is the initial user prompt (first iteration only).
	Prompt string `json:"prompt,omitempty"`

	// State is the serialized ConversationState (subsequent iterations).
	State json.RawMessage `json:"state,omitempty"`
}

// AgentTaskOutput is the JSON payload returned by the agent step
// when it completes. Contains the final text output and artifact
// references for downstream steps.
type AgentTaskOutput struct {
	Text      string   `json:"text"`
	Artifacts []string `json:"artifacts,omitempty"`
}

// ConfigLoader loads an AgentConfig by name. In production, this
// reads from the agent_configs KV bucket. In tests, a simple map.
type ConfigLoader interface {
	LoadConfig(
		ctx context.Context, name string,
	) (AgentConfig, error)
}

// WorkerContext is the subset of worker.TaskContext that the agent
// handler needs. Defined as an interface to avoid importing worker/
// in unit tests while maintaining compile-time safety.
type WorkerContext interface {
	Input() []byte
	RunID() string
	StepID() string
	RetryCount() int
	Complete(output []byte) error
	Fail(err error) error
	Continue(output []byte) error
	PutStream(data []byte) error
}

// Handler processes "agent-run" tasks. It holds the LLM client,
// tool registry, config loader, and telemetry — all injected at
// worker startup time.
type Handler struct {
	llmRegistry *llm.ProviderRegistry
	tools       *tools.Registry
	configs     ConfigLoader
	tel         *observe.Telemetry
}

// NewHandler creates an agent task handler. All arguments required.
func NewHandler(
	llmReg *llm.ProviderRegistry,
	toolReg *tools.Registry,
	configs ConfigLoader,
	tel *observe.Telemetry,
) *Handler {
	if llmReg == nil {
		panic("NewHandler: llmRegistry must not be nil")
	}
	if toolReg == nil {
		panic("NewHandler: tools must not be nil")
	}
	if configs == nil {
		panic("NewHandler: configs must not be nil")
	}
	if tel == nil {
		panic("NewHandler: tel must not be nil")
	}
	return &Handler{
		llmRegistry: llmReg,
		tools:       toolReg,
		configs:     configs,
		tel:         tel,
	}
}

// Handle processes a single agent-run task. Designed to be registered
// as: worker.Handle("agent-run", handler.Handle)
func (h *Handler) Handle(ctx WorkerContext) error {
	if ctx == nil {
		panic("Handler.Handle: ctx must not be nil")
	}

	bgCtx := context.Background()
	var taskInput AgentTaskInput
	if err := json.Unmarshal(ctx.Input(), &taskInput); err != nil {
		return fmt.Errorf("unmarshal task input: %w", err)
	}

	config, err := h.configs.LoadConfig(
		bgCtx, taskInput.AgentConfigName,
	)
	if err != nil {
		return fmt.Errorf("load agent config: %w", err)
	}

	// Get the LLM client for this agent's provider.
	// API key lookup would come from env or secrets KV in production.
	llmClient, err := h.llmRegistry.Get(config.Provider, "")
	if err != nil {
		return fmt.Errorf("get LLM client: %w", err)
	}

	// Build the conversation state for this iteration.
	var stateBytes []byte
	if taskInput.State != nil {
		stateBytes = taskInput.State
	} else {
		state := NewConversationState(
			taskInput.AgentConfigName, taskInput.Prompt,
		)
		stateBytes, err = state.Marshal()
		if err != nil {
			return fmt.Errorf("marshal initial state: %w", err)
		}
	}

	// Wire PutStream for token streaming.
	streamFunc := func(token string) {
		ctx.PutStream([]byte(token))
	}

	runner := NewRunner(llmClient, h.tools, h.tel)
	result, err := runner.Run(bgCtx, config, stateBytes, streamFunc)
	if err != nil {
		return fmt.Errorf("agent run: %w", err)
	}

	if result.Done {
		return ctx.Complete(result.FinalOutput)
	}
	return ctx.Continue(result.Output)
}

// MapConfigLoader is a simple in-memory ConfigLoader for tests.
type MapConfigLoader struct {
	Configs map[string]AgentConfig
}

// LoadConfig returns the config by name or an error if not found.
func (m *MapConfigLoader) LoadConfig(
	ctx context.Context, name string,
) (AgentConfig, error) {
	cfg, ok := m.Configs[name]
	if !ok {
		return AgentConfig{},
			fmt.Errorf("agent config %q not found", name)
	}
	return cfg, nil
}
