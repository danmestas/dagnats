// agent/tools/registry.go

// Package tools defines the Tool interface and Registry for the agent
// system. Tools execute within a sandbox defined by SandboxConfig.
// The Registry acts as both a ToolExecutor (for the Runner) and a
// tool lister (for building LLM tool definitions).
package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/danmestas/dagnats/agent/llm"
)

// Tool is a single callable tool. Schema() returns the JSON Schema
// the LLM sees. Execute() runs the tool and returns the result.
type Tool interface {
	Name() string
	Description() string
	Schema() json.RawMessage
	Execute(
		ctx context.Context, input json.RawMessage,
	) (json.RawMessage, error)
}

// Registry holds all available tools. Thread-safe for concurrent
// registration and lookup. Implements both ToolExecutor (for Runner)
// and the toolLister interface (for building LLM schemas).
type Registry struct {
	mu    sync.RWMutex
	tools map[string]Tool
}

// NewRegistry creates an empty tool registry.
func NewRegistry() *Registry {
	return &Registry{tools: make(map[string]Tool)}
}

// Register adds a tool to the registry. Panics if the tool is nil
// or has an empty name — these are programmer errors.
func (r *Registry) Register(tool Tool) {
	if tool == nil {
		panic("Registry.Register: tool must not be nil")
	}
	if tool.Name() == "" {
		panic("Registry.Register: tool name must not be empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tools[tool.Name()] = tool
}

// Execute dispatches a tool call by name. Returns an error if the
// tool is not found. This method satisfies the ToolExecutor interface
// used by the agent Runner.
func (r *Registry) Execute(
	ctx context.Context,
	name string,
	input json.RawMessage,
) (json.RawMessage, error) {
	r.mu.RLock()
	tool, ok := r.tools[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown tool: %q", name)
	}
	return tool.Execute(ctx, input)
}

// ListToolDefs returns ToolDef schemas for the named tools. Unknown
// names are silently skipped. Used by the Runner to build the tools
// array sent to the LLM.
func (r *Registry) ListToolDefs(names []string) []llm.ToolDef {
	r.mu.RLock()
	defer r.mu.RUnlock()
	defs := make([]llm.ToolDef, 0, len(names))
	for _, name := range names {
		tool, ok := r.tools[name]
		if !ok {
			continue
		}
		defs = append(defs, llm.ToolDef{
			Name:        tool.Name(),
			Description: tool.Description(),
			InputSchema: tool.Schema(),
		})
	}
	return defs
}

// Names returns all registered tool names.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.tools))
	for name := range r.tools {
		names = append(names, name)
	}
	return names
}
