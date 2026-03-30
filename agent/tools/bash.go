// agent/tools/bash.go

// Bash tool: executes shell commands within sandbox constraints.
// Commands run with a timeout and output is capped at BashMaxOutput.
package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
)

// bashTool executes shell commands.
type bashTool struct {
	sandbox SandboxConfig
}

// NewBashTool creates a bash tool that respects sandbox constraints.
func NewBashTool(sandbox SandboxConfig) Tool {
	return &bashTool{sandbox: sandbox}
}

func (t *bashTool) Name() string { return "bash" }
func (t *bashTool) Description() string {
	return "Execute a bash command and return stdout/stderr. " +
		"Commands run in the workspace directory with a timeout."
}

func (t *bashTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"command": {
				"type": "string",
				"description": "The bash command to execute"
			}
		},
		"required": ["command"]
	}`)
}

func (t *bashTool) Execute(
	ctx context.Context, input json.RawMessage,
) (json.RawMessage, error) {
	var params struct {
		Command string `json:"command"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return nil, fmt.Errorf("parse input: %w", err)
	}
	if params.Command == "" {
		return nil, fmt.Errorf("command must not be empty")
	}

	timeout := t.sandbox.BashTimeout
	if timeout <= 0 {
		timeout = DefaultSandbox().BashTimeout
	}
	maxOutput := t.sandbox.BashMaxOutput
	if maxOutput <= 0 {
		maxOutput = DefaultSandbox().BashMaxOutput
	}

	cmdCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, "bash", "-c", params.Command)
	cmd.Dir = t.sandbox.WorkspaceDir

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	// Build result with both stdout and stderr.
	result := buildBashResult(
		stdout.Bytes(), stderr.Bytes(), err, maxOutput,
	)
	return json.Marshal(result)
}

// buildBashResult formats the command output as a string.
func buildBashResult(
	stdout, stderr []byte, err error, maxOutput int,
) string {
	var result bytes.Buffer

	stdoutStr := truncateBytes(stdout, maxOutput/2)
	stderrStr := truncateBytes(stderr, maxOutput/2)

	if len(stdoutStr) > 0 {
		result.WriteString(stdoutStr)
	}
	if len(stderrStr) > 0 {
		if result.Len() > 0 {
			result.WriteString("\n--- stderr ---\n")
		}
		result.WriteString(stderrStr)
	}
	if err != nil {
		if result.Len() > 0 {
			result.WriteString("\n")
		}
		result.WriteString(fmt.Sprintf("Exit error: %s", err.Error()))
	}
	if result.Len() == 0 {
		return "(no output)"
	}
	return result.String()
}

// truncateBytes returns the string representation of data, truncated
// to maxLen bytes with an indicator if truncated.
func truncateBytes(data []byte, maxLen int) string {
	if len(data) <= maxLen {
		return string(data)
	}
	return string(data[:maxLen]) + "\n... (truncated)"
}

// RegisterBashTool registers the bash tool on the registry.
func RegisterBashTool(registry *Registry, sandbox SandboxConfig) {
	registry.Register(NewBashTool(sandbox))
}
