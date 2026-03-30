// agent/tools/file.go

// File tools: read_file, write_file, edit_file, list_dir. All
// operations validate paths against the SandboxConfig before any I/O.
package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// readFileTool reads file contents.
type readFileTool struct {
	sandbox SandboxConfig
}

func NewReadFileTool(sandbox SandboxConfig) Tool {
	return &readFileTool{sandbox: sandbox}
}

func (t *readFileTool) Name() string { return "read_file" }
func (t *readFileTool) Description() string {
	return "Read the contents of a file at the given path."
}

func (t *readFileTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"path": {
				"type": "string",
				"description": "The file path to read"
			}
		},
		"required": ["path"]
	}`)
}

func (t *readFileTool) Execute(
	ctx context.Context, input json.RawMessage,
) (json.RawMessage, error) {
	var params struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return nil, fmt.Errorf("parse input: %w", err)
	}
	absPath, err := ValidatePath(t.sandbox, params.Path)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	// Cap output at 1MB to prevent huge responses.
	const maxFileSize = 1024 * 1024
	if len(data) > maxFileSize {
		data = data[:maxFileSize]
	}
	return json.Marshal(string(data))
}

// writeFileTool writes content to a file, creating directories as needed.
type writeFileTool struct {
	sandbox SandboxConfig
}

func NewWriteFileTool(sandbox SandboxConfig) Tool {
	return &writeFileTool{sandbox: sandbox}
}

func (t *writeFileTool) Name() string { return "write_file" }
func (t *writeFileTool) Description() string {
	return "Write content to a file. Creates the file and parent " +
		"directories if they don't exist."
}

func (t *writeFileTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"path": {
				"type": "string",
				"description": "The file path to write to"
			},
			"content": {
				"type": "string",
				"description": "The content to write"
			}
		},
		"required": ["path", "content"]
	}`)
}

func (t *writeFileTool) Execute(
	ctx context.Context, input json.RawMessage,
) (json.RawMessage, error) {
	var params struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return nil, fmt.Errorf("parse input: %w", err)
	}
	absPath, err := ValidatePath(t.sandbox, params.Path)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(absPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}
	if err := os.WriteFile(absPath, []byte(params.Content), 0644); err != nil {
		return nil, fmt.Errorf("write file: %w", err)
	}
	return json.Marshal(fmt.Sprintf("Wrote %d bytes to %s",
		len(params.Content), params.Path))
}

// editFileTool performs exact string replacement in a file.
type editFileTool struct {
	sandbox SandboxConfig
}

func NewEditFileTool(sandbox SandboxConfig) Tool {
	return &editFileTool{sandbox: sandbox}
}

func (t *editFileTool) Name() string { return "edit_file" }
func (t *editFileTool) Description() string {
	return "Edit a file by replacing an exact string with a new " +
		"string. The old_string must appear exactly once in the file."
}

func (t *editFileTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"path": {
				"type": "string",
				"description": "The file path to edit"
			},
			"old_string": {
				"type": "string",
				"description": "The exact string to find and replace"
			},
			"new_string": {
				"type": "string",
				"description": "The replacement string"
			}
		},
		"required": ["path", "old_string", "new_string"]
	}`)
}

func (t *editFileTool) Execute(
	ctx context.Context, input json.RawMessage,
) (json.RawMessage, error) {
	var params struct {
		Path      string `json:"path"`
		OldString string `json:"old_string"`
		NewString string `json:"new_string"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return nil, fmt.Errorf("parse input: %w", err)
	}
	absPath, err := ValidatePath(t.sandbox, params.Path)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	content := string(data)
	count := strings.Count(content, params.OldString)
	if count == 0 {
		return nil, fmt.Errorf(
			"old_string not found in %s", params.Path)
	}
	if count > 1 {
		return nil, fmt.Errorf(
			"old_string found %d times in %s (must be unique)",
			count, params.Path)
	}
	newContent := strings.Replace(
		content, params.OldString, params.NewString, 1)
	if err := os.WriteFile(absPath, []byte(newContent), 0644); err != nil {
		return nil, fmt.Errorf("write file: %w", err)
	}
	return json.Marshal("Edit applied successfully")
}

// listDirTool lists directory contents.
type listDirTool struct {
	sandbox SandboxConfig
}

func NewListDirTool(sandbox SandboxConfig) Tool {
	return &listDirTool{sandbox: sandbox}
}

func (t *listDirTool) Name() string { return "list_dir" }
func (t *listDirTool) Description() string {
	return "List the contents of a directory."
}

func (t *listDirTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"path": {
				"type": "string",
				"description": "The directory path to list"
			}
		},
		"required": ["path"]
	}`)
}

func (t *listDirTool) Execute(
	ctx context.Context, input json.RawMessage,
) (json.RawMessage, error) {
	var params struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return nil, fmt.Errorf("parse input: %w", err)
	}
	absPath, err := ValidatePath(t.sandbox, params.Path)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(absPath)
	if err != nil {
		return nil, fmt.Errorf("read directory: %w", err)
	}
	const maxEntries = 1000
	var lines []string
	for i, entry := range entries {
		if i >= maxEntries {
			lines = append(lines, fmt.Sprintf(
				"... and %d more entries", len(entries)-maxEntries))
			break
		}
		suffix := ""
		if entry.IsDir() {
			suffix = "/"
		}
		lines = append(lines, entry.Name()+suffix)
	}
	return json.Marshal(strings.Join(lines, "\n"))
}

// RegisterFileTools registers all file-based tools on the registry.
func RegisterFileTools(registry *Registry, sandbox SandboxConfig) {
	registry.Register(NewReadFileTool(sandbox))
	registry.Register(NewWriteFileTool(sandbox))
	registry.Register(NewEditFileTool(sandbox))
	registry.Register(NewListDirTool(sandbox))
}
