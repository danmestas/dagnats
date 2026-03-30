// agent/tools/search.go

// Search tools: glob and grep. Glob finds files by pattern, grep
// searches file contents. Both respect sandbox path constraints.
package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// globTool finds files matching a glob pattern.
type globTool struct {
	sandbox SandboxConfig
}

func NewGlobTool(sandbox SandboxConfig) Tool {
	return &globTool{sandbox: sandbox}
}

func (t *globTool) Name() string { return "glob" }
func (t *globTool) Description() string {
	return "Find files matching a glob pattern (e.g. '**/*.go')."
}

func (t *globTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"pattern": {
				"type": "string",
				"description": "Glob pattern to match files"
			},
			"path": {
				"type": "string",
				"description": "Base directory to search in"
			}
		},
		"required": ["pattern"]
	}`)
}

func (t *globTool) Execute(
	ctx context.Context, input json.RawMessage,
) (json.RawMessage, error) {
	var params struct {
		Pattern string `json:"pattern"`
		Path    string `json:"path"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return nil, fmt.Errorf("parse input: %w", err)
	}
	baseDir := params.Path
	if baseDir == "" {
		baseDir = t.sandbox.WorkspaceDir
	}
	absBase, err := ValidatePath(t.sandbox, baseDir)
	if err != nil {
		return nil, err
	}

	var matches []string
	const maxResults = 500
	const maxWalk = 50000

	walkCount := 0
	err = filepath.Walk(absBase, func(
		path string, info os.FileInfo, err error,
	) error {
		if err != nil {
			return nil // skip inaccessible entries
		}
		walkCount++
		if walkCount > maxWalk {
			return filepath.SkipAll
		}
		relPath, relErr := filepath.Rel(absBase, path)
		if relErr != nil {
			return nil
		}
		matched, matchErr := filepath.Match(
			params.Pattern, filepath.Base(path),
		)
		if matchErr != nil {
			return nil
		}
		// Also try matching against the relative path for
		// patterns like "**/*.go" (simplified — no recursive glob).
		if !matched {
			matched, _ = filepath.Match(
				params.Pattern, relPath,
			)
		}
		if matched && len(matches) < maxResults {
			matches = append(matches, relPath)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk directory: %w", err)
	}

	return json.Marshal(strings.Join(matches, "\n"))
}

// grepTool searches file contents for a pattern.
type grepTool struct {
	sandbox SandboxConfig
}

func NewGrepTool(sandbox SandboxConfig) Tool {
	return &grepTool{sandbox: sandbox}
}

func (t *grepTool) Name() string { return "grep" }
func (t *grepTool) Description() string {
	return "Search for a text pattern in files. Returns matching " +
		"lines with file paths and line numbers."
}

func (t *grepTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"pattern": {
				"type": "string",
				"description": "Text pattern to search for"
			},
			"path": {
				"type": "string",
				"description": "File or directory to search in"
			},
			"file_pattern": {
				"type": "string",
				"description": "Only search files matching this glob"
			}
		},
		"required": ["pattern"]
	}`)
}

func (t *grepTool) Execute(
	ctx context.Context, input json.RawMessage,
) (json.RawMessage, error) {
	var params struct {
		Pattern     string `json:"pattern"`
		Path        string `json:"path"`
		FilePattern string `json:"file_pattern"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return nil, fmt.Errorf("parse input: %w", err)
	}
	searchDir := params.Path
	if searchDir == "" {
		searchDir = t.sandbox.WorkspaceDir
	}
	absDir, err := ValidatePath(t.sandbox, searchDir)
	if err != nil {
		return nil, err
	}

	var results []string
	const maxResults = 200
	const maxWalk = 50000

	walkCount := 0
	err = filepath.Walk(absDir, func(
		path string, info os.FileInfo, err error,
	) error {
		if err != nil || info.IsDir() {
			return nil
		}
		walkCount++
		if walkCount > maxWalk {
			return filepath.SkipAll
		}
		if len(results) >= maxResults {
			return filepath.SkipAll
		}
		// Filter by file pattern if specified.
		if params.FilePattern != "" {
			matched, _ := filepath.Match(
				params.FilePattern, filepath.Base(path),
			)
			if !matched {
				return nil
			}
		}
		// Skip binary files (rough heuristic: check first 512 bytes).
		if isBinaryFile(path) {
			return nil
		}
		searchFile(path, absDir, params.Pattern, &results, maxResults)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}

	if len(results) == 0 {
		return json.Marshal("No matches found")
	}
	return json.Marshal(strings.Join(results, "\n"))
}

// searchFile reads a file and appends matching lines to results.
func searchFile(
	path, baseDir, pattern string,
	results *[]string,
	maxResults int,
) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	relPath, err := filepath.Rel(baseDir, path)
	if err != nil {
		relPath = path
	}
	lines := strings.Split(string(data), "\n")
	for i, line := range lines {
		if len(*results) >= maxResults {
			return
		}
		if strings.Contains(line, pattern) {
			*results = append(*results, fmt.Sprintf(
				"%s:%d:%s", relPath, i+1, line))
		}
	}
}

// isBinaryFile checks if a file likely contains binary content.
func isBinaryFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return true
	}
	defer f.Close()
	buf := make([]byte, 512)
	n, err := f.Read(buf)
	if err != nil || n == 0 {
		return true
	}
	for _, b := range buf[:n] {
		if b == 0 {
			return true
		}
	}
	return false
}

// RegisterSearchTools registers glob and grep on the registry.
func RegisterSearchTools(registry *Registry, sandbox SandboxConfig) {
	registry.Register(NewGlobTool(sandbox))
	registry.Register(NewGrepTool(sandbox))
}
