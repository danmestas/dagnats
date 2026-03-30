// agent/tools/sandbox.go

// Sandbox enforcement for tool execution. ValidatePath checks that
// file paths stay within the workspace directory or allowed paths.
// All file-based tools delegate to these functions before performing
// any I/O.
package tools

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// SandboxConfig defines the security boundary for tool execution.
// Duplicated from agent/config.go to avoid a circular import.
// The worker handler copies values from agent.SandboxConfig into this.
type SandboxConfig struct {
	WorkspaceDir  string
	AllowedPaths  []string
	BashTimeout   time.Duration
	BashMaxOutput int
	NetworkAccess bool
}

// DefaultSandbox returns a permissive sandbox for development use.
// Production deployments should always provide an explicit config.
func DefaultSandbox() SandboxConfig {
	return SandboxConfig{
		WorkspaceDir:  "/",
		BashTimeout:   30 * time.Second,
		BashMaxOutput: 1024 * 1024,
		NetworkAccess: false,
	}
}

// ValidatePath checks that the given path is under the workspace
// directory or one of the allowed paths. Returns the cleaned absolute
// path or an error. Rejects path traversal attempts.
func ValidatePath(
	sandbox SandboxConfig, rawPath string,
) (string, error) {
	if rawPath == "" {
		return "", fmt.Errorf("path must not be empty")
	}

	// Resolve to absolute path relative to workspace.
	var absPath string
	if filepath.IsAbs(rawPath) {
		absPath = filepath.Clean(rawPath)
	} else {
		absPath = filepath.Clean(
			filepath.Join(sandbox.WorkspaceDir, rawPath),
		)
	}

	// Check workspace directory.
	if isUnderDir(absPath, sandbox.WorkspaceDir) {
		return absPath, nil
	}

	// Check allowed paths.
	for _, allowed := range sandbox.AllowedPaths {
		cleanAllowed := filepath.Clean(allowed)
		if isUnderDir(absPath, cleanAllowed) {
			return absPath, nil
		}
	}

	return "", fmt.Errorf(
		"path %q is outside sandbox (workspace: %q)",
		rawPath, sandbox.WorkspaceDir,
	)
}

// isUnderDir returns true if path is equal to or under dir.
func isUnderDir(path, dir string) bool {
	cleanDir := filepath.Clean(dir)
	if path == cleanDir {
		return true
	}
	prefix := cleanDir
	if !strings.HasSuffix(prefix, string(filepath.Separator)) {
		prefix += string(filepath.Separator)
	}
	return strings.HasPrefix(path, prefix)
}
