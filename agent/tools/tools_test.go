// agent/tools/tools_test.go
// Tests for tool registry, sandbox validation, and built-in tools.
// Methodology: Each test verifies positive behavior and negative space.

package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// --- Registry Tests ---

func TestRegistry_RegisterAndExecute(t *testing.T) {
	reg := NewRegistry()
	reg.Register(&stubTool{name: "echo"})
	result, err := reg.Execute(
		context.Background(), "echo",
		json.RawMessage(`{"msg":"hello"}`),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestRegistry_ExecuteUnknownTool(t *testing.T) {
	reg := NewRegistry()
	_, err := reg.Execute(
		context.Background(), "missing",
		json.RawMessage(`{}`),
	)
	if err == nil {
		t.Fatal("expected error for unknown tool")
	}
}

func TestRegistry_ListToolDefs(t *testing.T) {
	reg := NewRegistry()
	reg.Register(&stubTool{name: "read_file"})
	reg.Register(&stubTool{name: "bash"})
	defs := reg.ListToolDefs([]string{"read_file", "bash", "missing"})
	if len(defs) != 2 {
		t.Fatalf("expected 2 defs, got %d", len(defs))
	}
	// "missing" should be silently skipped.
}

func TestRegistry_PanicsOnNilTool(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	reg := NewRegistry()
	reg.Register(nil)
}

// --- Sandbox Tests ---

func TestValidatePath_WithinWorkspace(t *testing.T) {
	sandbox := SandboxConfig{WorkspaceDir: "/workspace"}
	path, err := ValidatePath(sandbox, "/workspace/src/main.go")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if path != "/workspace/src/main.go" {
		t.Fatalf("expected /workspace/src/main.go, got %q", path)
	}
}

func TestValidatePath_RelativePath(t *testing.T) {
	sandbox := SandboxConfig{WorkspaceDir: "/workspace"}
	path, err := ValidatePath(sandbox, "src/main.go")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if path != "/workspace/src/main.go" {
		t.Fatalf("expected /workspace/src/main.go, got %q", path)
	}
}

func TestValidatePath_OutsideWorkspace(t *testing.T) {
	sandbox := SandboxConfig{WorkspaceDir: "/workspace"}
	_, err := ValidatePath(sandbox, "/etc/passwd")
	if err == nil {
		t.Fatal("expected error for path outside workspace")
	}
}

func TestValidatePath_TraversalAttack(t *testing.T) {
	sandbox := SandboxConfig{WorkspaceDir: "/workspace"}
	_, err := ValidatePath(sandbox, "/workspace/../etc/passwd")
	if err == nil {
		t.Fatal("expected error for path traversal")
	}
}

func TestValidatePath_AllowedPaths(t *testing.T) {
	sandbox := SandboxConfig{
		WorkspaceDir: "/workspace",
		AllowedPaths: []string{"/usr/share/dict"},
	}
	path, err := ValidatePath(sandbox, "/usr/share/dict/words")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if path != "/usr/share/dict/words" {
		t.Fatalf("expected /usr/share/dict/words, got %q", path)
	}
}

func TestValidatePath_EmptyPath(t *testing.T) {
	sandbox := SandboxConfig{WorkspaceDir: "/workspace"}
	_, err := ValidatePath(sandbox, "")
	if err == nil {
		t.Fatal("expected error for empty path")
	}
}

// --- File Tool Tests ---

func TestReadFileTool(t *testing.T) {
	dir := t.TempDir()
	testFile := filepath.Join(dir, "test.txt")
	os.WriteFile(testFile, []byte("hello world"), 0644)

	sandbox := SandboxConfig{WorkspaceDir: dir}
	tool := NewReadFileTool(sandbox)
	input, _ := json.Marshal(map[string]string{"path": "test.txt"})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var content string
	json.Unmarshal(result, &content)
	if content != "hello world" {
		t.Fatalf("expected 'hello world', got %q", content)
	}
}

func TestReadFileTool_OutsideSandbox(t *testing.T) {
	sandbox := SandboxConfig{WorkspaceDir: t.TempDir()}
	tool := NewReadFileTool(sandbox)
	input, _ := json.Marshal(map[string]string{"path": "/etc/passwd"})
	_, err := tool.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for path outside sandbox")
	}
}

func TestWriteFileTool(t *testing.T) {
	dir := t.TempDir()
	sandbox := SandboxConfig{WorkspaceDir: dir}
	tool := NewWriteFileTool(sandbox)
	input, _ := json.Marshal(map[string]interface{}{
		"path":    "sub/new.txt",
		"content": "new content",
	})
	_, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "sub/new.txt"))
	if err != nil {
		t.Fatalf("read written file: %v", err)
	}
	if string(data) != "new content" {
		t.Fatalf("expected 'new content', got %q", string(data))
	}
}

func TestEditFileTool(t *testing.T) {
	dir := t.TempDir()
	testFile := filepath.Join(dir, "edit.txt")
	os.WriteFile(testFile, []byte("hello world"), 0644)

	sandbox := SandboxConfig{WorkspaceDir: dir}
	tool := NewEditFileTool(sandbox)
	input, _ := json.Marshal(map[string]string{
		"path":       "edit.txt",
		"old_string": "world",
		"new_string": "dagnats",
	})
	_, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	data, _ := os.ReadFile(testFile)
	if string(data) != "hello dagnats" {
		t.Fatalf("expected 'hello dagnats', got %q", string(data))
	}
}

func TestEditFileTool_NotFound(t *testing.T) {
	dir := t.TempDir()
	testFile := filepath.Join(dir, "edit.txt")
	os.WriteFile(testFile, []byte("hello world"), 0644)

	sandbox := SandboxConfig{WorkspaceDir: dir}
	tool := NewEditFileTool(sandbox)
	input, _ := json.Marshal(map[string]string{
		"path":       "edit.txt",
		"old_string": "missing",
		"new_string": "replaced",
	})
	_, err := tool.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error when old_string not found")
	}
}

func TestListDirTool(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0644)
	os.Mkdir(filepath.Join(dir, "subdir"), 0755)

	sandbox := SandboxConfig{WorkspaceDir: dir}
	tool := NewListDirTool(sandbox)
	input, _ := json.Marshal(map[string]string{"path": "."})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var listing string
	json.Unmarshal(result, &listing)
	if listing == "" {
		t.Fatal("expected non-empty listing")
	}
}

// --- Bash Tool Tests ---

func TestBashTool_SimpleCommand(t *testing.T) {
	sandbox := SandboxConfig{
		WorkspaceDir:  t.TempDir(),
		BashTimeout:   5 * time.Second,
		BashMaxOutput: 4096,
	}
	tool := NewBashTool(sandbox)
	input, _ := json.Marshal(map[string]string{
		"command": "echo hello",
	})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var output string
	json.Unmarshal(result, &output)
	if output == "" {
		t.Fatal("expected non-empty output")
	}
}

func TestBashTool_EmptyCommand(t *testing.T) {
	sandbox := SandboxConfig{
		WorkspaceDir:  t.TempDir(),
		BashTimeout:   5 * time.Second,
		BashMaxOutput: 4096,
	}
	tool := NewBashTool(sandbox)
	input, _ := json.Marshal(map[string]string{"command": ""})
	_, err := tool.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for empty command")
	}
}

func TestBashTool_FailingCommand(t *testing.T) {
	sandbox := SandboxConfig{
		WorkspaceDir:  t.TempDir(),
		BashTimeout:   5 * time.Second,
		BashMaxOutput: 4096,
	}
	tool := NewBashTool(sandbox)
	input, _ := json.Marshal(map[string]string{
		"command": "exit 1",
	})
	result, err := tool.Execute(context.Background(), input)
	// Bash tool returns results even on failure (exit code in output).
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var output string
	json.Unmarshal(result, &output)
	if output == "" {
		t.Fatal("expected output with exit error")
	}
}

// --- Search Tool Tests ---

func TestGrepTool(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "main.go"),
		[]byte("package main\nfunc main() {}\n"), 0644)

	sandbox := SandboxConfig{WorkspaceDir: dir}
	tool := NewGrepTool(sandbox)
	input, _ := json.Marshal(map[string]string{
		"pattern": "func main",
	})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var output string
	json.Unmarshal(result, &output)
	if output == "No matches found" {
		t.Fatal("expected to find 'func main'")
	}
}

func TestGlobTool(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "main.go"), []byte(""), 0644)
	os.WriteFile(filepath.Join(dir, "main.txt"), []byte(""), 0644)

	sandbox := SandboxConfig{WorkspaceDir: dir}
	tool := NewGlobTool(sandbox)
	input, _ := json.Marshal(map[string]string{
		"pattern": "*.go",
	})
	result, err := tool.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var output string
	json.Unmarshal(result, &output)
	if output == "" {
		t.Fatal("expected to find *.go files")
	}
}

// --- Registration Helper Tests ---

func TestRegisterAllTools(t *testing.T) {
	reg := NewRegistry()
	sandbox := DefaultSandbox()
	RegisterFileTools(reg, sandbox)
	RegisterSearchTools(reg, sandbox)
	RegisterBashTool(reg, sandbox)
	names := reg.Names()
	if len(names) < 7 {
		t.Fatalf("expected at least 7 tools, got %d", len(names))
	}
}

// --- Helpers ---

type stubTool struct {
	name string
}

func (s *stubTool) Name() string        { return s.name }
func (s *stubTool) Description() string { return "stub" }
func (s *stubTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object"}`)
}
func (s *stubTool) Execute(
	ctx context.Context, input json.RawMessage,
) (json.RawMessage, error) {
	return json.RawMessage(`"ok"`), nil
}
