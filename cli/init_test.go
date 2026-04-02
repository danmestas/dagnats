// cli/init_test.go
// Pure unit tests for the init scaffold command. No NATS required --
// creates project directories on disk with boilerplate files.
// Methodology: call scaffoldProject in temp directories, assert on
// file existence, contents, and error cases for valid and invalid inputs.
package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestScaffoldProjectCreatesDirectory(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "my-workflow")

	err := scaffoldProject(dir, "my-workflow")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Positive: directory must exist.
	info, statErr := os.Stat(dir)
	if statErr != nil {
		t.Fatalf("expected directory to exist: %v", statErr)
	}
	if !info.IsDir() {
		t.Fatal("expected path to be a directory")
	}

	// Positive: workflow.json must exist.
	wfPath := filepath.Join(dir, "workflow.json")
	if _, statErr := os.Stat(wfPath); statErr != nil {
		t.Fatalf("expected workflow.json to exist: %v", statErr)
	}

	// Positive: main.go must exist.
	mainPath := filepath.Join(dir, "main.go")
	if _, statErr := os.Stat(mainPath); statErr != nil {
		t.Fatalf("expected main.go to exist: %v", statErr)
	}

	// Negative: no extra files beyond workflow.json and main.go.
	entries, readErr := os.ReadDir(dir)
	if readErr != nil {
		t.Fatalf("read dir: %v", readErr)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 files, got %d", len(entries))
	}
}

func TestScaffoldProjectWorkflowContainsName(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "test-proj")

	err := scaffoldProject(dir, "test-proj")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	data, readErr := os.ReadFile(filepath.Join(dir, "workflow.json"))
	if readErr != nil {
		t.Fatalf("read workflow.json: %v", readErr)
	}

	// Positive: workflow name must match project name.
	var wf map[string]any
	if err := json.Unmarshal(data, &wf); err != nil {
		t.Fatalf("unmarshal workflow.json: %v", err)
	}
	if wf["name"] != "test-proj" {
		t.Fatalf(
			"expected name 'test-proj', got %q", wf["name"],
		)
	}

	// Positive: must have steps array.
	steps, ok := wf["steps"].([]any)
	if !ok || len(steps) == 0 {
		t.Fatal("expected non-empty steps array")
	}

	// Negative: must not contain placeholder text.
	if strings.Contains(string(data), "{{") {
		t.Fatal("workflow.json contains unresolved template")
	}
}

func TestScaffoldProjectMainGoContainsHandler(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "handler-proj")

	err := scaffoldProject(dir, "handler-proj")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	data, readErr := os.ReadFile(filepath.Join(dir, "main.go"))
	if readErr != nil {
		t.Fatalf("read main.go: %v", readErr)
	}

	content := string(data)

	// Positive: must contain worker Handle call.
	if !strings.Contains(content, `w.Handle("process"`) {
		t.Fatal("main.go must contain w.Handle(\"process\"")
	}

	// Positive: must be a main package.
	if !strings.Contains(content, "package main") {
		t.Fatal("main.go must declare package main")
	}

	// Negative: must not contain template markers.
	if strings.Contains(content, "{{") {
		t.Fatal("main.go contains unresolved template")
	}
}

func TestScaffoldProjectFailsIfDirExists(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "existing")

	// Pre-create the directory.
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	err := scaffoldProject(dir, "existing")

	// Positive: must return an error.
	if err == nil {
		t.Fatal("expected error when directory already exists")
	}

	// Positive: error must mention "already exists".
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf(
			"expected 'already exists' in error, got: %v", err,
		)
	}
}

func TestValidateProjectNameRejectsEmpty(t *testing.T) {
	err := validateProjectName("")

	// Positive: must return error for empty name.
	if err == nil {
		t.Fatal("expected error for empty name")
	}

	// Positive: error must mention empty.
	if !strings.Contains(err.Error(), "empty") {
		t.Fatalf("expected 'empty' in error, got: %v", err)
	}
}

func TestValidateProjectNameRejectsInvalidChars(t *testing.T) {
	err := validateProjectName("bad name!")

	// Positive: must return error for invalid chars.
	if err == nil {
		t.Fatal("expected error for name with spaces/symbols")
	}

	// Positive: error must mention allowed chars.
	if !strings.Contains(err.Error(), "alphanumeric") {
		t.Fatalf(
			"expected 'alphanumeric' in error, got: %v", err,
		)
	}

	// Negative: valid names must pass.
	if err := validateProjectName("my-workflow-2"); err != nil {
		t.Fatalf("expected valid name to pass, got: %v", err)
	}
}

func TestValidateProjectNameRejectsLeadingHyphen(t *testing.T) {
	err := validateProjectName("-leading")

	// Positive: must reject leading hyphen.
	if err == nil {
		t.Fatal("expected error for leading hyphen")
	}

	// Negative: trailing hyphen at end is still alphanumeric
	// pattern violation only if we want, but name like "a-b" is ok.
	if err := validateProjectName("a-b"); err != nil {
		t.Fatalf("expected 'a-b' to be valid, got: %v", err)
	}
}

func TestInitResultJSON(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "json-proj")

	err := scaffoldProject(dir, "json-proj")
	if err != nil {
		t.Fatalf("scaffold: %v", err)
	}

	result := initResult{
		Name:      "json-proj",
		Directory: dir,
		Files:     []string{"workflow.json", "main.go"},
	}

	data, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		t.Fatalf("marshal: %v", marshalErr)
	}

	// Positive: must contain the project name.
	if !strings.Contains(string(data), "json-proj") {
		t.Fatal("JSON must contain project name")
	}

	// Positive: must contain files array.
	var decoded initResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(decoded.Files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(decoded.Files))
	}

	// Negative: error field must be empty on success.
	if decoded.Error != "" {
		t.Fatalf("expected empty error, got %q", decoded.Error)
	}
}
