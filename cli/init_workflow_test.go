// cli/init_workflow_test.go
// Pure unit tests for the init workflow scaffold command. No NATS required --
// generates workflow JSON files on disk with step definitions.
// Methodology: call scaffoldWorkflow in temp directories, assert on file
// existence, contents, step chaining, and error cases.
package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitWorkflow_SingleStep(t *testing.T) {
	dir := t.TempDir()

	err := scaffoldWorkflow(dir, "image-resize", nil)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	path := filepath.Join(dir, "image-resize.json")
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("expected file to exist: %v", readErr)
	}

	content := string(data)

	// Positive: must contain the workflow name.
	if !strings.Contains(content, `"name": "image-resize"`) {
		t.Fatal("workflow must contain name 'image-resize'")
	}

	// Positive: default step task must be named <name>-process.
	if !strings.Contains(
		content, `"task": "image-resize-process"`,
	) {
		t.Fatal(
			"default step task must be 'image-resize-process'",
		)
	}

	// Negative: single step must not have depends_on.
	if strings.Contains(content, "depends_on") {
		t.Fatal("single step must not have depends_on")
	}
}

func TestInitWorkflow_MultipleSteps(t *testing.T) {
	dir := t.TempDir()

	steps := []string{"fetch", "transform", "load"}
	err := scaffoldWorkflow(dir, "etl-pipeline", steps)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	path := filepath.Join(dir, "etl-pipeline.json")
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("expected file to exist: %v", readErr)
	}

	content := string(data)

	// Positive: all task names must be present.
	for _, step := range steps {
		taskName := "etl-pipeline-" + step
		if !strings.Contains(content, taskName) {
			t.Fatalf("expected task %q in workflow", taskName)
		}
	}

	// Positive: load step depends on transform (linear chain).
	var wf map[string]any
	if err := json.Unmarshal(data, &wf); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	stepsArr, ok := wf["steps"].([]any)
	if !ok {
		t.Fatal("expected steps array")
	}
	if len(stepsArr) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(stepsArr))
	}

	// The load step (index 2) must depend on transform.
	loadStep, ok := stepsArr[2].(map[string]any)
	if !ok {
		t.Fatal("expected load step to be object")
	}
	deps, ok := loadStep["depends_on"].([]any)
	if !ok || len(deps) == 0 {
		t.Fatal("load step must have depends_on")
	}
	if deps[0] != "transform" {
		t.Fatalf(
			"load must depend on 'transform', got %v", deps[0],
		)
	}

	// Negative: first step must not have depends_on.
	fetchStep, ok := stepsArr[0].(map[string]any)
	if !ok {
		t.Fatal("expected fetch step to be object")
	}
	if _, hasDeps := fetchStep["depends_on"]; hasDeps {
		t.Fatal("first step must not have depends_on")
	}
}

// TestInitWorkflow_IncludesDisabledTriggerStub confirms the scaffold
// emits a disabled trigger block so operators discover the embedded-
// trigger pattern (issue #181). Disabled means it's dormant — the
// generated workflow registers cleanly without firing on a schedule.
func TestInitWorkflow_IncludesDisabledTriggerStub(t *testing.T) {
	dir := t.TempDir()

	if err := scaffoldWorkflow(dir, "demo-wf", nil); err != nil {
		t.Fatalf("scaffoldWorkflow: %v", err)
	}

	path := filepath.Join(dir, "demo-wf.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var wf map[string]any
	if err := json.Unmarshal(data, &wf); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	triggers, ok := wf["triggers"].([]any)
	// Positive: triggers array exists.
	if !ok {
		t.Fatalf("expected triggers array in scaffold, got: %v",
			wf["triggers"])
	}
	if len(triggers) != 1 {
		t.Fatalf("expected 1 trigger stub, got %d", len(triggers))
	}
	stub, ok := triggers[0].(map[string]any)
	if !ok {
		t.Fatal("trigger stub must be an object")
	}
	// Positive: stub is disabled by default.
	if enabled, _ := stub["enabled"].(bool); enabled {
		t.Fatal("scaffolded trigger stub must default to disabled")
	}
	// Positive: stub uses a 5-field cron (per #172).
	cron, ok := stub["cron"].(map[string]any)
	if !ok {
		t.Fatal("stub must include a cron block")
	}
	expr, _ := cron["expression"].(string)
	if strings.Count(expr, " ") != 4 {
		t.Fatalf(
			"cron expression must be 5-field, got %q "+
				"(field count = %d)",
			expr, strings.Count(expr, " ")+1)
	}
}

func TestInitWorkflow_AlreadyExists(t *testing.T) {
	dir := t.TempDir()

	// Pre-create the file.
	path := filepath.Join(dir, "my-wf.json")
	if err := os.WriteFile(
		path, []byte("{}"), 0644,
	); err != nil {
		t.Fatalf("pre-create: %v", err)
	}

	err := scaffoldWorkflow(dir, "my-wf", nil)

	// Positive: must return an error.
	if err == nil {
		t.Fatal("expected error when file already exists")
	}

	// Positive: error must mention "already exists".
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf(
			"expected 'already exists' in error, got: %v", err,
		)
	}
}

func TestToPascalCase(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"image-pipeline", "ImagePipeline"},
		{"etl", "Etl"},
		{"my-cool-workflow", "MyCoolWorkflow"},
	}
	for _, tc := range cases {
		got := toPascalCase(tc.input)

		// Positive: must match expected PascalCase.
		if got != tc.want {
			t.Fatalf(
				"toPascalCase(%q) = %q, want %q",
				tc.input, got, tc.want,
			)
		}
	}

	// Negative: must not contain hyphens.
	got := toPascalCase("a-b-c")
	if strings.Contains(got, "-") {
		t.Fatalf("PascalCase must not contain hyphens: %q", got)
	}
}

func TestScaffoldWorkflow_TooManySteps(t *testing.T) {
	dir := t.TempDir()

	steps := make([]string, 21)
	for i := range steps {
		steps[i] = "step"
	}

	err := scaffoldWorkflow(dir, "big", steps)

	// Positive: must return an error.
	if err == nil {
		t.Fatal("expected error for > 20 steps")
	}

	// Negative: file must not have been created.
	path := filepath.Join(dir, "big.json")
	if _, statErr := os.Stat(path); statErr == nil {
		t.Fatal("file should not exist after error")
	}
}
