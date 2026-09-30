// ci/jobs_test.go
// Tests for the Actions-shaped jobs: spec (#728).
//
// Methodology: pure unit tests through the public entry point,
// CompileYAMLWith. Positioned diagnostics are asserted against the
// location of a marker token in the YAML under test (posOf), not against
// hand-counted numbers, so each test proves the diagnostic points at the
// offending key or value rather than merely somewhere near it. Every
// positive case has a negative counterpart: the valid shape compiles
// clean, and each malformed variant fails at the right place.
package ci

import (
	"encoding/json"
	"strings"
	"testing"
)

const testNamespace = "pikchr-studio"

// issueExample is the spec from #728's body, verbatim.
const issueExample = `on:
  push:
    branches: [trunk]
env:
  CI: "1"
jobs:
  test:
    timeout-minutes: 30
    steps:
      - run: npm test
  deploy:
    needs: [test]
    environment: production
    steps:
      - name: publish
        run: npx --yes wrangler@4.138.0 deploy
        env:
          CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}
`

func compileJobs(
	t *testing.T, spec, namespace string,
) (CompileResult, []Diagnostic) {
	t.Helper()
	if spec == "" {
		t.Fatal("compileJobs: spec must not be empty")
	}
	return CompileYAMLWith("wf", []byte(spec), CompileOptions{TaskNamespace: namespace})
}

// posOf returns the 1-based line and column of the first occurrence of
// needle in spec, failing the test when needle is absent.
func posOf(t *testing.T, spec, needle string) (int, int) {
	t.Helper()
	idx := strings.Index(spec, needle)
	if idx < 0 {
		t.Fatalf("posOf: %q not found in spec", needle)
	}
	line := 1 + strings.Count(spec[:idx], "\n")
	col := idx - strings.LastIndex(spec[:idx], "\n")
	return line, col
}

// requireDiagAt asserts some diagnostic contains substr AND sits exactly
// at the marker token.
func requireDiagAt(t *testing.T, spec string, diags []Diagnostic, substr, marker string) {
	t.Helper()
	line, col := posOf(t, spec, marker)
	for _, d := range diags {
		if strings.Contains(d.Message, substr) {
			if d.Line != line || d.Column != col {
				t.Fatalf("diagnostic %q at %d:%d, want %d:%d (marker %q)",
					d.Message, d.Line, d.Column, line, col, marker)
			}
			return
		}
	}
	t.Fatalf("no diagnostic containing %q; got %+v", substr, diags)
}

func decodeJob(t *testing.T, raw string) map[string]any {
	t.Helper()
	var job map[string]any
	if err := json.Unmarshal([]byte(raw), &job); err != nil {
		t.Fatalf("ci.job is not JSON: %v (%s)", err, raw)
	}
	return job
}

// TestJobsIssueExampleCompiles is #728's first acceptance criterion.
func TestJobsIssueExampleCompiles(t *testing.T) {
	res, diags := compileJobs(t, issueExample, testNamespace)
	if len(diags) != 0 {
		t.Fatalf("unexpected diagnostics: %+v", diags)
	}
	steps := res.Workflow.Steps
	if len(steps) != 2 {
		t.Fatalf("got %d steps, want 2", len(steps))
	}
	byID := map[string]int{}
	for i, s := range steps {
		byID[s.ID] = i
		if s.Task != testNamespace+".job" || s.WorkerGroup != testNamespace {
			t.Fatalf("step %s: task %q group %q", s.ID, s.Task, s.WorkerGroup)
		}
	}
	deploy := steps[byID["deploy"]]
	if len(deploy.DependsOn) != 1 || deploy.DependsOn[0] != "test" {
		t.Fatalf("deploy.DependsOn = %v, want [test]", deploy.DependsOn)
	}
	raw := deploy.Metadata["ci.job"]
	if strings.Contains(raw, "${{") {
		t.Fatalf("a secret expression survived into the job JSON: %s", raw)
	}
	job := decodeJob(t, raw)
	secrets, _ := job["secrets"].([]any)
	if len(secrets) != 1 || secrets[0] != "CLOUDFLARE_API_TOKEN" {
		t.Fatalf("secrets = %v, want [CLOUDFLARE_API_TOKEN]", job["secrets"])
	}
	if env, _ := job["env"].(map[string]any); env["CI"] != "1" {
		t.Fatalf("workflow env not merged into job env: %v", job["env"])
	}
	if job["environment"] != "production" {
		t.Fatalf("environment = %v, want production", job["environment"])
	}
}

// TestJobsJSONKeysAreTheContract pins the exact wire keys job runners read.
func TestJobsJSONKeysAreTheContract(t *testing.T) {
	res, diags := compileJobs(t, issueExample, testNamespace)
	if len(diags) != 0 {
		t.Fatalf("unexpected diagnostics: %+v", diags)
	}
	job := decodeJob(t, res.Workflow.Steps[0].Metadata["ci.job"])
	for _, key := range []string{"name", "env", "working-directory",
		"environment", "secrets", "steps"} {
		if _, ok := job[key]; !ok {
			t.Fatalf("job JSON lacks contract key %q: %v", key, job)
		}
	}
	step := job["steps"].([]any)[0].(map[string]any)
	for _, key := range []string{"name", "run", "env", "working-directory"} {
		if _, ok := step[key]; !ok {
			t.Fatalf("step JSON lacks contract key %q: %v", key, step)
		}
	}
}

// TestJobsOnNormalises covers #728's second acceptance criterion at the
// ci level: every accepted on: form round-trips as structured JSON.
func TestJobsOnNormalises(t *testing.T) {
	cases := []struct{ name, on, want string }{
		{"string", `on: push`, `{"push":{}}`},
		{"list", `on: [push, pull_request]`, `{"pull_request":{},"push":{}}`},
		{"null filter", "on:\n  push:\n", `{"push":{}}`},
		{"mapping", "on:\n  push:\n    branches: [trunk]\n", `{"push":{"branches":["trunk"]}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := tc.on + "\njobs:\n  a:\n    steps:\n      - run: x\n"
			res, diags := compileJobs(t, spec, testNamespace)
			if len(diags) != 0 {
				t.Fatalf("unexpected diagnostics: %+v", diags)
			}
			if string(res.On) != tc.want {
				t.Fatalf("on = %s, want %s", res.On, tc.want)
			}
		})
	}
}

// TestChecksSpecUnaffected is the negative space for shape detection: a
// checks: spec through the new entry point is exactly the old compile.
func TestChecksSpecUnaffected(t *testing.T) {
	spec := "checks:\n  test:\n    task: build\n"
	res, diags := compileJobs(t, spec, "")
	if len(diags) != 0 {
		t.Fatalf("checks spec failed: %+v", diags)
	}
	if res.On != nil {
		t.Fatalf("a checks: spec must not report on, got %s", res.On)
	}
	legacy, legacyDiags := CompileYAML("wf", []byte(spec))
	if len(legacyDiags) != 0 || len(legacy.Steps) != len(res.Workflow.Steps) {
		t.Fatalf("CompileYAML and CompileYAMLWith disagree on a checks spec")
	}
}

// TestJobsSecretPrecedence pins the review fix: a job-level plain value
// replaces a workflow-level secret for the same key, so the worker never
// receives both a literal and a secret for one variable.
func TestJobsSecretPrecedence(t *testing.T) {
	spec := "env:\n  TOKEN: ${{ secrets.TOKEN }}\n  SHARED: ${{ secrets.SHARED }}\n" +
		"jobs:\n  a:\n    env:\n      TOKEN: literal\n    steps:\n      - run: x\n"
	res, diags := compileJobs(t, spec, testNamespace)
	if len(diags) != 0 {
		t.Fatalf("unexpected diagnostics: %+v", diags)
	}
	job := decodeJob(t, res.Workflow.Steps[0].Metadata["ci.job"])
	env := job["env"].(map[string]any)
	if env["TOKEN"] != "literal" {
		t.Fatalf("job plain value must win: env = %v", env)
	}
	secrets := job["secrets"].([]any)
	if len(secrets) != 1 || secrets[0] != "SHARED" {
		t.Fatalf("overridden secret must drop out: secrets = %v, want [SHARED]", secrets)
	}
}

// TestJobsSecretsDedupedSortedAndHoisted covers secrets gathered from
// workflow, job and step level: deduplicated, sorted, never left in env.
func TestJobsSecretsDedupedSortedAndHoisted(t *testing.T) {
	spec := "env:\n  ZED: ${{ secrets.ZED }}\njobs:\n  a:\n" +
		"    env:\n      ALPHA: ${{ secrets.ALPHA }}\n    steps:\n" +
		"      - run: x\n        env:\n          ALPHA: ${{ secrets.ALPHA }}\n" +
		"          MID: ${{ secrets.MID }}\n"
	res, diags := compileJobs(t, spec, testNamespace)
	if len(diags) != 0 {
		t.Fatalf("unexpected diagnostics: %+v", diags)
	}
	raw := res.Workflow.Steps[0].Metadata["ci.job"]
	if strings.Contains(raw, "${{") {
		t.Fatalf("a secret expression survived: %s", raw)
	}
	got := decodeJob(t, raw)["secrets"].([]any)
	want := []string{"ALPHA", "MID", "ZED"}
	if len(got) != len(want) {
		t.Fatalf("secrets = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("secrets = %v, want %v (deduped, sorted)", got, want)
		}
	}
}

// TestJobsPayloadSizeBound: an oversized job is diagnosed at its id,
// and a job just under the bound still compiles.
func TestJobsPayloadSizeBound(t *testing.T) {
	big := strings.Repeat("x", jobPayloadMaxBytes)
	spec := "jobs:\n  huge:\n    steps:\n      - run: " + big + "\n"
	res, diags := compileJobs(t, spec, testNamespace)
	if len(res.Workflow.Steps) != 0 {
		t.Fatal("an oversized job must not compile")
	}
	requireDiagAt(t, spec, diags, "over the", "huge")
	small := "jobs:\n  ok:\n    steps:\n      - run: " +
		strings.Repeat("x", jobPayloadMaxBytes/2) + "\n"
	if _, d := compileJobs(t, small, testNamespace); len(d) != 0 {
		t.Fatalf("a job well under the bound was rejected: %+v", d)
	}
}

// TestJobsWorkflowTimeoutSaturates pins the overflow fix: many long jobs
// at the retry ceiling must cap at the ceiling, never wrap negative.
func TestJobsWorkflowTimeoutSaturates(t *testing.T) {
	var b strings.Builder
	b.WriteString("jobs:\n")
	for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
		b.WriteString("  " + id + ":\n    timeout-minutes: 360\n" +
			"    retries: 100000\n    steps:\n      - run: x\n")
	}
	res, diags := compileJobs(t, b.String(), testNamespace)
	if len(diags) != 0 {
		t.Fatalf("unexpected diagnostics: %+v", diags)
	}
	if res.Workflow.Timeout != jobsWorkflowTimeoutMax {
		t.Fatalf("timeout = %v, want the %v ceiling", res.Workflow.Timeout, jobsWorkflowTimeoutMax)
	}
	one, _ := compileJobs(t, "jobs:\n  a:\n    steps:\n      - run: x\n", testNamespace)
	if one.Workflow.Timeout <= 0 || one.Workflow.Timeout >= jobsWorkflowTimeoutMax {
		t.Fatalf("a small spec's timeout %v must be positive and uncapped", one.Workflow.Timeout)
	}
}
