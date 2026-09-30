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
	"fmt"
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
	// The token is bound on the publish step, so only that step sees it.
	if secrets, _ := job["secrets"].([]any); len(secrets) != 0 {
		t.Fatalf("job secrets = %v, want none: the secret is step-scoped", secrets)
	}
	step := job["steps"].([]any)[0].(map[string]any)
	stepSecrets, _ := step["secrets"].([]any)
	if len(stepSecrets) != 1 || stepSecrets[0] != "CLOUDFLARE_API_TOKEN" {
		t.Fatalf("step secrets = %v, want [CLOUDFLARE_API_TOKEN]", step["secrets"])
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
	for _, key := range []string{"name", "run", "env", "working-directory", "secrets"} {
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

// TestJobsSecretOrPlainNotBoth: a variable bound to a secret anywhere in a
// job may not also be a plain value anywhere in it, since the job JSON would
// then hand a runner both for one key. Each case is diagnosed at the plain
// entry; the same secret bound at two levels is not a conflict.
func TestJobsSecretOrPlainNotBoth(t *testing.T) {
	const secretK = "${{ secrets.K }}"
	runDiagCases(t, []diagCase{
		{name: "workflow secret, job plain",
			spec: "env:\n  K: " + secretK + "\njobs:\n  a:\n    env:\n      K: lit\n" +
				"    steps:\n      - run: x\n",
			want: "either a secret or a plain value", marker: "K: lit"},
		{name: "job plain, step secret",
			spec: "jobs:\n  a:\n    env:\n      K: lit\n    steps:\n      - run: x\n" +
				"        env:\n          K: " + secretK + "\n",
			want: "either a secret or a plain value", marker: "K: lit"},
		{name: "one step secret, another step plain",
			spec: "jobs:\n  a:\n    steps:\n      - run: x\n        env:\n" +
				"          K: " + secretK + "\n      - run: y\n        env:\n          K: lit\n",
			want: "either a secret or a plain value", marker: "K: lit"},
	})
	spec := "env:\n  K: " + secretK + "\njobs:\n  a:\n    env:\n      K: " + secretK +
		"\n    steps:\n      - run: x\n"
	res, diags := compileJobs(t, spec, testNamespace)
	if len(diags) != 0 {
		t.Fatalf("the same secret at two levels is not a conflict: %+v", diags)
	}
	secrets := decodeJob(t, res.Workflow.Steps[0].Metadata["ci.job"])["secrets"].([]any)
	if len(secrets) != 1 || secrets[0] != "K" {
		t.Fatalf("secrets = %v, want [K] once", secrets)
	}
}

// TestJobsSecretsScopedAndSorted: workflow and job secrets reach every step
// through the job's list; a step's secret reaches that step alone. Lists
// are deduplicated and sorted, and no expression survives into the JSON.
func TestJobsSecretsScopedAndSorted(t *testing.T) {
	spec := "env:\n  ZED: ${{ secrets.ZED }}\njobs:\n  a:\n" +
		"    env:\n      ALPHA: ${{ secrets.ALPHA }}\n    steps:\n" +
		"      - run: x\n        env:\n          MID: ${{ secrets.MID }}\n" +
		"          ALPHA: ${{ secrets.ALPHA }}\n      - run: y\n"
	res, diags := compileJobs(t, spec, testNamespace)
	if len(diags) != 0 {
		t.Fatalf("unexpected diagnostics: %+v", diags)
	}
	raw := res.Workflow.Steps[0].Metadata["ci.job"]
	if strings.Contains(raw, "${{") {
		t.Fatalf("a secret expression survived: %s", raw)
	}
	job := decodeJob(t, raw)
	steps := job["steps"].([]any)
	for _, tc := range []struct {
		name string
		got  any
		want []string
	}{
		{"job", job["secrets"], []string{"ALPHA", "ZED"}},
		{"step 0", steps[0].(map[string]any)["secrets"], []string{"ALPHA", "MID"}},
		{"step 1", steps[1].(map[string]any)["secrets"], []string{}},
	} {
		got, _ := tc.got.([]any)
		if len(got) != len(tc.want) {
			t.Fatalf("%s secrets = %v, want %v", tc.name, tc.got, tc.want)
		}
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Fatalf("%s secrets = %v, want %v (deduped, sorted)", tc.name, got, tc.want)
			}
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

// TestJobsTotalPayloadBound: a workflow env value is copied into every job,
// so many jobs each under the per-job bound can still add up past the
// total. That is diagnosed at jobs:, and the same env with few jobs compiles.
func TestJobsTotalPayloadBound(t *testing.T) {
	build := func(jobCount int) string {
		var b strings.Builder
		b.WriteString("env:\n  BIG: " + strings.Repeat("x", 60000) + "\njobs:\n")
		for i := 0; i < jobCount; i++ {
			b.WriteString(fmt.Sprintf("  j%d:\n    steps:\n      - run: x\n", i))
		}
		return b.String()
	}
	spec := build(20)
	res, diags := compileJobs(t, spec, testNamespace)
	if len(res.Workflow.Steps) != 0 {
		t.Fatal("jobs over the total bound must not compile")
	}
	requireDiagAt(t, spec, diags, "together", "jobs:")
	if _, d := compileJobs(t, build(5), testNamespace); len(d) != 0 {
		t.Fatalf("five jobs sharing the env fit the bound: %+v", d)
	}
}

// TestJobsWorkflowTimeoutSaturates pins the overflow fix: jobs whose
// timeouts cap at the ceiling, never wrap negative. The first spec has each
// job alone over the ceiling; the second has jobs individually under it
// whose SUM is over, which is the branch that guards the running total.
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
	// 360m x 1001 attempts is about 0.69 years: under the cap alone, over
	// it in pairs.
	pair := "jobs:\n  a:\n    timeout-minutes: 360\n    retries: 1000\n" +
		"    steps:\n      - run: x\n  b:\n    timeout-minutes: 360\n" +
		"    retries: 1000\n    steps:\n      - run: y\n"
	summed, diags := compileJobs(t, pair, testNamespace)
	if len(diags) != 0 || summed.Workflow.Timeout != jobsWorkflowTimeoutMax {
		t.Fatalf("summed timeout = %v (%+v), want the %v ceiling",
			summed.Workflow.Timeout, diags, jobsWorkflowTimeoutMax)
	}
	one, _ := compileJobs(t, "jobs:\n  a:\n    steps:\n      - run: x\n", testNamespace)
	if one.Workflow.Timeout <= 0 || one.Workflow.Timeout >= jobsWorkflowTimeoutMax {
		t.Fatalf("a small spec's timeout %v must be positive and uncapped", one.Workflow.Timeout)
	}
}
