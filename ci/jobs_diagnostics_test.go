// ci/jobs_diagnostics_test.go
// Positioned diagnostics for the jobs: spec (#728).
//
// Methodology: table-driven. Each case is a small spec, the substring the
// diagnostic must carry, and a MARKER token whose location the diagnostic
// must match exactly (see requireDiagAt/posOf in jobs_test.go). Asserting
// against a marker rather than hand-counted numbers proves the diagnostic
// points at the offending key or value. Every case also asserts the
// compile produced no workflow, the negative space: a diagnosed spec must
// never half-compile.
package ci

import (
	"strings"
	"testing"
)

type diagCase struct {
	name, spec, namespace, want, marker string
}

func runDiagCases(t *testing.T, cases []diagCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ns := tc.namespace
			if ns == "" {
				ns = testNamespace
			}
			res, diags := compileJobs(t, tc.spec, ns)
			if len(res.Workflow.Steps) != 0 {
				t.Fatalf("a diagnosed spec produced %d steps", len(res.Workflow.Steps))
			}
			requireDiagAt(t, tc.spec, diags, tc.want, tc.marker)
		})
	}
}

// TestJobsDiagnosticsStructure covers shape, key, and graph mistakes.
func TestJobsDiagnosticsStructure(t *testing.T) {
	runDiagCases(t, []diagCase{
		{name: "mixed jobs and checks",
			spec: "checks:\n  x:\n    task: t\njobs:\n  a:\n    steps:\n      - run: x\n",
			want: "either jobs: or checks:", marker: "jobs:"},
		{name: "unknown top-level key",
			spec: "jobz: 1\njobs:\n  a:\n    steps:\n      - run: x\n",
			want: "jobz", marker: "jobz"},
		{name: "unknown job key",
			spec: "jobs:\n  a:\n    stpes: []\n    steps:\n      - run: x\n",
			want: "stpes", marker: "stpes"},
		{name: "unknown step key",
			spec: "jobs:\n  a:\n    steps:\n      - run: x\n        rnu: y\n",
			want: "rnu", marker: "rnu"},
		{name: "unsupported job key runs-on",
			spec: "jobs:\n  a:\n    runs-on: ubuntu\n    steps:\n      - run: x\n",
			want: "`runs-on` is not supported", marker: "runs-on"},
		{name: "unsupported step key uses",
			spec: "jobs:\n  a:\n    steps:\n      - uses: actions/checkout@v4\n        run: x\n",
			want: "`uses` is not supported", marker: "uses"},
		{name: "unsupported top-level key permissions",
			spec: "permissions: read-all\njobs:\n  a:\n    steps:\n      - run: x\n",
			want: "`permissions` is not supported", marker: "permissions"},
		{name: "needs names no job",
			spec: "jobs:\n  a:\n    needs: [ghost]\n    steps:\n      - run: x\n",
			want: `needs "ghost", which is not a job`, marker: "needs"},
		{name: "two-job cycle",
			spec: "jobs:\n  a:\n    needs: [b]\n    steps:\n      - run: x\n" +
				"  b:\n    needs: [a]\n    steps:\n      - run: y\n",
			want: "needs cycle: a -> b -> a", marker: "needs: [a]"},
		{name: "self cycle",
			spec: "jobs:\n  a:\n    needs: [a]\n    steps:\n      - run: x\n",
			want: "needs cycle: a -> a", marker: "needs"},
		{name: "job with no steps key",
			spec: "jobs:\n  lonely:\n    name: n\n",
			want: "job has no steps", marker: "lonely"},
		{name: "job with an empty steps list",
			spec: "jobs:\n  a:\n    steps: []\n",
			want: "job has no steps", marker: "[]"},
		{name: "step with no run",
			spec: "jobs:\n  a:\n    steps:\n      - name: only-a-name\n",
			want: "step has no run", marker: "name: only-a-name"},
	})
}

// TestJobsDiagnosticsValues covers malformed values and bounds.
func TestJobsDiagnosticsValues(t *testing.T) {
	runDiagCases(t, []diagCase{
		{name: "timeout-minutes zero",
			spec: "jobs:\n  a:\n    timeout-minutes: 0\n    steps:\n      - run: x\n",
			want: "timeout-minutes must be between", marker: "0\n"},
		{name: "timeout-minutes over the ceiling",
			spec: "jobs:\n  a:\n    timeout-minutes: 99999\n    steps:\n      - run: x\n",
			want: "timeout-minutes must be between", marker: "99999"},
		{name: "timeout-minutes not an integer",
			spec: "jobs:\n  a:\n    timeout-minutes: soon\n    steps:\n      - run: x\n",
			want: "positive integer number of minutes", marker: "soon"},
		{name: "retries negative",
			spec: "jobs:\n  a:\n    retries: -1\n    steps:\n      - run: x\n",
			want: "must not be negative", marker: "-1"},
		{name: "retries over the engine ceiling",
			spec: "jobs:\n  a:\n    retries: 100001\n    steps:\n      - run: x\n",
			want: "must be at most", marker: "100001"},
	})
}

// TestJobsDiagnosticsExpressions covers the ${{ }} rules and secret binding.
func TestJobsDiagnosticsExpressions(t *testing.T) {
	runDiagCases(t, []diagCase{
		{name: "expression in run",
			spec: "jobs:\n  a:\n    steps:\n      - run: echo ${{ github.sha }}\n",
			want: "expressions are not supported", marker: "echo ${{"},
		{name: "partial secret in an env value",
			spec: "jobs:\n  a:\n    env:\n      T: Bearer ${{ secrets.T }}\n    steps:\n      - run: x\n",
			want: "expressions are not supported", marker: "Bearer"},
		{name: "expression in an env key",
			spec: "jobs:\n  a:\n    env:\n      ${{ secrets.K }}: v\n    steps:\n      - run: x\n",
			want: "expressions are not supported", marker: "${{ secrets.K }}: v"},
		{name: "secret bound to a different env var",
			spec: "jobs:\n  a:\n    env:\n      API_KEY: ${{ secrets.PROD_TOKEN }}\n    steps:\n      - run: x\n",
			want: "bound to an env var of the same name", marker: "${{ secrets.PROD_TOKEN }}"},
		{name: "expression in on",
			spec: "on:\n  push:\n    branches: ['${{ x }}']\njobs:\n  a:\n    steps:\n      - run: x\n",
			want: "on: expressions are not supported", marker: "push"},
		{name: "empty on mapping",
			spec: "on: {}\njobs:\n  a:\n    steps:\n      - run: x\n",
			want: "at least one event", marker: "{}"},
		{name: "empty event name in on list",
			spec: "on: ['']\njobs:\n  a:\n    steps:\n      - run: x\n",
			want: "on list entries must be event names", marker: "''"},
	})
}

// TestJobsNamespace covers the caller-supplied namespace.
func TestJobsNamespace(t *testing.T) {
	spec := "jobs:\n  a:\n    steps:\n      - run: x\n"
	for _, tc := range []struct{ ns, want string }{
		{"", "needs a task_namespace"},
		{"has.dot", "invalid task_namespace"},
		{"has space", "invalid task_namespace"},
	} {
		res, diags := CompileYAMLWith("wf", []byte(spec), CompileOptions{TaskNamespace: tc.ns})
		if len(res.Workflow.Steps) != 0 {
			t.Fatalf("namespace %q: produced steps despite the diagnostic", tc.ns)
		}
		found := false
		for _, d := range diags {
			found = found || strings.Contains(d.Message, tc.want)
		}
		if !found {
			t.Fatalf("namespace %q: no %q diagnostic in %+v", tc.ns, tc.want, diags)
		}
	}
	legacy, diags := CompileYAML("wf", []byte(spec))
	if len(legacy.Steps) != 0 || len(diags) == 0 {
		t.Fatal("CompileYAML must diagnose a jobs: spec, since it has no namespace")
	}
}

// TestJobsDiagnosticsAccumulate proves one pass reports every problem
// rather than stopping at the first.
func TestJobsDiagnosticsAccumulate(t *testing.T) {
	spec := "jobs:\n  a:\n    runs-on: x\n    needs: [ghost]\n" +
		"    steps:\n      - run: echo ${{ a }}\n"
	_, diags := compileJobs(t, spec, testNamespace)
	for _, want := range []string{"runs-on", "ghost", "expressions are not supported"} {
		found := false
		for _, d := range diags {
			found = found || strings.Contains(d.Message, want)
		}
		if !found {
			t.Fatalf("missing %q among %d diagnostics: %+v", want, len(diags), diags)
		}
	}
}
