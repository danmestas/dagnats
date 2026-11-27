// ci/jobs_concurrency_test.go
// Pins #1031's spec half: a jobs: spec's top-level
// concurrency: {group, cancel-in-progress: true} compiles to the workflow's
// singleton in cancel mode, so a newer run cancels the older one in the same
// group and a worker is handed only the newest. The group's one expression
// names the run-input path the lock is keyed on.
//
// Methodology: compile specs and read the resulting WorkflowDef.Singleton,
// or the diagnostics. Positive: the common GitHub group forms. Negative:
// every form dagnats cannot honour is a diagnostic, never a silent change in
// meaning, and a spec with no concurrency has no singleton.
package ci

import (
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dag"
)

const concurrencyJob = "jobs:\n  test:\n    steps:\n      - run: x\n"

func TestJobsConcurrencyCompilesToSingletonCancel(t *testing.T) {
	cases := []struct {
		name, group, wantKey string
	}{
		{"ref expression", "${{ github.ref }}", "github.ref"},
		{"workflow and ref", "${{ github.workflow }}-${{ github.ref }}", "github.ref"},
		{"no spaces", "${{github.ref}}", "github.ref"},
		{"literal group", "deploys", ""},
	}
	for _, c := range cases {
		spec := "concurrency:\n  group: '" + c.group + "'\n  cancel-in-progress: true\n" + concurrencyJob
		res, diags := compileJobs(t, spec, testNamespace)
		if len(diags) != 0 {
			t.Fatalf("%s: unexpected diagnostics %+v", c.name, diags)
		}
		got := res.Workflow.Singleton
		if got == nil || got.Mode != dag.SingletonModeCancel || got.Key != c.wantKey {
			t.Fatalf("%s: singleton %+v, want cancel keyed on %q", c.name, got, c.wantKey)
		}
	}
	res, diags := compileJobs(t, concurrencyJob, testNamespace)
	if len(diags) != 0 || res.Workflow.Singleton != nil {
		t.Fatalf("no concurrency: singleton %+v diags %+v, want none", res.Workflow.Singleton, diags)
	}
}

func TestJobsConcurrencyRefusesWhatItCannotHonour(t *testing.T) {
	cases := []struct {
		name, concurrency, wantInMessage string
	}{
		{"queue, not cancel", "concurrency:\n  group: ci\n  cancel-in-progress: false\n", "cancel-in-progress"},
		{"no cancel-in-progress", "concurrency:\n  group: ci\n", "cancel-in-progress"},
		{"scalar form", "concurrency: ci\n", "cancel-in-progress"},
		{"no group", "concurrency:\n  cancel-in-progress: true\n", "group"},
		{"mixed expression", "concurrency:\n  group: '${{ github.ref }}-x'\n  cancel-in-progress: true\n", "group"},
		{"secret as group", "concurrency:\n  group: '${{ secrets.TOKEN }}'\n  cancel-in-progress: true\n", "group"},
		{"unknown key", "concurrency:\n  group: ci\n  cancel-in-progress: true\n  queue: max\n", "queue"},
	}
	for _, c := range cases {
		_, diags := compileJobs(t, c.concurrency+concurrencyJob, testNamespace)
		if len(diags) == 0 {
			t.Fatalf("%s: compiled, want a diagnostic", c.name)
		}
		found := false
		for _, d := range diags {
			found = found || strings.Contains(d.Message, c.wantInMessage)
		}
		if !found {
			t.Fatalf("%s: diagnostics %+v do not mention %q", c.name, diags, c.wantInMessage)
		}
	}
}
