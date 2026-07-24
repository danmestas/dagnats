// pages_enrichment_log_test.go pins the observability contract for the
// secondary/enrichment DataSource reads the workflows-list and runs-list
// builders make (#580). The primary list read propagates failure via the
// returned error; the enrichment reads (triggers + runs on the workflows
// side, workflow names on the runs side) degrade gracefully — the page
// still renders — but their failure must be observable, not silently
// swallowed by a blank-identifier discard.
//
// Methodology:
//   - Narrow fakes embed deepUnimplemented so any port the builder does
//     NOT read panics (negative space: the builder touches only its
//     declared reads). Each fake exposes one error seam per enrichment
//     read so the test can force the degrade path in isolation.
//   - The logger writes to a bytes.Buffer; the test asserts the sentinel
//     error text appears (positive space, the whole point of #580) and
//     that a green enrichment read logs no warning (negative space, proof
//     the happy path is unchanged and silent).
package console

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/trigger"
)

// workflowsEnrichFake backs buildWorkflowsView: ListWorkflows is the
// primary read (always succeeds here so the test exercises the enrichment
// degrade, not the primary failure the handler already covers); the
// trigger / run reads carry independent error seams.
type workflowsEnrichFake struct {
	deepUnimplemented
	workflows       []dag.WorkflowDef
	listTriggersErr error
	listRunsErr     error
}

func (f *workflowsEnrichFake) ListWorkflows(
	context.Context,
) ([]dag.WorkflowDef, error) {
	return append([]dag.WorkflowDef{}, f.workflows...), nil
}

func (f *workflowsEnrichFake) ListTriggers(
	context.Context,
) ([]trigger.TriggerDef, error) {
	return nil, f.listTriggersErr
}

func (f *workflowsEnrichFake) ListRuns(
	context.Context, string,
) ([]dag.WorkflowRun, error) {
	return nil, f.listRunsErr
}

func (f *workflowsEnrichFake) SparklineData(
	context.Context, string, string, int,
) ([]float64, error) {
	return nil, nil
}

// runsEnrichFake backs buildRunsView: ListRuns is the primary read; the
// workflow-name lookup that feeds the filter dropdown is the enrichment
// read under test.
type runsEnrichFake struct {
	deepUnimplemented
	runs             []dag.WorkflowRun
	listWorkflowsErr error
}

func (f *runsEnrichFake) ListRuns(
	context.Context, string,
) ([]dag.WorkflowRun, error) {
	return append([]dag.WorkflowRun{}, f.runs...), nil
}

func (f *runsEnrichFake) ListWorkflows(
	context.Context,
) ([]dag.WorkflowDef, error) {
	return nil, f.listWorkflowsErr
}

func TestBuildWorkflowsView_triggerEnrichmentObservable(t *testing.T) {
	cases := []struct {
		name       string
		triggerErr error
		wantLog    bool
	}{
		{"secondary fails logs warning",
			errors.New("triggers backend down"), true},
		{"secondary ok logs nothing", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			fake := &workflowsEnrichFake{
				workflows:       []dag.WorkflowDef{{Name: "alpha", Version: "1"}},
				listTriggersErr: tc.triggerErr,
			}
			view, err := buildWorkflowsView(
				context.Background(), fake, map[string][]string{}, logger)
			if err != nil {
				t.Fatalf("primary build must succeed: %v", err)
			}
			// Positive space: enrichment failure does not blank the page.
			if len(view.Rows) != 1 || view.Rows[0].Name != "alpha" {
				t.Fatalf("primary content missing: rows=%+v", view.Rows)
			}
			logged := strings.Contains(buf.String(), "triggers backend down")
			if tc.wantLog && !logged {
				t.Errorf("trigger enrichment error not logged; log=%q",
					buf.String())
			}
			// Negative space: a green enrichment read stays silent.
			if !tc.wantLog && strings.Contains(buf.String(), "level=WARN") {
				t.Errorf("happy path logged a warning; log=%q", buf.String())
			}
		})
	}
}

func TestBuildWorkflowsView_runEnrichmentObservable(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	fake := &workflowsEnrichFake{
		workflows:   []dag.WorkflowDef{{Name: "alpha", Version: "1"}},
		listRunsErr: errors.New("runs backend down"),
	}
	view, err := buildWorkflowsView(
		context.Background(), fake, map[string][]string{}, logger)
	if err != nil {
		t.Fatalf("primary build must succeed: %v", err)
	}
	if len(view.Rows) != 1 {
		t.Fatalf("primary content missing: rows=%+v", view.Rows)
	}
	if !strings.Contains(buf.String(), "runs backend down") {
		t.Errorf("run enrichment error not logged; log=%q", buf.String())
	}
}

func TestBuildRunsView_workflowEnrichmentObservable(t *testing.T) {
	cases := []struct {
		name        string
		workflowErr error
		wantLog     bool
	}{
		{"secondary fails logs warning",
			errors.New("workflows backend down"), true},
		{"secondary ok logs nothing", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			fake := &runsEnrichFake{
				runs: []dag.WorkflowRun{{
					RunID:      "run-abc-000000000001",
					WorkflowID: "alpha",
					Status:     dag.RunStatusCompleted,
					CreatedAt:  time.Now(),
				}},
				listWorkflowsErr: tc.workflowErr,
			}
			view, err := buildRunsView(
				context.Background(), fake, map[string][]string{}, logger)
			if err != nil {
				t.Fatalf("primary build must succeed: %v", err)
			}
			// Positive space: run rows still render without workflow names.
			if len(view.Rows) != 1 || view.Rows[0].WorkflowID != "alpha" {
				t.Fatalf("primary content missing: rows=%+v", view.Rows)
			}
			logged := strings.Contains(buf.String(), "workflows backend down")
			if tc.wantLog && !logged {
				t.Errorf("workflow enrichment error not logged; log=%q",
					buf.String())
			}
			if !tc.wantLog && strings.Contains(buf.String(), "level=WARN") {
				t.Errorf("happy path logged a warning; log=%q", buf.String())
			}
		})
	}
}
