package console

import (
	"context"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
)

// DataSource is the read-only surface the console needs from the
// running api.Service. Keeping it narrow lets tests substitute a fake
// without standing up NATS, and makes the surface PR-by-PR additive
// (later PRs widen it as new mutations land).
//
// Every method must be safe to call concurrently with the rest of the
// system; the underlying api.Service already meets that bar.
type DataSource interface {
	ListWorkflows(ctx context.Context) ([]dag.WorkflowDef, error)
	GetWorkflow(name string) (dag.WorkflowDef, error)
	ListRuns(ctx context.Context, workflowFilter string) ([]dag.WorkflowRun, error)
	GetRun(ctx context.Context, runID string) (dag.WorkflowRun, error)
	ListRunEvents(ctx context.Context, runID string, fullData bool) ([]api.RunEvent, error)
	ListTriggers(ctx context.Context) ([]trigger.TriggerDef, error)
}

// apiServiceAdapter wraps *api.Service to satisfy DataSource. The
// adapter exists so callers in server/server.go can pass *api.Service
// directly without code there knowing about console.DataSource.
type apiServiceAdapter struct {
	svc *api.Service
}

// NewAPIDataSource returns a DataSource backed by the live api.Service.
// Panics on nil so misconfiguration fails at startup, not at first
// request.
func NewAPIDataSource(svc *api.Service) DataSource {
	if svc == nil {
		panic("NewAPIDataSource: svc is nil")
	}
	return &apiServiceAdapter{svc: svc}
}

func (a *apiServiceAdapter) ListWorkflows(
	ctx context.Context,
) ([]dag.WorkflowDef, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.ListWorkflows: svc is nil")
	}
	if ctx == nil {
		panic("apiServiceAdapter.ListWorkflows: ctx is nil")
	}
	return a.svc.ListWorkflows(ctx)
}

func (a *apiServiceAdapter) GetWorkflow(name string) (dag.WorkflowDef, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.GetWorkflow: svc is nil")
	}
	if name == "" {
		panic("apiServiceAdapter.GetWorkflow: name is empty")
	}
	return a.svc.GetWorkflow(name)
}

func (a *apiServiceAdapter) ListRuns(
	ctx context.Context, workflowFilter string,
) ([]dag.WorkflowRun, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.ListRuns: svc is nil")
	}
	if ctx == nil {
		panic("apiServiceAdapter.ListRuns: ctx is nil")
	}
	return a.svc.ListRuns(ctx, workflowFilter)
}

func (a *apiServiceAdapter) GetRun(
	ctx context.Context, runID string,
) (dag.WorkflowRun, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.GetRun: svc is nil")
	}
	if runID == "" {
		panic("apiServiceAdapter.GetRun: runID is empty")
	}
	return a.svc.GetRun(ctx, runID)
}

func (a *apiServiceAdapter) ListRunEvents(
	ctx context.Context, runID string, fullData bool,
) ([]api.RunEvent, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.ListRunEvents: svc is nil")
	}
	if runID == "" {
		panic("apiServiceAdapter.ListRunEvents: runID is empty")
	}
	return a.svc.ListRunEvents(ctx, runID, fullData)
}

func (a *apiServiceAdapter) ListTriggers(
	ctx context.Context,
) ([]trigger.TriggerDef, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.ListTriggers: svc is nil")
	}
	if ctx == nil {
		panic("apiServiceAdapter.ListTriggers: ctx is nil")
	}
	return a.svc.ListTriggers(ctx)
}
