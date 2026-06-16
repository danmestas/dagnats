// workflow_detail_run_button_test.go pins the Run-workflow affordance
// on the workflow-detail page header (console-wf-detail-run). It mirrors
// the inline list-button contract (#329, run_button_test.go) on the
// detail page's console-section-header. The button must:
//
//  1. Render for a workflow with no required input — a Run button wired
//     to the run-confirm hook posting to the existing run route.
//  2. Render disabled with a tooltip naming CONSOLE_READ_ONLY when the
//     console is in read-only mode.
//  3. Render disabled with a "required input" tooltip when the workflow
//     declares a required input schema.
//
// The detail header reuses the EXISTING run route + gating; no new
// engine call or handler is introduced.
//
// Methodology:
//   - Render the "content" template against a WorkflowDetailView built
//     with explicit Runnable / ReadOnly / CSRFToken fields. The detail
//     page's content template lives on the per-page clone, so we look it
//     up via set.pageTemplates["workflow-detail"].
//   - Two assertions per test minimum (positive + negative space).
package console

import (
	"context"
	"strings"
	"testing"
)

// TestBuildWorkflowDetail_Runnable pins the builder: buildWorkflowDetail
// classifies a no-input workflow as runnable and a required-input
// workflow as not runnable, mirroring the list builder's check.
func TestBuildWorkflowDetail_Runnable(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = append(fake.workflows,
		runnableWorkflow("demo-noop"),
		inputRequiredWorkflow("needs-input"),
	)
	noInput := buildWorkflowDetail(context.Background(), fake, "demo-noop")
	if !noInput.Runnable {
		t.Errorf("no-input workflow must be Runnable; got false")
	}
	required := buildWorkflowDetail(context.Background(), fake, "needs-input")
	if required.Runnable {
		t.Errorf("required-input workflow must NOT be Runnable; got true")
	}
}

// detailContentTemplate returns the per-page clone that owns the
// workflow-detail `content` overlay. renderFragment runs against it.
func detailContentTemplate(t *testing.T) *templateSet {
	t.Helper()
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	if set.pageTemplates["workflow-detail"] == nil {
		t.Fatalf("workflow-detail page template not registered")
	}
	return set
}

// TestWorkflowDetailRunButton_RendersForNoInputWorkflow pins the
// positive branch: a runnable workflow renders the Run button posting to
// the existing run route, wired to the run-confirm hook.
func TestWorkflowDetailRunButton_RendersForNoInputWorkflow(t *testing.T) {
	set := detailContentTemplate(t)
	view := WorkflowDetailView{
		Name:      "demo-noop",
		Version:   "v1",
		Runnable:  true,
		ReadOnly:  false,
		CSRFToken: "tok",
	}
	html, err := renderFragment(
		set.pageTemplates["workflow-detail"], "content", pageData{Page: view},
	)
	if err != nil {
		t.Fatalf("renderFragment content: %v", err)
	}
	if !strings.Contains(html, `data-action-confirm="run"`) {
		t.Errorf("expected Run button hook in detail header; got:\n%s", html)
	}
	if !strings.Contains(html,
		`action="/console/workflows/demo-noop/run"`) {
		t.Errorf("expected POST form for run in detail header; got:\n%s", html)
	}
}

// TestWorkflowDetailRunButton_ReadOnly pins the read-only branch: the
// detail button renders disabled with a tooltip naming the env var.
func TestWorkflowDetailRunButton_ReadOnly(t *testing.T) {
	set := detailContentTemplate(t)
	view := WorkflowDetailView{
		Name:      "demo-noop",
		Version:   "v1",
		Runnable:  true,
		ReadOnly:  true,
		CSRFToken: "tok",
	}
	html, err := renderFragment(
		set.pageTemplates["workflow-detail"], "content", pageData{Page: view},
	)
	if err != nil {
		t.Fatalf("renderFragment content: %v", err)
	}
	if strings.Contains(html, `data-action-confirm="run"`) {
		t.Errorf("read-only header must NOT carry the run confirm hook; got:\n%s", html)
	}
	if !strings.Contains(html, "CONSOLE_READ_ONLY") {
		t.Errorf("read-only tooltip must reference the env var; got:\n%s", html)
	}
}

// TestWorkflowDetailRunButton_RequiredInput pins the not-runnable
// branch: a workflow with a required input schema renders the button
// disabled with a tooltip naming the reason.
func TestWorkflowDetailRunButton_RequiredInput(t *testing.T) {
	set := detailContentTemplate(t)
	view := WorkflowDetailView{
		Name:      "needs-input",
		Version:   "v1",
		Runnable:  false,
		ReadOnly:  false,
		CSRFToken: "tok",
	}
	html, err := renderFragment(
		set.pageTemplates["workflow-detail"], "content", pageData{Page: view},
	)
	if err != nil {
		t.Fatalf("renderFragment content: %v", err)
	}
	if strings.Contains(html, `data-action-confirm="run"`) {
		t.Errorf("non-runnable header must NOT carry the run confirm hook; got:\n%s", html)
	}
	if !strings.Contains(html, `aria-disabled="true"`) {
		t.Errorf("non-runnable header must render a disabled affordance; got:\n%s", html)
	}
	if !strings.Contains(html, "required input") {
		t.Errorf("disabled tooltip must explain the reason; got:\n%s", html)
	}
}
