// cli/status_detail_test.go
// Tests for status detail helpers: formatBytes, formatCount, and stream
// details integration. Methodology: pure unit tests for formatting functions,
// integration test with embedded NATS for stream output verification.
package cli

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestFormatBytesZero(t *testing.T) {
	got := formatBytes(0)

	// Positive: zero bytes renders as "0 B".
	if got != "0 B" {
		t.Fatalf("formatBytes(0) = %q, want %q", got, "0 B")
	}
	// Negative: must not contain a decimal point.
	if strings.Contains(got, ".") {
		t.Fatal("formatBytes(0) should not contain a decimal")
	}
}

func TestFormatBytesSmall(t *testing.T) {
	got := formatBytes(512)

	// Positive: small byte values use B suffix.
	if got != "512 B" {
		t.Fatalf("formatBytes(512) = %q, want %q", got, "512 B")
	}
	// Negative: must not use KB for sub-KB values.
	if strings.Contains(got, "KB") {
		t.Fatal("formatBytes(512) should not use KB")
	}
}

func TestFormatBytesKilobytes(t *testing.T) {
	got := formatBytes(2048)

	// Positive: 2048 bytes = 2.0 KB.
	if got != "2.0 KB" {
		t.Fatalf("formatBytes(2048) = %q, want %q", got, "2.0 KB")
	}
	// Negative: must not use MB for KB-range values.
	if strings.Contains(got, "MB") {
		t.Fatal("formatBytes(2048) should not use MB")
	}
}

func TestFormatBytesMegabytes(t *testing.T) {
	got := formatBytes(2_500_000)

	// Positive: ~2.4 MB range renders with MB suffix.
	if !strings.HasSuffix(got, " MB") {
		t.Fatalf("formatBytes(2500000) = %q, want MB suffix", got)
	}
	// Negative: must not use GB.
	if strings.Contains(got, "GB") {
		t.Fatal("formatBytes(2500000) should not use GB")
	}
}

func TestFormatBytesGigabytes(t *testing.T) {
	got := formatBytes(2 * 1024 * 1024 * 1024)

	// Positive: 2 GB renders correctly.
	if got != "2.0 GB" {
		t.Fatalf("formatBytes(2GB) = %q, want %q", got, "2.0 GB")
	}
	// Negative: must not use MB for GB-range values.
	if strings.Contains(got, "MB") {
		t.Fatal("formatBytes(2GB) should not use MB")
	}
}

func TestFormatCountSmall(t *testing.T) {
	got := formatCount(42)

	// Positive: small numbers have no commas.
	if got != "42" {
		t.Fatalf("formatCount(42) = %q, want %q", got, "42")
	}
	// Negative: must not contain comma separators.
	if strings.Contains(got, ",") {
		t.Fatal("formatCount(42) should not contain commas")
	}
}

func TestFormatCountThousands(t *testing.T) {
	got := formatCount(1_204)

	// Positive: thousands get comma separator.
	if got != "1,204" {
		t.Fatalf("formatCount(1204) = %q, want %q", got, "1,204")
	}
	// Negative: must not have leading zeros in first group.
	if strings.HasPrefix(got, "0") {
		t.Fatal("formatCount should not have leading zeros")
	}
}

func TestFormatCountMillions(t *testing.T) {
	got := formatCount(1_234_567)

	// Positive: millions get two comma separators.
	if got != "1,234,567" {
		t.Fatalf(
			"formatCount(1234567) = %q, want %q",
			got, "1,234,567",
		)
	}
	// Negative: count of commas must be exactly 2.
	commas := strings.Count(got, ",")
	if commas != 2 {
		t.Fatalf("expected 2 commas, got %d", commas)
	}
}

func TestCollectStreamInfoIntegration(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	streams := collectStreamInfo(js)

	// Positive: must return known streams.
	found := false
	for _, s := range streams {
		if s.Name == "WORKFLOW_HISTORY" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected WORKFLOW_HISTORY in collected streams")
	}

	// Negative: no empty names allowed.
	for _, s := range streams {
		if s.Name == "" {
			t.Fatal("stream name must not be empty")
		}
	}
}

func TestStreamDetailsIntegration(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	output := captureOutput(func() {
		printStreamDetails(js)
	})

	// Positive: output must contain known stream names.
	if !strings.Contains(output, "WORKFLOW_HISTORY") {
		t.Fatalf(
			"expected WORKFLOW_HISTORY in output, got: %s",
			output,
		)
	}
	if !strings.Contains(output, "TASK_QUEUES") {
		t.Fatalf(
			"expected TASK_QUEUES in output, got: %s",
			output,
		)
	}

	// Negative: must not contain error markers.
	if strings.Contains(output, "(error") {
		t.Fatalf("unexpected error in output: %s", output)
	}
}

func TestGroupRunsByWorkflowGrouping(t *testing.T) {
	now := time.Now().UTC()
	earlier := now.Add(-1 * time.Hour)

	runs := []dag.WorkflowRun{
		{
			WorkflowID: "wf-a",
			Status:     dag.RunStatusCompleted,
			CreatedAt:  earlier,
		},
		{
			WorkflowID: "wf-a",
			Status:     dag.RunStatusRunning,
			CreatedAt:  now,
		},
		{
			WorkflowID: "wf-a",
			Status:     dag.RunStatusFailed,
			CreatedAt:  earlier,
		},
		{
			WorkflowID: "wf-b",
			Status:     dag.RunStatusCompleted,
			CreatedAt:  now,
		},
	}

	metrics := groupRunsByWorkflow(runs)

	// Positive: two distinct workflows should be returned.
	if len(metrics) != 2 {
		t.Fatalf("expected 2 workflows, got %d", len(metrics))
	}

	// Positive: wf-a has 3 total, should be first (sorted desc).
	if metrics[0].Name != "wf-a" {
		t.Fatalf("expected wf-a first, got %q", metrics[0].Name)
	}
	if metrics[0].Total != 3 {
		t.Fatalf("expected wf-a total=3, got %d", metrics[0].Total)
	}

	// Negative: wf-b must not have failed or running counts.
	if metrics[1].Running != 0 {
		t.Fatalf(
			"expected wf-b running=0, got %d",
			metrics[1].Running,
		)
	}
	if metrics[1].Failed != 0 {
		t.Fatalf(
			"expected wf-b failed=0, got %d",
			metrics[1].Failed,
		)
	}
}

func TestGroupRunsByWorkflowStatusCounts(t *testing.T) {
	now := time.Now().UTC()
	runs := []dag.WorkflowRun{
		{
			WorkflowID: "wf-x",
			Status:     dag.RunStatusRunning,
			CreatedAt:  now,
		},
		{
			WorkflowID: "wf-x",
			Status:     dag.RunStatusFailed,
			CreatedAt:  now,
		},
		{
			WorkflowID: "wf-x",
			Status:     dag.RunStatusCompleted,
			CreatedAt:  now,
		},
		{
			WorkflowID: "wf-x",
			Status:     dag.RunStatusCompleted,
			CreatedAt:  now,
		},
	}

	metrics := groupRunsByWorkflow(runs)

	// Positive: single workflow with correct status breakdown.
	if len(metrics) != 1 {
		t.Fatalf("expected 1 workflow, got %d", len(metrics))
	}
	m := metrics[0]
	if m.Running != 1 || m.Failed != 1 || m.Completed != 2 {
		t.Fatalf(
			"expected running=1 failed=1 completed=2, "+
				"got running=%d failed=%d completed=%d",
			m.Running, m.Failed, m.Completed,
		)
	}

	// Negative: total must equal sum of counted statuses
	// plus any uncounted (pending/cancelled).
	if m.Total != 4 {
		t.Fatalf("expected total=4, got %d", m.Total)
	}
}

func TestGroupRunsByWorkflowLastRunAt(t *testing.T) {
	older := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 4, 3, 14, 30, 0, 0, time.UTC)

	runs := []dag.WorkflowRun{
		{
			WorkflowID: "wf-t",
			Status:     dag.RunStatusCompleted,
			CreatedAt:  older,
		},
		{
			WorkflowID: "wf-t",
			Status:     dag.RunStatusRunning,
			CreatedAt:  newer,
		},
	}

	metrics := groupRunsByWorkflow(runs)

	// Positive: last_run_at reflects the most recent CreatedAt.
	if metrics[0].LastRunAt != "2026-04-03 14:30" {
		t.Fatalf(
			"expected last_run_at '2026-04-03 14:30', got %q",
			metrics[0].LastRunAt,
		)
	}
	// Negative: must not be the older timestamp.
	if strings.Contains(metrics[0].LastRunAt, "2026-01-01") {
		t.Fatal("last_run_at should not be the older timestamp")
	}
}

func TestGroupRunsByWorkflowEmpty(t *testing.T) {
	runs := []dag.WorkflowRun{}
	metrics := groupRunsByWorkflow(runs)

	// Positive: empty input yields empty output.
	if len(metrics) != 0 {
		t.Fatalf("expected 0 workflows, got %d", len(metrics))
	}
	// Negative: result must not be nil (it's an empty slice).
	if metrics == nil {
		t.Fatal("expected empty slice, not nil")
	}
}

func TestRunBreakdownNoRuns(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	nc.Close()

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	svc, svcNc := connectService()
	defer svcNc.Close()

	output := captureOutput(func() {
		printRunBreakdown(svc)
	})

	// Positive: when no runs exist, output says "none".
	if !strings.Contains(output, "none") {
		t.Fatalf("expected 'none' in output, got: %s", output)
	}
	// Negative: must not contain numeric counts.
	if strings.Contains(output, "completed") {
		t.Fatalf("should not show status counts: %s", output)
	}
}
