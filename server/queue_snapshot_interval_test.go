// Methodology: unit tests for the DAGNATS_QUEUE_SNAPSHOT_INTERVAL config
// knob (#632). Verify the 5s default when unset, an explicit valid
// override, and that an invalid/out-of-range value is a hard config-load
// error rather than a silent clamp. Positive + negative space; no NATS
// server needed -- ParseQueueSnapshotInterval (internal/api) is pure.
package server

import (
	"os"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/api"
)

func TestDefaultConfig_QueueSnapshotIntervalDefaultsTo5s(t *testing.T) {
	cfg := DefaultConfig()

	// Positive: an unconfigured serve resolves the documented 5s default.
	if cfg.QueueSnapshotInterval != 5*time.Second {
		t.Errorf("QueueSnapshotInterval = %v, want 5s", cfg.QueueSnapshotInterval)
	}
}

func TestConfigFromEnv_QueueSnapshotIntervalOverride(t *testing.T) {
	dir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	defer func() {
		if err := os.Chdir(origDir); err != nil {
			t.Fatalf("Chdir restore: %v", err)
		}
	}()
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	t.Setenv("DAGNATS_QUEUE_SNAPSHOT_INTERVAL", "30s")

	cfg := ConfigFromEnv()

	// Positive: env override is applied.
	if cfg.QueueSnapshotInterval != 30*time.Second {
		t.Errorf("QueueSnapshotInterval = %v, want 30s", cfg.QueueSnapshotInterval)
	}
}

func TestConfigWithPath_QueueSnapshotIntervalInvalidIsError(t *testing.T) {
	t.Setenv("DAGNATS_QUEUE_SNAPSHOT_INTERVAL", "not-a-duration")
	_, _, err := ConfigWithPath("")
	// Positive: a garbage value refuses startup with a clear error.
	if err == nil {
		t.Fatal("expected error for invalid DAGNATS_QUEUE_SNAPSHOT_INTERVAL, got nil")
	}
}

func TestConfigWithPath_QueueSnapshotIntervalOutOfRangeIsError(t *testing.T) {
	t.Setenv("DAGNATS_QUEUE_SNAPSHOT_INTERVAL", "10m")
	_, _, err := ConfigWithPath("")
	// Negative: an in-range-looking duration string that exceeds the 5m
	// ceiling is still a hard error, not a silent clamp.
	if err == nil {
		t.Fatal("expected error for out-of-range DAGNATS_QUEUE_SNAPSHOT_INTERVAL, got nil")
	}
}

// sanity check that api.ParseQueueSnapshotInterval is the single source
// of truth this test file exercises indirectly through Config -- keeps
// the two test suites (internal/api's table test and this file's
// config-wiring test) from silently drifting on the default value.
func TestQueueSnapshotIntervalDefaultMatchesAPIPackage(t *testing.T) {
	want, err := api.ParseQueueSnapshotInterval("")
	if err != nil {
		t.Fatalf("api.ParseQueueSnapshotInterval(\"\"): %v", err)
	}
	if DefaultConfig().QueueSnapshotInterval != want {
		t.Errorf("DefaultConfig().QueueSnapshotInterval = %v, want %v (api default)",
			DefaultConfig().QueueSnapshotInterval, want)
	}
}
