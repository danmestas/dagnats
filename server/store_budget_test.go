// Methodology: Pure unit tests using TDD. Disk-size scenarios fake
// availableDiskBytesFn so they are deterministic and never touch the real
// filesystem; a couple of tests exercise the real statfs path against a
// scratch directory to prove the probe itself works. No NATS or external
// dependencies. No goroutines, so no timeouts needed.

package server

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// withFakeDisk points availableDiskBytesFn at a canned result for the
// duration of the test, restoring the real probe afterward.
func withFakeDisk(t *testing.T, availableBytes int64, err error) {
	t.Helper()
	orig := availableDiskBytesFn
	availableDiskBytesFn = func(string) (int64, error) {
		return availableBytes, err
	}
	t.Cleanup(func() { availableDiskBytesFn = orig })
}

func TestDeriveMaxStoreBytes_SmallDiskDerivesHalfAvailable(t *testing.T) {
	const available = 4 << 30 // 4 GiB
	withFakeDisk(t, available, nil)

	got := deriveMaxStoreBytes("/fake/data-dir")

	// Positive: half of the fake available bytes
	if got != available/2 {
		t.Errorf("deriveMaxStoreBytes() = %d, want %d", got, available/2)
	}
	// Negative: must not fall back to the unconditional cap
	if got == defaultMaxStoreBytes {
		t.Errorf("deriveMaxStoreBytes() returned the cap on a small disk")
	}
}

func TestDeriveMaxStoreBytes_HugeDiskCapsAtDefault(t *testing.T) {
	const available = 1 << 40 // 1 TiB
	withFakeDisk(t, available, nil)

	got := deriveMaxStoreBytes("/fake/data-dir")

	// Positive: capped at the compiled-in ceiling
	if got != defaultMaxStoreBytes {
		t.Errorf("deriveMaxStoreBytes() = %d, want cap %d", got, defaultMaxStoreBytes)
	}
	// Negative: must not be uncapped half-available
	if got == available/2 {
		t.Errorf("deriveMaxStoreBytes() returned half-available uncapped")
	}
}

func TestDeriveMaxStoreBytes_ProbeFailureFallsBackToCap(t *testing.T) {
	withFakeDisk(t, 0, fmt.Errorf("statfs boom"))

	got := deriveMaxStoreBytes("/fake/data-dir")

	// Positive: falls back to the compiled-in cap
	if got != defaultMaxStoreBytes {
		t.Errorf("deriveMaxStoreBytes() = %d, want cap %d", got, defaultMaxStoreBytes)
	}
	// Negative: never leaves the sentinel it was asked to resolve
	if got == 0 {
		t.Errorf("deriveMaxStoreBytes() returned 0 on probe failure")
	}
}

func TestConfigWithPath_UnsetBudgetDerivesFromDisk(t *testing.T) {
	const available = 4 << 30 // 4 GiB
	withFakeDisk(t, available, nil)

	dataDir := t.TempDir()
	t.Setenv("DAGNATS_DATA_DIR", dataDir)
	t.Setenv("DAGNATS_MAX_STORE_BYTES", "")

	cfg, _, err := ConfigWithPath("")
	if err != nil {
		t.Fatalf("ConfigWithPath() failed: %v", err)
	}

	// Positive: derived from the fake disk, not the sentinel
	if cfg.MaxStoreBytes != available/2 {
		t.Errorf("MaxStoreBytes = %d, want %d", cfg.MaxStoreBytes, available/2)
	}
	// Negative: the sentinel must not survive resolution
	if cfg.MaxStoreBytes == 0 {
		t.Errorf("MaxStoreBytes still 0 after ConfigWithPath()")
	}
}

func TestConfigWithPath_ExplicitBudgetWinsUntouched(t *testing.T) {
	const available = 1 << 20 // tiny fake disk; half would be far smaller
	withFakeDisk(t, available, nil)

	dataDir := t.TempDir()
	t.Setenv("DAGNATS_DATA_DIR", dataDir)
	t.Setenv("DAGNATS_MAX_STORE_BYTES", "1073741824") // 1 GiB

	cfg, _, err := ConfigWithPath("")
	if err != nil {
		t.Fatalf("ConfigWithPath() failed: %v", err)
	}

	// Positive: explicit value passed through untouched
	if cfg.MaxStoreBytes != 1073741824 {
		t.Errorf("MaxStoreBytes = %d, want 1073741824", cfg.MaxStoreBytes)
	}
	// Negative: not derived from the tiny fake disk
	if cfg.MaxStoreBytes == available/2 {
		t.Errorf("explicit MaxStoreBytes was overwritten by derivation")
	}
}

// TestConfigWithPath_FullDiskReturnsErrorNotPanic is the regression test
// for the full-disk edge blocker: statfs can legitimately report 0 bytes
// available (Bavail=0), which makes deriveMaxStoreBytes return 0 too.
// ConfigWithPath must report that as a returned config-load error -- host
// state an operator can act on -- never as a panic.
func TestConfigWithPath_FullDiskReturnsErrorNotPanic(t *testing.T) {
	withFakeDisk(t, 0, nil) // disk backing DataDir reports 0 bytes available

	dataDir := t.TempDir()
	t.Setenv("DAGNATS_DATA_DIR", dataDir)
	t.Setenv("DAGNATS_MAX_STORE_BYTES", "")

	_, _, err := ConfigWithPath("")

	// Positive: a returned error, not a panic
	if err == nil {
		t.Fatal("ConfigWithPath() succeeded on a full disk, want an error")
	}
	// Negative: the error must be operator-actionable -- naming the data
	// dir and the space problem -- not a bare recycled panic message.
	if !strings.Contains(err.Error(), dataDir) ||
		!strings.Contains(err.Error(), "space") {
		t.Errorf(
			"error = %q, want it to name the data dir and disk space",
			err.Error(),
		)
	}
}

// TestNew_DefaultConfigDerivesBudgetWithoutPanic is the regression test for
// the second blocker: any caller that builds a Config via DefaultConfig()
// directly (not through ConfigWithPath) and passes it to New must still
// get a resolved, non-zero MaxStoreBytes -- not a startNATS panic later.
func TestNew_DefaultConfigDerivesBudgetWithoutPanic(t *testing.T) {
	const available = 4 << 30 // 4 GiB
	withFakeDisk(t, available, nil)

	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()

	srv := New(cfg)

	// Positive: New resolved the sentinel
	if srv.cfg.MaxStoreBytes != available/2 {
		t.Errorf(
			"srv.cfg.MaxStoreBytes = %d, want %d",
			srv.cfg.MaxStoreBytes, available/2,
		)
	}
	// Negative: the sentinel must not survive construction
	if srv.cfg.MaxStoreBytes == 0 {
		t.Errorf("srv.cfg.MaxStoreBytes still 0 after New()")
	}
}

func TestApplyMaxStoreBytesEnv_RejectsNonPositive(t *testing.T) {
	cases := []string{"0", "-1"}
	for _, val := range cases {
		t.Run(val, func(t *testing.T) {
			t.Setenv("DAGNATS_MAX_STORE_BYTES", val)
			cfg := DefaultConfig()

			err := applyMaxStoreBytesEnv(&cfg)

			// Positive: rejected
			if err == nil {
				t.Fatalf("applyMaxStoreBytesEnv(%q) succeeded, want error", val)
			}
			// Negative: the sentinel is untouched by the rejected value
			if cfg.MaxStoreBytes != 0 {
				t.Errorf("MaxStoreBytes = %d, want untouched 0", cfg.MaxStoreBytes)
			}
		})
	}
}

func TestApplyConfigValue_MaxStoreBytesRejectsNonPositive(t *testing.T) {
	cases := []string{"0", "-1073741824"}
	for _, val := range cases {
		t.Run(val, func(t *testing.T) {
			cfg := DefaultConfig()

			err := applyConfigValue("max_store_bytes", val, 1, &cfg)

			// Positive: rejected
			if err == nil {
				t.Fatalf("applyConfigValue(max_store_bytes, %q) succeeded, want error", val)
			}
			// Negative: the sentinel is untouched
			if cfg.MaxStoreBytes != 0 {
				t.Errorf("MaxStoreBytes = %d, want untouched 0", cfg.MaxStoreBytes)
			}
		})
	}
}

func TestWarnIfStoreBudgetTooLarge_FiresOverThreshold(t *testing.T) {
	const available = 10 << 30 // 10 GiB
	withFakeDisk(t, available, nil)

	var buf bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(orig) })

	warnIfStoreBudgetTooLarge("/fake/data-dir", 9<<30) // 90% of available

	// Positive: a headroom warning is logged
	if !strings.Contains(buf.String(), "little headroom") {
		t.Errorf("log output = %q, want a headroom warning", buf.String())
	}
	// Negative: it must be logged at WARN, not something quieter
	if !strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("log output = %q, want level=WARN", buf.String())
	}
}

func TestWarnIfStoreBudgetTooLarge_SilentUnderThreshold(t *testing.T) {
	const available = 10 << 30 // 10 GiB
	var probedPath string
	orig := availableDiskBytesFn
	availableDiskBytesFn = func(path string) (int64, error) {
		probedPath = path
		return available, nil
	}
	t.Cleanup(func() { availableDiskBytesFn = orig })

	var buf bytes.Buffer
	origLog := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(origLog) })

	warnIfStoreBudgetTooLarge("/fake/data-dir", 5<<30) // 50% of available

	// Positive: nothing is logged
	if buf.Len() != 0 {
		t.Errorf("log output = %q, want empty", buf.String())
	}
	// Negative: the silence must be because the threshold genuinely
	// wasn't crossed, not because the disk probe was skipped entirely
	// (which would also produce an empty log, for the wrong reason).
	if probedPath != "/fake/data-dir" {
		t.Errorf("availableDiskBytesFn probed %q, want /fake/data-dir", probedPath)
	}
}

// TestWarnIfStoreBudgetTooLarge_SilentAtExactThreshold pins the threshold
// comparison's inclusive boundary: exactly storeBudgetWarnThresholdPct
// (80%) must NOT warn. Mutating the "<=" in warnIfStoreBudgetTooLarge to
// "<" flips this case to firing, so this test catches that mutation where
// TestWarnIfStoreBudgetTooLarge_FiresOverThreshold (comfortably over) and
// TestWarnIfStoreBudgetTooLarge_SilentUnderThreshold (comfortably under)
// both would not.
func TestWarnIfStoreBudgetTooLarge_SilentAtExactThreshold(t *testing.T) {
	const available = 10 << 30 // 10 GiB
	withFakeDisk(t, available, nil)

	var buf bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(orig) })

	warnIfStoreBudgetTooLarge("/fake/data-dir", 8<<30) // exactly 80% of available

	if buf.Len() != 0 {
		t.Errorf("log output = %q, want empty at exactly the threshold", buf.String())
	}
}

func TestStatfsAvailableBytes_RealDirReturnsPositive(t *testing.T) {
	got, err := statfsAvailableBytes(t.TempDir())

	// Positive: the real probe succeeds against a real directory
	if err != nil {
		t.Fatalf("statfsAvailableBytes() failed: %v", err)
	}
	// Negative: a live filesystem never reports non-positive space
	if got <= 0 {
		t.Errorf("statfsAvailableBytes() = %d, want positive", got)
	}
}

func TestNearestExistingAncestor_WalksUpToExistingDir(t *testing.T) {
	base := t.TempDir()
	nested := filepath.Join(base, "does", "not", "exist", "yet")

	got, err := nearestExistingAncestor(nested)

	// Positive: resolves to the nearest real ancestor
	if err != nil {
		t.Fatalf("nearestExistingAncestor() failed: %v", err)
	}
	if got != base {
		t.Errorf("nearestExistingAncestor() = %q, want %q", got, base)
	}
	// Negative: the returned ancestor must actually exist on disk -- not
	// just differ structurally from the nonexistent input, which the
	// equality check above could already pass for the wrong reason (e.g.
	// a bug that always returns filepath.Dir(path) once, landing on
	// "yet"'s existing parent by coincidence rather than by walking to
	// one that is real).
	if _, statErr := os.Stat(got); statErr != nil {
		t.Errorf("returned ancestor %q does not exist: %v", got, statErr)
	}
}
