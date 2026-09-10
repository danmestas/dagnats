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
	os.Unsetenv("DAGNATS_MAX_STORE_BYTES")

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
	withFakeDisk(t, available, nil)

	var buf bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(orig) })

	warnIfStoreBudgetTooLarge("/fake/data-dir", 5<<30) // 50% of available

	// Positive: nothing is logged
	if buf.Len() != 0 {
		t.Errorf("log output = %q, want empty", buf.String())
	}
	// Negative: sanity check that the threshold logic can fire at all
	if strings.Contains(buf.String(), "headroom") {
		t.Errorf("unexpected headroom warning under threshold")
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
	// Negative: must not just echo the nonexistent input back
	if got == nested {
		t.Errorf("nearestExistingAncestor() returned the nonexistent path unchanged")
	}
}
