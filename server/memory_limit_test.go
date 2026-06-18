// Methodology: unit tests for the heap-return half of issue #441. Verify
// the MaxMemoryBytes config knob (default + wiring into JetStreamMaxMemory)
// and the debug.SetMemoryLimit helper, which makes the Go runtime GC harder
// and return heap to the OS near the limit. Positive + negative space; no
// NATS server needed for the config-level assertions.

package server

import (
	"runtime/debug"
	"testing"
)

func TestDefaultConfig_MaxMemoryBytes(t *testing.T) {
	cfg := DefaultConfig()

	// Positive: default is the documented tunable ceiling.
	if cfg.MaxMemoryBytes != defaultMaxMemoryBytes {
		t.Errorf("MaxMemoryBytes = %d, want %d",
			cfg.MaxMemoryBytes, defaultMaxMemoryBytes)
	}
	// Negative: default must be a sane positive value, not zero.
	if cfg.MaxMemoryBytes <= 0 {
		t.Errorf("MaxMemoryBytes = %d, want positive default",
			cfg.MaxMemoryBytes)
	}
}

func TestConfigFromEnv_MaxMemoryBytesOverride(t *testing.T) {
	t.Setenv("DAGNATS_MAX_MEMORY_BYTES", "536870912") // 512 MiB

	cfg := ConfigFromEnv()

	if cfg.MaxMemoryBytes != 536870912 {
		t.Errorf("MaxMemoryBytes = %d, want 536870912",
			cfg.MaxMemoryBytes)
	}
}

// TestStartNATS_WiresJetStreamMaxMemory boots an embedded server and asserts
// the configured MaxMemoryBytes lands on the running JetStream config (it was
// previously unset → unbounded memory store).
func TestStartNATS_WiresJetStreamMaxMemory(t *testing.T) {
	const memLimit = int64(512 << 20)
	cfg := Config{
		DataDir:        t.TempDir(),
		HTTPAddr:       ":8080",
		NATSPort:       -1,
		MaxStoreBytes:  1 << 30,
		MaxMemoryBytes: memLimit,
	}

	ns, err := startNATS(cfg)
	if err != nil {
		t.Fatalf("startNATS failed: %v", err)
	}
	defer ns.Shutdown()

	jsCfg := ns.JetStreamConfig()
	if jsCfg == nil {
		t.Fatal("JetStreamConfig is nil")
	}
	if jsCfg.MaxMemory != memLimit {
		t.Errorf("JetStream MaxMemory = %d, want %d",
			jsCfg.MaxMemory, memLimit)
	}
	// Negative: store limit still wired independently.
	if jsCfg.MaxStore != cfg.MaxStoreBytes {
		t.Errorf("JetStream MaxStore = %d, want %d",
			jsCfg.MaxStore, cfg.MaxStoreBytes)
	}
}

// TestApplyGoMemoryLimit_SetsLimit verifies a non-zero config value is
// applied as the soft GOMEMLIMIT. Restores the prior limit so test order
// is unaffected.
func TestApplyGoMemoryLimit_SetsLimit(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	const want = int64(700 << 20)
	applyGoMemoryLimit(want, false)

	if got := debug.SetMemoryLimit(-1); got != want {
		t.Errorf("memory limit = %d, want %d", got, want)
	}
}

// TestApplyGoMemoryLimit_ZeroNoop guards the unset/zero case: never set a
// 0 limit (that would force the GC to run continuously).
func TestApplyGoMemoryLimit_ZeroNoop(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	applyGoMemoryLimit(0, false)

	if got := debug.SetMemoryLimit(-1); got != prev {
		t.Errorf("memory limit = %d, want unchanged %d", got, prev)
	}
}

// TestApplyGoMemoryLimit_EnvGuard guards the explicit-GOMEMLIMIT case: when
// the operator has already set GOMEMLIMIT, the config value must not
// override it.
func TestApplyGoMemoryLimit_EnvGuard(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	const sentinel = int64(123 << 20)
	debug.SetMemoryLimit(sentinel)

	// envSet=true means GOMEMLIMIT is present; config must not override.
	applyGoMemoryLimit(900<<20, true)

	if got := debug.SetMemoryLimit(-1); got != sentinel {
		t.Errorf("memory limit = %d, want unchanged %d (env wins)",
			got, sentinel)
	}
}
