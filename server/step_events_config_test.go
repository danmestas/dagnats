// server/step_events_config_test.go
// Config surface for fenced step events (#801): legacy_step_events and
// loop_state_max_bytes defaults, file keys, env overrides and the
// max_payload bound. Methodology: pure config loading, no server.
package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/protocol"
)

func writeConfigFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dagnats.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestStepEventsConfig_Defaults(t *testing.T) {
	cfg := DefaultConfig()
	if !cfg.LegacyStepEvents {
		t.Fatal("legacy_step_events must default to true")
	}
	if cfg.LoopStateMaxBytes != protocol.LoopStateMaxBytesDefault {
		t.Fatalf("loop_state_max_bytes = %d, want %d",
			cfg.LoopStateMaxBytes, protocol.LoopStateMaxBytesDefault)
	}
}

func TestStepEventsConfig_FileKeysAndEnv(t *testing.T) {
	path := writeConfigFile(t, "legacy_step_events: false\nloop_state_max_bytes: 4096\n")
	cfg, _, err := ConfigWithPath(path)
	if err != nil {
		t.Fatalf("ConfigWithPath: %v", err)
	}
	if cfg.LegacyStepEvents || cfg.LoopStateMaxBytes != 4096 {
		t.Fatalf("file keys: legacy=%v cap=%d, want false and 4096",
			cfg.LegacyStepEvents, cfg.LoopStateMaxBytes)
	}
	t.Setenv("DAGNATS_LEGACY_STEP_EVENTS", "yes")
	t.Setenv("DAGNATS_LOOP_STATE_MAX_BYTES", "8192")
	cfg, _, err = ConfigWithPath(path)
	if err != nil {
		t.Fatalf("ConfigWithPath with env: %v", err)
	}
	if !cfg.LegacyStepEvents || cfg.LoopStateMaxBytes != 8192 {
		t.Fatalf("env override: legacy=%v cap=%d, want true and 8192",
			cfg.LegacyStepEvents, cfg.LoopStateMaxBytes)
	}
}

func TestStepEventsConfig_RejectsBadValues(t *testing.T) {
	cases := []struct{ name, body, want string }{
		{"bad bool", "legacy_step_events: maybe\n", "legacy_step_events"},
		{"zero cap", "loop_state_max_bytes: 0\n", "loop_state_max_bytes"},
		{"over max_payload", "max_payload: 65536\nloop_state_max_bytes: 70000\n", "exceeds max_payload"},
		{"over nats default", "loop_state_max_bytes: 2097152\n", "exceeds max_payload"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := ConfigWithPath(writeConfigFile(t, tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want one mentioning %q", err, tc.want)
			}
		})
	}
	t.Setenv("DAGNATS_LOOP_STATE_MAX_BYTES", "-5")
	if _, _, err := ConfigWithPath(writeConfigFile(t, "")); err == nil {
		t.Fatal("negative DAGNATS_LOOP_STATE_MAX_BYTES accepted")
	}
}
