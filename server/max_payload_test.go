// Methodology: tests for the embedded NATS max_payload knob (#735). The
// integration half boots a real embedded server and publishes a 1.5 MB
// message (the size of the FAA TFMS FADT messages that were being dropped)
// with and without the knob; the config half drives both explicit sources
// (the DAGNATS_MAX_PAYLOAD env var and the max_payload config-file key)
// through ConfigWithPath and asserts out-of-range values are refused at
// load. Positive and negative space throughout.

package server

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// fadtSizedPayload is 1.5 MB: above the 1 MiB nats-server default and
// inside the 1.29-1.51 MB range of the production messages.
const fadtSizedPayload = 1_500_000

func startMaxPayloadServer(t *testing.T, maxPayload int32) *nats.Conn {
	t.Helper()
	cfg := Config{
		DataDir:       t.TempDir(),
		NATSPort:      -1,
		MaxStoreBytes: 1 << 30,
		MaxPayload:    maxPayload,
	}
	ns, err := startNATS(cfg)
	if err != nil {
		t.Fatalf("startNATS failed: %v", err)
	}
	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func TestStartNATS_MaxPayloadAcceptsLargeMessage(t *testing.T) {
	nc := startMaxPayloadServer(t, 4<<20)

	if got := nc.MaxPayload(); got != 4<<20 {
		t.Fatalf("server INFO max_payload = %d, want %d", got, 4<<20)
	}

	sub, err := nc.SubscribeSync("tfms.fadt")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	payload := bytes.Repeat([]byte("x"), fadtSizedPayload)
	if err := nc.Publish("tfms.fadt", payload); err != nil {
		t.Fatalf("publish 1.5 MB with 4 MiB max_payload: %v", err)
	}
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("receive 1.5 MB message: %v", err)
	}
	if len(msg.Data) != fadtSizedPayload {
		t.Fatalf("received %d bytes, want %d", len(msg.Data), fadtSizedPayload)
	}
}

func TestStartNATS_DefaultMaxPayloadRejectsLargeMessage(t *testing.T) {
	nc := startMaxPayloadServer(t, 0)

	if got := nc.MaxPayload(); got != 1<<20 {
		t.Fatalf("default server INFO max_payload = %d, want %d", got, 1<<20)
	}

	payload := bytes.Repeat([]byte("x"), fadtSizedPayload)
	err := nc.Publish("tfms.fadt", payload)
	if !errors.Is(err, nats.ErrMaxPayload) {
		t.Fatalf("publish 1.5 MB at default max_payload: err = %v, want %v",
			err, nats.ErrMaxPayload)
	}
}

func TestStartNATS_RejectsOutOfRangeMaxPayload(t *testing.T) {
	for _, n := range []int32{-1, 9 << 20} {
		cfg := Config{
			DataDir:       t.TempDir(),
			NATSPort:      -1,
			MaxStoreBytes: 1 << 30,
			MaxPayload:    n,
		}
		ns, err := startNATS(cfg)
		if err == nil {
			ns.Shutdown()
			t.Fatalf("startNATS accepted MaxPayload %d", n)
		}
		if !strings.Contains(err.Error(), "max_payload") {
			t.Errorf("MaxPayload %d: error %q does not name max_payload", n, err)
		}
	}
}

// isolateConfigLoad points config resolution at a temp data dir and away
// from any real config file, so ConfigWithPath sees only what a test sets.
func isolateConfigLoad(t *testing.T) {
	t.Helper()
	t.Setenv("DAGNATS_DATA_DIR", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("DAGNATS_MAX_PAYLOAD", "")
	t.Chdir(t.TempDir())
}

func TestConfigWithPath_MaxPayloadDefaultIsUnset(t *testing.T) {
	isolateConfigLoad(t)
	cfg, _, err := ConfigWithPath("")
	if err != nil {
		t.Fatalf("ConfigWithPath: %v", err)
	}
	if cfg.MaxPayload != 0 {
		t.Errorf("MaxPayload = %d, want 0 (nats-server default)", cfg.MaxPayload)
	}
}

func TestConfigWithPath_MaxPayloadEnv(t *testing.T) {
	isolateConfigLoad(t)
	t.Setenv("DAGNATS_MAX_PAYLOAD", "4194304")
	cfg, _, err := ConfigWithPath("")
	if err != nil {
		t.Fatalf("ConfigWithPath: %v", err)
	}
	if cfg.MaxPayload != 4<<20 {
		t.Errorf("MaxPayload = %d, want %d", cfg.MaxPayload, 4<<20)
	}
}

func TestConfigWithPath_MaxPayloadFileKey(t *testing.T) {
	isolateConfigLoad(t)
	path := filepath.Join(t.TempDir(), "dagnats.yaml")
	if err := os.WriteFile(path, []byte("max_payload: 4194304\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, _, err := ConfigWithPath(path)
	if err != nil {
		t.Fatalf("ConfigWithPath: %v", err)
	}
	if cfg.MaxPayload != 4<<20 {
		t.Errorf("MaxPayload = %d, want %d", cfg.MaxPayload, 4<<20)
	}
}

func TestConfigWithPath_MaxPayloadEnvOverridesFile(t *testing.T) {
	isolateConfigLoad(t)
	path := filepath.Join(t.TempDir(), "dagnats.yaml")
	if err := os.WriteFile(path, []byte("max_payload: 2097152\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("DAGNATS_MAX_PAYLOAD", "4194304")
	cfg, _, err := ConfigWithPath(path)
	if err != nil {
		t.Fatalf("ConfigWithPath: %v", err)
	}
	if cfg.MaxPayload != 4<<20 {
		t.Errorf("MaxPayload = %d, want env value %d", cfg.MaxPayload, 4<<20)
	}
}

func TestConfigWithPath_MaxPayloadRejectsBadValues(t *testing.T) {
	bad := []string{"0", "-1", "9437184", "8388609", "4MiB", "99999999999"}
	for _, val := range bad {
		t.Run("env="+val, func(t *testing.T) {
			isolateConfigLoad(t)
			t.Setenv("DAGNATS_MAX_PAYLOAD", val)
			_, _, err := ConfigWithPath("")
			if err == nil {
				t.Fatalf("DAGNATS_MAX_PAYLOAD=%s accepted", val)
			}
			if !strings.Contains(err.Error(), "DAGNATS_MAX_PAYLOAD") {
				t.Errorf("error %q does not name DAGNATS_MAX_PAYLOAD", err)
			}
		})
		t.Run("file="+val, func(t *testing.T) {
			isolateConfigLoad(t)
			path := filepath.Join(t.TempDir(), "dagnats.yaml")
			body := []byte("max_payload: " + val + "\n")
			if err := os.WriteFile(path, body, 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			_, _, err := ConfigWithPath(path)
			if err == nil {
				t.Fatalf("max_payload: %s accepted", val)
			}
			if !strings.Contains(err.Error(), "max_payload") {
				t.Errorf("error %q does not name max_payload", err)
			}
		})
	}
}

func TestConfigWithPath_MaxPayloadAcceptsCeiling(t *testing.T) {
	isolateConfigLoad(t)
	t.Setenv("DAGNATS_MAX_PAYLOAD", "8388608")
	cfg, _, err := ConfigWithPath("")
	if err != nil {
		t.Fatalf("8 MiB (the ceiling) rejected: %v", err)
	}
	if cfg.MaxPayload != 8<<20 {
		t.Errorf("MaxPayload = %d, want %d", cfg.MaxPayload, 8<<20)
	}
}
