// server/metrics_auth_log_test.go
// Methodology: capture slog output via a tee handler, run
// LogMetricsAuthStartup over a matrix of (mode, httpAddr), and
// assert the right level + auth_mode attribute appears. The test
// pins the operator-actionable WARN that fires when the listener is
// on a public interface AND auth is set to "none".
package server

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func bufLogger(t *testing.T) (*slog.Logger, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	h := slog.NewTextHandler(buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	return slog.New(h), buf
}

func TestLogMetricsAuthStartup_LoopbackAddrInfoLevel(t *testing.T) {
	logger, buf := bufLogger(t)
	cfg := MetricsAuthConfig{Mode: MetricsAuthLoopback}
	LogMetricsAuthStartup(logger, cfg, "127.0.0.1:8080")
	out := buf.String()
	if !strings.Contains(out, "level=INFO") {
		t.Errorf("expected INFO level, got: %s", out)
	}
	if !strings.Contains(out, "auth_mode=loopback") {
		t.Errorf("missing auth_mode=loopback, got: %s", out)
	}
}

func TestLogMetricsAuthStartup_NoneOnPublicWarns(t *testing.T) {
	logger, buf := bufLogger(t)
	cfg := MetricsAuthConfig{Mode: MetricsAuthNone}
	LogMetricsAuthStartup(logger, cfg, "0.0.0.0:8080")
	out := buf.String()
	if !strings.Contains(out, "level=WARN") {
		t.Errorf("expected WARN level for none-on-public, got: %s", out)
	}
	if !strings.Contains(out, "anyone on the network") {
		t.Errorf("missing safety warning text, got: %s", out)
	}
	if !strings.Contains(out, "auth_mode=none") {
		t.Errorf("missing auth_mode=none, got: %s", out)
	}
}

func TestLogMetricsAuthStartup_NoneOnLoopbackInfoLevel(t *testing.T) {
	logger, buf := bufLogger(t)
	cfg := MetricsAuthConfig{Mode: MetricsAuthNone}
	LogMetricsAuthStartup(logger, cfg, "127.0.0.1:8080")
	out := buf.String()
	// none on loopback is operator-acknowledged: stays INFO so the
	// WARN noise doesn't fire for dev / single-host deployments.
	if !strings.Contains(out, "level=INFO") {
		t.Errorf("expected INFO for none-on-loopback, got: %s", out)
	}
}

func TestLogMetricsAuthStartup_ForwardModeInfoLevel(t *testing.T) {
	logger, buf := bufLogger(t)
	cfg := MetricsAuthConfig{Mode: MetricsAuthForward}
	LogMetricsAuthStartup(logger, cfg, "0.0.0.0:8080")
	out := buf.String()
	if !strings.Contains(out, "level=INFO") {
		t.Errorf("expected INFO for forward mode, got: %s", out)
	}
	if !strings.Contains(out, "auth_mode=forward") {
		t.Errorf("missing auth_mode=forward, got: %s", out)
	}
}

func TestIsHTTPAddrLoopback_table(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1:8080": true,
		"localhost:8080": true,
		"[::1]:8080":     true,
		"0.0.0.0:8080":   false,
		":8080":          false,
		"10.0.0.1:8080":  false,
		"192.168.1.1:80": false,
	}
	for addr, want := range cases {
		got := isHTTPAddrLoopback(addr)
		if got != want {
			t.Errorf("isHTTPAddrLoopback(%q) = %v, want %v",
				addr, got, want)
		}
	}
}

func TestLogMetricsAuthStartup_NilLoggerDoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	cfg := MetricsAuthConfig{Mode: MetricsAuthLoopback}
	LogMetricsAuthStartup(nil, cfg, "127.0.0.1:8080")
}
