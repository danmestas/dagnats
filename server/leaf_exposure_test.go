// server/leaf_exposure_test.go
// Methodology: warnLeafExposure is a pure function of Config that logs
// through slog. Each test swaps the default logger for a buffer, calls it
// with one exposed and one closed configuration, and asserts the warning
// appears for the first and not for the second.
package server

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func captureDefaultLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(orig) })
	return &buf
}

// A leaf whose client port is open to every interface without a login
// hands the hub's subjects to anyone who reaches that port.
func TestWarnLeafExposure_OpenClientPortOnLeaf(t *testing.T) {
	buf := captureDefaultLog(t)
	warnLeafExposure(Config{LeafRemotes: []string{"nats-leaf://100.64.0.9:7422"}})
	if !strings.Contains(buf.String(), "nats_host") {
		t.Fatalf("open client port on a leaf: want a warning, got %q", buf.String())
	}

	buf.Reset()
	warnLeafExposure(Config{
		LeafRemotes: []string{"nats-leaf://100.64.0.9:7422"},
		NATSHost:    "127.0.0.1",
	})
	if buf.Len() != 0 {
		t.Fatalf("loopback client port: want no warning, got %q", buf.String())
	}

	buf.Reset()
	warnLeafExposure(Config{
		LeafRemotes: []string{"nats-leaf://100.64.0.9:7422"},
		NATSUser:    "e",
	})
	if buf.Len() != 0 {
		t.Fatalf("client login set: want no warning, got %q", buf.String())
	}
}

// The leaf CONNECT password crosses the wire in cleartext, so a listener
// on every interface is worth a warning; a named private IP is not.
func TestWarnLeafExposure_ListenerOnEveryInterface(t *testing.T) {
	buf := captureDefaultLog(t)
	for _, host := range []string{"0.0.0.0", "::"} {
		buf.Reset()
		warnLeafExposure(Config{LeafPort: 7422, LeafHost: host})
		if !strings.Contains(buf.String(), "leaf_host") {
			t.Fatalf("leaf_host %s: want a warning, got %q", host, buf.String())
		}
	}

	buf.Reset()
	warnLeafExposure(Config{LeafPort: 7422, LeafHost: "100.64.0.9"})
	if buf.Len() != 0 {
		t.Fatalf("named leaf_host: want no warning, got %q", buf.String())
	}
}
