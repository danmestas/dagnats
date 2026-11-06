// Methodology: Unit tests feed fake per-connection counters through the
// monitor's source function and pass synthetic times into check(), so the
// sustained-window, reminder and recovery transitions are deterministic
// with no sleeps. One integration test runs a real embedded server and a
// client publishing in a tight loop, then calls check() with synthetic
// times 60 s apart to prove the real Connz source reports it hot.

package server

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natstest"
)

func newTestTrafficMonitor(
	t *testing.T, samples *[]connSample, threshold int,
) (*trafficMonitor, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	source := func() ([]connSample, error) { return *samples, nil }
	return newTrafficMonitor(source, threshold, log), &buf
}

// runSamples advances the monitor one trafficCheckInterval per step with
// the given cumulative InMsgs for connection 7, returning the final time.
func runSamples(m *trafficMonitor, in *[]connSample, start time.Time, counts []int64) time.Time {
	now := start
	for _, c := range counts {
		(*in)[0].InMsgs = c
		m.check(now)
		now = now.Add(trafficCheckInterval)
	}
	return now
}

func TestTrafficMonitor_HotAfterSustainedSamples(t *testing.T) {
	conns := []connSample{{CID: 7, Name: "pump", Addr: "127.0.0.1:5000", Account: "A"}}
	m, buf := newTestTrafficMonitor(t, &conns, 100)
	// 6000 msgs per 60 s sample = 100/s, exactly the threshold.
	counts := []int64{0, 6000, 12000, 18000, 24000}
	runSamples(m, &conns, time.Unix(1000, 0), counts)
	if buf.Len() != 0 || len(m.Status().Hot) != 0 {
		t.Fatalf("hot before %d samples: %q %+v", trafficHotSamples, buf.String(), m.Status())
	}
	conns[0].InMsgs = 30000
	m.check(time.Unix(1000, 0).Add(5 * trafficCheckInterval))
	status := m.Status()
	if len(status.Hot) != 1 || status.Hot[0].CID != 7 || status.Hot[0].Name != "pump" ||
		status.Hot[0].MsgsPerSec != 100 || status.ThresholdMsgsPerSec != 100 {
		t.Fatalf("status = %+v", status)
	}
	for _, want := range []string{"level=ERROR", "cid=7", "name=pump",
		"addr=127.0.0.1:5000", "account=A", "msgs_per_sec=100", "threshold_msgs_per_sec=100"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("log missing %s: %q", want, buf.String())
		}
	}
}

func TestTrafficMonitor_BelowThresholdNeverHot(t *testing.T) {
	conns := []connSample{{CID: 7}}
	m, buf := newTestTrafficMonitor(t, &conns, 100)
	counts := []int64{0, 5940, 11880, 17820, 23760, 29700, 35640, 41580} // 99/s
	runSamples(m, &conns, time.Unix(1000, 0), counts)
	if buf.Len() != 0 || len(m.Status().Hot) != 0 {
		t.Errorf("99/s flagged: %q %+v", buf.String(), m.Status())
	}
}

func TestTrafficMonitor_DipResetsStreak(t *testing.T) {
	conns := []connSample{{CID: 7}}
	m, buf := newTestTrafficMonitor(t, &conns, 100)
	// four fast samples, one idle, four fast: never five in a row.
	counts := []int64{0, 6000, 12000, 18000, 24000, 24000, 30000, 36000, 42000, 48000}
	runSamples(m, &conns, time.Unix(1000, 0), counts)
	if buf.Len() != 0 || len(m.Status().Hot) != 0 {
		t.Errorf("interrupted streak flagged: %q %+v", buf.String(), m.Status())
	}
}

func TestTrafficMonitor_ReminderAndRecovery(t *testing.T) {
	conns := []connSample{{CID: 7}}
	m, buf := newTestTrafficMonitor(t, &conns, 100)
	start := time.Unix(1000, 0)
	counts := []int64{0, 6000, 12000, 18000, 24000, 30000} // hot at last
	now := runSamples(m, &conns, start, counts)
	if n := strings.Count(buf.String(), "level=ERROR"); n != 1 {
		t.Fatalf("entering hot: %d errors, want 1", n)
	}
	// Six more fast minutes: still inside the 10 min reminder window.
	next := int64(30000)
	for i := 0; i < 5; i++ {
		next += 6000
		conns[0].InMsgs = next
		m.check(now)
		now = now.Add(trafficCheckInterval)
	}
	if n := strings.Count(buf.String(), "level=ERROR"); n != 1 {
		t.Fatalf("repeat inside window: %d errors, want 1", n)
	}
	for i := 0; i < 6; i++ { // crosses 10 min since first log
		next += 6000
		conns[0].InMsgs = next
		m.check(now)
		now = now.Add(trafficCheckInterval)
	}
	if n := strings.Count(buf.String(), "level=ERROR"); n != 2 {
		t.Fatalf("reminder: %d errors, want 2", n)
	}
	m.check(now) // counter flat: rate 0
	if !strings.Contains(buf.String(), "level=INFO") || len(m.Status().Hot) != 0 {
		t.Errorf("recovery not logged: %q %+v", buf.String(), m.Status())
	}
}

func TestTrafficMonitor_ClosedConnectionRecovers(t *testing.T) {
	conns := []connSample{{CID: 7}}
	m, buf := newTestTrafficMonitor(t, &conns, 100)
	now := runSamples(m, &conns, time.Unix(1000, 0), []int64{0, 6000, 12000, 18000, 24000, 30000})
	if len(m.Status().Hot) != 1 {
		t.Fatalf("setup: not hot")
	}
	conns = nil
	m.check(now)
	if len(m.Status().Hot) != 0 || !strings.Contains(buf.String(), "level=INFO") {
		t.Errorf("closed conn still hot: %q %+v", buf.String(), m.Status())
	}
}

func TestTrafficMonitor_DefaultThreshold(t *testing.T) {
	conns := []connSample{}
	m, _ := newTestTrafficMonitor(t, &conns, 0)
	if got := m.Status().ThresholdMsgsPerSec; got != defaultConnMsgsWarnPerSec {
		t.Errorf("threshold = %d, want %d", got, defaultConnMsgsWarnPerSec)
	}
}

func TestTrafficMonitor_RealServerPublishLoop(t *testing.T) {
	ns, nc := natstest.StartServer(t)
	var buf bytes.Buffer
	m := newTrafficMonitor(natsConnzSource(ns), 100, slog.New(slog.NewTextHandler(&buf, nil)))
	now := time.Unix(1000, 0)
	m.check(now)
	for i := 0; i < trafficHotSamples; i++ {
		// 100k msgs against a synthetic 60 s window is well over 100/s.
		for j := 0; j < 100_000; j++ {
			if err := nc.Publish("traffic.test", []byte("x")); err != nil {
				t.Fatalf("publish: %v", err)
			}
		}
		if err := nc.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		now = now.Add(trafficCheckInterval)
		m.check(now)
	}
	status := m.Status()
	if len(status.Hot) < 1 || status.Hot[0].MsgsPerSec < 100 {
		t.Fatalf("publisher not reported hot: %+v log=%q", status, buf.String())
	}
	if !strings.Contains(buf.String(), "level=ERROR") {
		t.Errorf("no error logged: %q", buf.String())
	}
}

func TestConnMsgsWarnPerSecConfig(t *testing.T) {
	cfg := DefaultConfig()
	if err := applyConfigValue("conn_msgs_warn_per_sec", "250", 1, &cfg); err != nil ||
		cfg.ConnMsgsWarnPerSec != 250 {
		t.Fatalf("file key: err=%v val=%d", err, cfg.ConnMsgsWarnPerSec)
	}
	if err := applyConfigValue("conn_msgs_warn_per_sec", "-1", 1, &cfg); err == nil {
		t.Errorf("negative file value accepted")
	}
	t.Setenv("DAGNATS_CONN_MSGS_WARN_PER_SEC", "500")
	if err := applyConnMsgsWarnEnv(&cfg); err != nil || cfg.ConnMsgsWarnPerSec != 500 {
		t.Fatalf("env: err=%v val=%d", err, cfg.ConnMsgsWarnPerSec)
	}
	t.Setenv("DAGNATS_CONN_MSGS_WARN_PER_SEC", "-5")
	if err := applyConnMsgsWarnEnv(&cfg); err == nil {
		t.Errorf("negative env accepted")
	}
	t.Setenv("DAGNATS_CONN_MSGS_WARN_PER_SEC", "abc")
	if err := applyConnMsgsWarnEnv(&cfg); err == nil {
		t.Errorf("non-numeric env accepted")
	}
}
