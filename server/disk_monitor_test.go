// Methodology: Pure unit tests. The statfs reading is faked through
// availableDiskBytesFn and time is passed into check(), so transition
// logging (once, reminder, recovery) and threshold math are deterministic
// with no goroutines or sleeps. Log output is captured via a slog text
// handler over a buffer.

package server

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

const gib = int64(1 << 30)

func TestDiskThresholdBytes(t *testing.T) {
	cases := []struct {
		name         string
		floor, store int64
		want         int64
	}{
		{"default floor wins", 0, 1 * gib, 10 * gib},
		{"twice store wins", 0, 8 * gib, 16 * gib},
		{"configured floor wins", 50 * gib, 8 * gib, 50 * gib},
		{"configured floor below default is honoured", 1 * gib, 1 * gib, 2 * gib},
	}
	for _, c := range cases {
		if got := diskThresholdBytes(c.floor, c.store); got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}

func newTestMonitor(t *testing.T, free *int64) (*diskMonitor, *bytes.Buffer) {
	t.Helper()
	orig := availableDiskBytesFn
	availableDiskBytesFn = func(string) (int64, error) { return *free, nil }
	t.Cleanup(func() { availableDiskBytesFn = orig })
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	return newDiskMonitor("/data", 0, 1*gib, log), &buf
}

func TestDiskMonitor_TransitionLogging(t *testing.T) {
	free := 100 * gib
	m, buf := newTestMonitor(t, &free)
	now := time.Unix(1000, 0)

	m.check(now)
	if buf.Len() != 0 || m.Status().Low {
		t.Fatalf("healthy disk logged or low: %q %+v", buf.String(), m.Status())
	}

	free = 3 * gib
	m.check(now.Add(time.Minute))
	if n := strings.Count(buf.String(), "level=ERROR"); n != 1 {
		t.Fatalf("entering low: %d errors, want 1: %q", n, buf.String())
	}
	for _, want := range []string{"path=/data", "free_bytes=3221225472",
		"threshold_bytes=10737418240"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("log missing %s: %q", want, buf.String())
		}
	}

	m.check(now.Add(5 * time.Minute)) // within reminder window: silent
	if n := strings.Count(buf.String(), "level=ERROR"); n != 1 {
		t.Fatalf("repeat inside window logged: %d errors", n)
	}

	m.check(now.Add(time.Minute + diskReminderInterval)) // reminder due
	if n := strings.Count(buf.String(), "level=ERROR"); n != 2 {
		t.Fatalf("reminder: %d errors, want 2", n)
	}

	free = 100 * gib
	m.check(now.Add(20 * time.Minute))
	if !strings.Contains(buf.String(), "level=INFO") || m.Status().Low {
		t.Fatalf("recovery not logged: %q %+v", buf.String(), m.Status())
	}
	before := buf.Len()
	m.check(now.Add(21 * time.Minute))
	if buf.Len() != before {
		t.Errorf("healthy check after recovery logged: %q", buf.String()[before:])
	}
}

func TestDiskMonitor_ThresholdBoundaryAndStatus(t *testing.T) {
	free := 10 * gib // exactly the threshold: not low
	m, _ := newTestMonitor(t, &free)
	m.check(time.Unix(1, 0))
	if got := m.Status(); got.Low || got.FreeBytes != 10*gib || got.ThresholdBytes != 10*gib {
		t.Errorf("at threshold: %+v", got)
	}
	free = 10*gib - 1
	m.check(time.Unix(2, 0))
	if !m.Status().Low {
		t.Errorf("one byte under threshold not low")
	}
}

func TestDiskMonitor_ProbeErrorKeepsStatus(t *testing.T) {
	free := 1 * gib
	m, buf := newTestMonitor(t, &free)
	m.check(time.Unix(1, 0))
	availableDiskBytesFn = func(string) (int64, error) { return 0, errors.New("boom") }
	m.check(time.Unix(2, 0))
	if !m.Status().Low || !strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("probe error should warn and keep last status: %q %+v",
			buf.String(), m.Status())
	}
}

func TestDiskFreeMinBytesConfig(t *testing.T) {
	cfg := DefaultConfig()
	if err := applyConfigValue("disk_free_min_bytes", "123", 1, &cfg); err != nil ||
		cfg.DiskFreeMinBytes != 123 {
		t.Fatalf("file key: err=%v val=%d", err, cfg.DiskFreeMinBytes)
	}
	if err := applyConfigValue("disk_free_min_bytes", "-1", 1, &cfg); err == nil {
		t.Errorf("negative file value accepted")
	}
	t.Setenv("DAGNATS_DISK_FREE_MIN_BYTES", "456")
	if err := applyDiskFreeMinBytesEnv(&cfg); err != nil || cfg.DiskFreeMinBytes != 456 {
		t.Fatalf("env: err=%v val=%d", err, cfg.DiskFreeMinBytes)
	}
	t.Setenv("DAGNATS_DISK_FREE_MIN_BYTES", "-5")
	if err := applyDiskFreeMinBytesEnv(&cfg); err == nil {
		t.Errorf("negative env value accepted")
	}
}
