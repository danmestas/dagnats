// Methodology: Pure unit tests. The AGENT_JOURNAL byte reading is faked
// through the monitor's source function and time is passed into check(),
// so the 80% threshold, transition logging (once, reminder, recovery) and
// the no-ceiling case are deterministic with no goroutines or sleeps. Log
// output is captured via a slog text handler over a buffer.

package server

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// fakeJournal is the journal's stored bytes and ceiling, or a read error.
type fakeJournal struct {
	bytes, maxBytes int64
	err             error
}

func newTestJournalMonitor(t *testing.T, j *fakeJournal) (*journalMonitor, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	source := func() (int64, int64, error) { return j.bytes, j.maxBytes, j.err }
	return newJournalMonitor(source, log), &buf
}

func TestJournalMonitor_TransitionLogging(t *testing.T) {
	j := &fakeJournal{bytes: 10, maxBytes: 1000}
	m, buf := newTestJournalMonitor(t, j)
	now := time.Unix(1000, 0)

	m.check(now)
	if buf.Len() != 0 || m.Status().Filling {
		t.Fatalf("roomy journal logged or filling: %q %+v", buf.String(), m.Status())
	}

	j.bytes = 850
	m.check(now.Add(time.Minute))
	if n := strings.Count(buf.String(), "level=ERROR"); n != 1 {
		t.Fatalf("entering filling: %d errors, want 1: %q", n, buf.String())
	}
	for _, want := range []string{"stream=AGENT_JOURNAL", "bytes=850",
		"max_bytes=1000", "warn_bytes=800", "refuses new records"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("log missing %s: %q", want, buf.String())
		}
	}
	if got := m.Status(); !got.Filling || got.Bytes != 850 || got.WarnBytes != 800 {
		t.Errorf("filling status = %+v", got)
	}

	m.check(now.Add(5 * time.Minute)) // within reminder window: silent
	if n := strings.Count(buf.String(), "level=ERROR"); n != 1 {
		t.Fatalf("repeat inside window logged: %d errors", n)
	}
	m.check(now.Add(time.Minute + journalReminderInterval))
	if n := strings.Count(buf.String(), "level=ERROR"); n != 2 {
		t.Fatalf("reminder: %d errors, want 2", n)
	}

	j.bytes = 100
	m.check(now.Add(20 * time.Minute))
	if !strings.Contains(buf.String(), "level=INFO") || m.Status().Filling {
		t.Fatalf("recovery not logged: %q %+v", buf.String(), m.Status())
	}
	before := buf.Len()
	m.check(now.Add(21 * time.Minute))
	if buf.Len() != before {
		t.Errorf("roomy check after recovery logged: %q", buf.String()[before:])
	}
}

func TestJournalMonitor_ThresholdBoundary(t *testing.T) {
	j := &fakeJournal{bytes: 799, maxBytes: 1000}
	m, _ := newTestJournalMonitor(t, j)
	m.check(time.Unix(1, 0))
	if m.Status().Filling {
		t.Errorf("one byte under 80%% marked filling: %+v", m.Status())
	}
	j.bytes = 800
	m.check(time.Unix(2, 0))
	if !m.Status().Filling {
		t.Errorf("reaching 80%% not filling: %+v", m.Status())
	}
}

func TestJournalMonitor_NoCeilingNeverFills(t *testing.T) {
	j := &fakeJournal{bytes: 1 << 40, maxBytes: 0}
	m, buf := newTestJournalMonitor(t, j)
	m.check(time.Unix(1, 0))
	if m.Status().Filling || buf.Len() != 0 {
		t.Errorf("an unbounded journal cannot fill: %q %+v", buf.String(), m.Status())
	}
}

func TestJournalMonitor_ReadErrorKeepsStatus(t *testing.T) {
	j := &fakeJournal{bytes: 900, maxBytes: 1000}
	m, buf := newTestJournalMonitor(t, j)
	m.check(time.Unix(1, 0))
	j.err = errors.New("boom")
	m.check(time.Unix(2, 0))
	if !m.Status().Filling || !strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("read error should warn and keep last status: %q %+v",
			buf.String(), m.Status())
	}
}
