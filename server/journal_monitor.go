package server

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/natsutil"
)

const (
	// journalWarnPercent is how full AGENT_JOURNAL may get, as a percent
	// of its MaxBytes, before the monitor warns.
	journalWarnPercent = 80

	// journalCheckInterval is how often the journal's size is read.
	journalCheckInterval = 60 * time.Second

	// journalReminderInterval bounds how often a still-filling journal is
	// re-logged.
	journalReminderInterval = 10 * time.Minute

	// journalReadTimeout bounds one stream info round trip.
	journalReadTimeout = 5 * time.Second
)

// journalSourceFunc reads AGENT_JOURNAL's stored bytes and its MaxBytes
// ceiling (<= 0 when it has none). Tests fake it.
type journalSourceFunc func() (bytes, maxBytes int64, err error)

// natsJournalSource reads the journal stream's info over js.
func natsJournalSource(js jetstream.JetStream) journalSourceFunc {
	if js == nil {
		panic("natsJournalSource: js is nil")
	}
	return func() (int64, int64, error) {
		ctx, cancel := context.WithTimeout(context.Background(), journalReadTimeout)
		defer cancel()
		stream, err := js.Stream(ctx, natsutil.AgentJournalStream)
		if err != nil {
			return 0, 0, fmt.Errorf("stream info: %w", err)
		}
		info := stream.CachedInfo()
		return int64(info.State.Bytes), info.Config.MaxBytes, nil
	}
}

// journalMonitor warns as AGENT_JOURNAL fills. The journal never drops a
// record (no age limit, DiscardNew), so at its byte ceiling it refuses
// appends and agent steps fail; the warning comes early enough to raise
// the budget or drain it first.
type journalMonitor struct {
	source journalSourceFunc
	log    *slog.Logger

	mu           sync.Mutex
	status       api.JournalStatus
	filling      bool
	lastLoggedAt time.Time
}

func newJournalMonitor(source journalSourceFunc, log *slog.Logger) *journalMonitor {
	if source == nil {
		panic("newJournalMonitor: source is nil")
	}
	if log == nil {
		panic("newJournalMonitor: log is nil")
	}
	return &journalMonitor{source: source, log: log}
}

// Status returns the most recent reading.
func (m *journalMonitor) Status() api.JournalStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.status
}

// check reads once and logs on transitions: ERROR on reaching the warn
// level and at most every journalReminderInterval while it persists, INFO
// on recovery. A failed read keeps the previous status.
func (m *journalMonitor) check(now time.Time) {
	used, maxBytes, err := m.source()
	if err != nil {
		m.log.Warn("agent journal size check failed", "error", err)
		return
	}
	if used < 0 {
		panic(fmt.Sprintf("journalMonitor.check: negative bytes %d", used))
	}
	warnBytes := max(maxBytes, 0) * journalWarnPercent / 100
	filling := maxBytes > 0 && used >= warnBytes

	m.mu.Lock()
	defer m.mu.Unlock()
	m.status = api.JournalStatus{
		Bytes: used, MaxBytes: maxBytes, WarnBytes: warnBytes, Filling: filling,
	}
	attrs := []any{"stream", natsutil.AgentJournalStream, "bytes", used,
		"max_bytes", maxBytes, "warn_bytes", warnBytes}
	switch {
	case filling && (!m.filling || now.Sub(m.lastLoggedAt) >= journalReminderInterval):
		m.lastLoggedAt = now
		m.log.Error("agent journal filling: at 100% the journal refuses new "+
			"records and agent steps fail; raise max_store_bytes or archive "+
			"the journal", attrs...)
	case !filling && m.filling:
		m.log.Info("agent journal below its warn level", attrs...)
	}
	m.filling = filling
}

// run checks every journalCheckInterval until ctx ends; the caller does
// the startup check so the first health report already has a reading.
func (m *journalMonitor) run(ctx context.Context) {
	ticker := time.NewTicker(journalCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			m.check(now)
		}
	}
}

// startJournalMonitor runs the monitor for the server's lifetime and wires
// its status into the cluster health report.
func (s *Server) startJournalMonitor() {
	if s.clusterReporter == nil {
		panic("startJournalMonitor: cluster reporter not built")
	}
	if s.nc == nil {
		panic("startJournalMonitor: NATS connection not open")
	}
	js, err := jetstream.New(s.nc)
	if err != nil {
		slog.Warn("agent journal monitor not started", "error", err)
		return
	}
	monitor := newJournalMonitor(natsJournalSource(js), slog.Default())
	monitor.check(time.Now())
	s.clusterReporter.SetJournalStatus(monitor.Status)
	ctx, stop := context.WithCancel(context.Background())
	s.journalMonitorStop = stop
	go monitor.run(ctx)
}
