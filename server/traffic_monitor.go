package server

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"

	"github.com/danmestas/dagnats/internal/api"
)

const (
	// defaultConnMsgsWarnPerSec is the per-connection inbound rate used
	// when conn_msgs_warn_per_sec is unset (0).
	defaultConnMsgsWarnPerSec = 100

	// trafficCheckInterval is how often connection counters are sampled.
	trafficCheckInterval = 60 * time.Second

	// trafficHotSamples is how many consecutive samples at or above the
	// threshold make a connection hot (~5 minutes sustained).
	trafficHotSamples = 5

	// trafficReminderInterval bounds how often a still-hot connection is
	// re-logged.
	trafficReminderInterval = 10 * time.Minute

	// trafficConnLimit bounds how many connections one sample examines.
	trafficConnLimit = 1024
)

// connSample is one client connection's cumulative counters at sample time.
type connSample struct {
	CID     uint64
	Name    string
	Addr    string
	Account string
	InMsgs  int64
}

// trafficSourceFunc reads the current per-connection counters. Tests fake
// it; production reads the embedded server directly.
type trafficSourceFunc func() ([]connSample, error)

// natsConnzSource reads open client connections (in-process ones
// included) straight from the embedded server, no HTTP.
func natsConnzSource(ns *natsserver.Server) trafficSourceFunc {
	if ns == nil {
		panic("natsConnzSource: ns is nil")
	}
	return func() ([]connSample, error) {
		connz, err := ns.Connz(&natsserver.ConnzOptions{
			Sort: natsserver.ByCid, Username: true, State: natsserver.ConnOpen,
			Limit: trafficConnLimit,
		})
		if err != nil {
			return nil, fmt.Errorf("connz: %w", err)
		}
		samples := make([]connSample, 0, len(connz.Conns))
		for _, info := range connz.Conns {
			samples = append(samples, connSample{
				CID: info.Cid, Name: info.Name, Account: info.Account,
				Addr:   fmt.Sprintf("%s:%d", info.IP, info.Port),
				InMsgs: info.InMsgs,
			})
		}
		return samples, nil
	}
}

// connTraffic is the monitor's memory of one connection.
type connTraffic struct {
	sample       connSample
	streak       int
	rate         float64
	hot          bool
	lastLoggedAt time.Time
}

// trafficMonitor flags client connections whose inbound message rate stays
// at or above a threshold for trafficHotSamples consecutive samples: a
// client stuck in a request loop looks healthy to every other check.
type trafficMonitor struct {
	source          trafficSourceFunc
	thresholdPerSec int
	log             *slog.Logger

	mu     sync.Mutex
	conns  map[uint64]*connTraffic
	lastAt time.Time
	status api.TrafficStatus
}

func newTrafficMonitor(
	source trafficSourceFunc, thresholdPerSec int, log *slog.Logger,
) *trafficMonitor {
	if source == nil {
		panic("newTrafficMonitor: source is nil")
	}
	if log == nil {
		panic("newTrafficMonitor: log is nil")
	}
	if thresholdPerSec < 0 {
		panic(fmt.Sprintf("newTrafficMonitor: negative threshold %d", thresholdPerSec))
	}
	if thresholdPerSec == 0 {
		thresholdPerSec = defaultConnMsgsWarnPerSec
	}
	return &trafficMonitor{
		source: source, thresholdPerSec: thresholdPerSec, log: log,
		conns:  map[uint64]*connTraffic{},
		status: api.TrafficStatus{ThresholdMsgsPerSec: thresholdPerSec},
	}
}

// Status returns the most recent verdict.
func (m *trafficMonitor) Status() api.TrafficStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	status := m.status
	status.Hot = append([]api.HotConnection(nil), m.status.Hot...)
	return status
}

// check samples once and logs on transitions: ERROR on entering hot and at
// most every trafficReminderInterval while it persists, INFO on recovery
// or when a hot connection closes. A failed read keeps the previous status.
func (m *trafficMonitor) check(now time.Time) {
	samples, err := m.source()
	if err != nil {
		m.log.Warn("connection traffic check failed", "error", err)
		return
	}
	if len(samples) > trafficConnLimit {
		panic(fmt.Sprintf("trafficMonitor.check: %d samples exceed limit", len(samples)))
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	elapsedSec := now.Sub(m.lastAt).Seconds()
	next := make(map[uint64]*connTraffic, len(samples))
	for _, sample := range samples {
		next[sample.CID] = m.advance(sample, elapsedSec, now)
	}
	for cid, gone := range m.conns {
		if _, still := next[cid]; !still && gone.hot {
			m.log.Info("connection traffic recovered: connection closed",
				"cid", cid, "name", gone.sample.Name)
		}
	}
	m.conns, m.lastAt = next, now
	m.status.Hot = hotConnections(next)
}

// advance folds one sample into the connection's state and logs its
// transition. The first sight of a connection only records a baseline.
func (m *trafficMonitor) advance(
	sample connSample, elapsedSec float64, now time.Time,
) *connTraffic {
	state := m.conns[sample.CID]
	if state == nil || m.lastAt.IsZero() || elapsedSec <= 0 ||
		sample.InMsgs < state.sample.InMsgs {
		return &connTraffic{sample: sample}
	}
	state.rate = float64(sample.InMsgs-state.sample.InMsgs) / elapsedSec
	state.sample = sample
	if state.rate >= float64(m.thresholdPerSec) {
		state.streak++
	} else {
		state.streak = 0
	}
	hot := state.streak >= trafficHotSamples
	attrs := []any{"cid", sample.CID, "name", sample.Name, "addr", sample.Addr,
		"account", sample.Account, "msgs_per_sec", state.rate,
		"threshold_msgs_per_sec", m.thresholdPerSec}
	switch {
	case hot && (!state.hot || now.Sub(state.lastLoggedAt) >= trafficReminderInterval):
		state.lastLoggedAt = now
		m.log.Error("connection sustaining a runaway message rate: a client "+
			"may be stuck in a loop", attrs...)
	case !hot && state.hot:
		m.log.Info("connection traffic recovered", attrs...)
	}
	state.hot = hot
	return state
}

// hotConnections lists hot connections ordered by CID for stable output.
func hotConnections(conns map[uint64]*connTraffic) []api.HotConnection {
	var hot []api.HotConnection
	for cid, state := range conns {
		if state.hot {
			hot = append(hot, api.HotConnection{
				CID: cid, Name: state.sample.Name, MsgsPerSec: state.rate,
			})
		}
	}
	sort.Slice(hot, func(i, j int) bool { return hot[i].CID < hot[j].CID })
	return hot
}

// run samples every trafficCheckInterval until ctx ends.
func (m *trafficMonitor) run(ctx context.Context) {
	ticker := time.NewTicker(trafficCheckInterval)
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

// startTrafficMonitor runs the monitor for the server's lifetime and wires
// its status into the cluster health report.
func (s *Server) startTrafficMonitor() {
	if s.clusterReporter == nil {
		panic("startTrafficMonitor: cluster reporter not built")
	}
	if s.ns == nil {
		panic("startTrafficMonitor: embedded server not started")
	}
	monitor := newTrafficMonitor(
		natsConnzSource(s.ns), s.cfg.ConnMsgsWarnPerSec, slog.Default(),
	)
	monitor.check(time.Now())
	s.clusterReporter.SetTrafficStatus(monitor.Status)
	ctx, stop := context.WithCancel(context.Background())
	s.trafficMonitorStop = stop
	go monitor.run(ctx)
}
