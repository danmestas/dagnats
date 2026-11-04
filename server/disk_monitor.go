package server

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/danmestas/dagnats/internal/api"
)

const (
	// defaultDiskFreeMinBytes is the absolute floor of the low-disk
	// threshold when disk_free_min_bytes is unset (0).
	defaultDiskFreeMinBytes int64 = 10 << 30

	// diskCheckInterval is how often the data dir's filesystem is probed.
	diskCheckInterval = 60 * time.Second

	// diskReminderInterval bounds how often a still-low disk is re-logged.
	diskReminderInterval = 10 * time.Minute

	// diskStoreHeadroomFactor reserves room for the full store budget plus
	// the same again for compaction, snapshots and raft logs.
	diskStoreHeadroomFactor = 2
)

// diskThresholdBytes is the free-space level below which the data dir's
// filesystem counts as low: the larger of the configured floor (default
// 10 GiB) and twice the store budget. A full disk wedges JetStream until
// restart with no hint in its own errors, so the warning must come first.
func diskThresholdBytes(floorBytes, maxStoreBytes int64) int64 {
	if floorBytes < 0 || maxStoreBytes < 0 {
		panic(fmt.Sprintf("diskThresholdBytes: negative input floor=%d store=%d",
			floorBytes, maxStoreBytes))
	}
	if floorBytes == 0 {
		floorBytes = defaultDiskFreeMinBytes
	}
	return max(floorBytes, diskStoreHeadroomFactor*maxStoreBytes)
}

// diskMonitor watches free space on the filesystem holding the data dir.
// It reads through availableDiskBytesFn, so tests fake the disk there.
type diskMonitor struct {
	path           string
	thresholdBytes int64
	log            *slog.Logger

	mu           sync.Mutex
	status       api.DiskStatus
	low          bool
	lastLoggedAt time.Time
}

func newDiskMonitor(
	path string, floorBytes, maxStoreBytes int64, log *slog.Logger,
) *diskMonitor {
	if path == "" {
		panic("newDiskMonitor: path is empty")
	}
	if log == nil {
		panic("newDiskMonitor: log is nil")
	}
	return &diskMonitor{
		path:           path,
		thresholdBytes: diskThresholdBytes(floorBytes, maxStoreBytes),
		log:            log,
	}
}

// Status returns the most recent probe result.
func (m *diskMonitor) Status() api.DiskStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.status
}

// check probes once and logs on transitions: ERROR on entering the low
// state and at most every diskReminderInterval while it persists, INFO on
// recovery. A failed probe keeps the previous status.
func (m *diskMonitor) check(now time.Time) {
	free, err := availableDiskBytesFn(m.path)
	if err != nil {
		m.log.Warn("disk free check failed", "path", m.path, "error", err)
		return
	}
	if free < 0 {
		panic(fmt.Sprintf("diskMonitor.check: negative free bytes %d", free))
	}
	low := free < m.thresholdBytes

	m.mu.Lock()
	defer m.mu.Unlock()
	m.status = api.DiskStatus{
		FreeBytes: free, ThresholdBytes: m.thresholdBytes, Low: low,
	}
	switch {
	case low && (!m.low || now.Sub(m.lastLoggedAt) >= diskReminderInterval):
		m.lastLoggedAt = now
		m.log.Error("disk space low: JetStream stops working when the "+
			"data dir's filesystem fills", "path", m.path,
			"free_bytes", free, "threshold_bytes", m.thresholdBytes)
	case !low && m.low:
		m.log.Info("disk space recovered", "path", m.path,
			"free_bytes", free, "threshold_bytes", m.thresholdBytes)
	}
	m.low = low
}

// run checks every diskCheckInterval until ctx ends; the caller does the
// startup check so the first health report already has a reading.
func (m *diskMonitor) run(ctx context.Context) {
	ticker := time.NewTicker(diskCheckInterval)
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

// startDiskMonitor runs the monitor for the server's lifetime and wires
// its status into the cluster health report.
func (s *Server) startDiskMonitor() {
	if s.clusterReporter == nil {
		panic("startDiskMonitor: cluster reporter not built")
	}
	if s.cfg.MaxStoreBytes <= 0 {
		panic("startDiskMonitor: MaxStoreBytes unresolved")
	}
	monitor := newDiskMonitor(
		s.cfg.DataDir, s.cfg.DiskFreeMinBytes, s.cfg.MaxStoreBytes,
		slog.Default(),
	)
	monitor.check(time.Now())
	s.clusterReporter.SetDiskStatus(monitor.Status)
	ctx, stop := context.WithCancel(context.Background())
	s.diskMonitorStop = stop
	go monitor.run(ctx)
}
