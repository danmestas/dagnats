package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/danmestas/dagnats/internal/api"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

const (
	// defaultDiskFreeMinBytes is the absolute floor of the low-disk
	// threshold when disk_free_min_bytes is unset (0).
	defaultDiskFreeMinBytes int64 = 10 << 30

	// diskReminderInterval bounds how often a still-low disk is re-logged.
	diskReminderInterval = 10 * time.Minute

	// diskStoreHeadroomFactor reserves room for the full store budget plus
	// the same again for compaction, snapshots and raft logs.
	diskStoreHeadroomFactor = 2
)

// diskCheckInterval is how often the data dir's filesystem is probed. A
// variable so integration tests can tick in milliseconds.
var diskCheckInterval = 60 * time.Second

// ErrJetStreamDisabled is returned by Server.Run when nats-server turned
// JetStream off on this server (it does so on a storage failure, such as
// a full disk, and never turns it back on) and the disk has since
// recovered. Only a process restart brings JetStream back, so the caller
// should exit non-zero and let its supervisor restart it (#999).
var ErrJetStreamDisabled = errors.New(
	"jetstream was disabled on this server, likely by a full disk; " +
		"restart the process to bring it back")

// shouldRestartForJetStream decides whether a server should exit so its
// supervisor restarts it: JetStream is configured but off, and the disk
// is back above the low-space threshold. While the disk is still low a
// restart would only crash-loop, so the server waits.
func shouldRestartForJetStream(jsConfigured, jsEnabled, diskOK bool) bool {
	return jsConfigured && !jsEnabled && diskOK
}

// jetStreamWatch is the monitor's view of local JetStream: whether it is
// configured, a probe for whether it is still on, and the callback that
// asks the server to exit.
type jetStreamWatch struct {
	configured bool
	enabled    func() bool
	onLost     func()
	disabled   metric.Int64Counter
}

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
	interval       time.Duration
	jetStream      *jetStreamWatch

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
		interval:       diskCheckInterval,
	}
}

// watchJetStream makes each successful probe also check local JetStream;
// see checkJetStream. Call before run.
func (m *diskMonitor) watchJetStream(configured bool, enabled func() bool, onLost func()) {
	if enabled == nil || onLost == nil {
		panic("diskMonitor.watchJetStream: enabled and onLost must not be nil")
	}
	if m.jetStream != nil {
		panic("diskMonitor.watchJetStream: already watching")
	}
	disabled, err := otel.Meter("dagnats/server").Int64Counter(
		"server.jetstream_disabled_checks",
		metric.WithDescription("Disk checks that found local JetStream "+
			"configured but disabled"))
	if err != nil {
		// Losing the counter must not stop the restart path.
		m.log.Warn("jetstream_disabled_checks counter not registered", "error", err)
		disabled = noop.Int64Counter{}
	}
	m.jetStream = &jetStreamWatch{
		configured: configured, enabled: enabled, onLost: onLost, disabled: disabled,
	}
}

// checkJetStream logs at ERROR on every check that finds JetStream
// configured but off, and asks the server to exit once the disk is OK.
// It never re-enables JetStream in process: on a clustered server a
// restart is the path nats-server supports for rejoining the meta group.
func (m *diskMonitor) checkJetStream(diskOK bool) {
	if m.jetStream == nil {
		return
	}
	watch := m.jetStream
	if watch.enabled == nil || watch.onLost == nil {
		panic("diskMonitor.checkJetStream: watch is incomplete")
	}
	enabled := watch.enabled()
	if !watch.configured || enabled {
		return
	}
	watch.disabled.Add(context.Background(), 1)
	if !shouldRestartForJetStream(watch.configured, enabled, diskOK) {
		m.log.Error("JetStream was disabled on this server, likely by a full "+
			"disk; waiting for disk space before exiting so the supervisor "+
			"restarts it", "path", m.path, "threshold_bytes", m.thresholdBytes)
		return
	}
	m.log.Error("JetStream was disabled on this server, likely by a full "+
		"disk; exiting so the supervisor restarts it",
		"path", m.path, "threshold_bytes", m.thresholdBytes)
	watch.onLost()
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
	m.checkJetStream(!low)

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

// run checks every m.interval until ctx ends; the caller does the
// startup check so the first health report already has a reading.
func (m *diskMonitor) run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
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
	opts, ok := liveOptions.Load(s.ns)
	if !ok {
		panic("startDiskMonitor: embedded server options not remembered")
	}
	monitor.watchJetStream(opts.(*natsserver.Options).JetStream,
		s.ns.JetStreamEnabled, s.signalJetStreamLost)
	monitor.check(time.Now())
	s.clusterReporter.SetDiskStatus(monitor.Status)
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	// Stop waits for the goroutine so no check runs after shutdown: a
	// late check could ask a stopped server to exit, or read a disk probe
	// a test has already swapped back.
	s.diskMonitorStop = func() {
		stop()
		<-done
	}
	go func() {
		defer close(done)
		monitor.run(ctx)
	}()
}
