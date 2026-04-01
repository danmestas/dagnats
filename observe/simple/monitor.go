// observe/simple/monitor.go
// StorageMonitor watches a NATS JetStream stream's byte usage and publishes
// an advisory to "alerts.storage.{stream}" when usage exceeds a configured
// ratio. Using core NATS publish (nc.Publish) so the advisory reaches all
// subscribers regardless of whether a stream captures the alerts subject.
package simple

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

const monitoredStream = "TELEMETRY"

// StorageMonitor polls the TELEMETRY stream at a fixed interval and publishes
// an advisory when storage usage exceeds the configured warn ratio.
type StorageMonitor struct {
	nc        *nats.Conn
	js        nats.JetStreamContext
	interval  time.Duration
	warnRatio float64
}

// storageAdvisory is the JSON payload published when usage crosses the threshold.
type storageAdvisory struct {
	Stream       string  `json:"stream"`
	UsageBytes   uint64  `json:"usage_bytes"`
	MaxBytes     int64   `json:"max_bytes"`
	UsagePercent float64 `json:"usage_percent"`
	Message      string  `json:"message"`
	Timestamp    string  `json:"timestamp"`
}

// NewStorageMonitor constructs a StorageMonitor.
// Panics on nil nc — a programmer error that must surface immediately.
func NewStorageMonitor(nc *nats.Conn, interval time.Duration, warnRatio float64) *StorageMonitor {
	if nc == nil {
		panic("NewStorageMonitor: nc must not be nil")
	}
	js, err := nc.JetStream()
	if err != nil {
		panic(fmt.Sprintf("NewStorageMonitor: JetStream unavailable: %v", err))
	}
	return &StorageMonitor{
		nc:        nc,
		js:        js,
		interval:  interval,
		warnRatio: warnRatio,
	}
}

// Start runs the monitor loop, ticking at m.interval until ctx is cancelled.
// On each tick it fetches stream info, computes byte usage, and publishes an
// advisory when usage >= warnRatio. All errors are logged and never returned —
// monitoring is best-effort and must not crash the host process.
func (m *StorageMonitor) Start(ctx context.Context) {
	if ctx == nil {
		panic("StorageMonitor.Start: ctx must not be nil")
	}
	if m.js == nil {
		panic("StorageMonitor.Start: js must not be nil")
	}
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.checkAndAlert()
		}
	}
}

// checkAndAlert fetches stream info and publishes an advisory if over threshold.
func (m *StorageMonitor) checkAndAlert() {
	info, err := m.js.StreamInfo(monitoredStream)
	if err != nil {
		log.Printf("StorageMonitor: StreamInfo error stream=%s: %v",
			monitoredStream, err)
		return
	}
	if info.Config.MaxBytes <= 0 {
		return
	}
	usage := float64(info.State.Bytes) / float64(info.Config.MaxBytes)
	if usage < m.warnRatio {
		return
	}
	m.publishAdvisory(info.State.Bytes, info.Config.MaxBytes, usage)
}

// publishAdvisory serializes and publishes the storage advisory via core NATS.
func (m *StorageMonitor) publishAdvisory(usageBytes uint64, maxBytes int64, usage float64) {
	if m.nc == nil {
		panic("StorageMonitor.publishAdvisory: nc must not be nil")
	}
	if maxBytes <= 0 {
		panic("StorageMonitor.publishAdvisory: maxBytes must be positive")
	}
	advisory := storageAdvisory{
		Stream:       monitoredStream,
		UsageBytes:   usageBytes,
		MaxBytes:     maxBytes,
		UsagePercent: usage * 100,
		Message: fmt.Sprintf("stream %s at %.1f%% capacity (%d/%d bytes)",
			monitoredStream, usage*100, usageBytes, maxBytes),
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(advisory)
	if err != nil {
		log.Printf("StorageMonitor.publishAdvisory: marshal error: %v", err)
		return
	}
	subject := "alerts.storage." + monitoredStream
	if err := m.nc.Publish(subject, data); err != nil {
		log.Printf("StorageMonitor.publishAdvisory: publish error subject=%s: %v",
			subject, err)
	}
}
