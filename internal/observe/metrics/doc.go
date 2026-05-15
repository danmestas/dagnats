// Package metrics provides an in-memory aggregator for the metric
// records published to the TELEMETRY JetStream stream by the existing
// observe/natsexporter pipeline. Two consumers depend on it:
//
//   - The Prometheus text exporter at /metrics (internal/observe/prom).
//   - The in-console metrics dashboard at /console/ops/metrics
//     (internal/console).
//
// Design choices:
//
//   - The aggregator is provider-agnostic. It speaks the JSON shape the
//     natsexporter publishes (observe/natsexporter.metricRecord), not
//     the OTel SDK types directly. Swapping the upstream emitter for a
//     non-OTel one is a single-adapter change.
//   - State lives in memory. A bounded ring buffer per metric retains
//     points within a fixed window (default 24h). On restart the
//     aggregator replays the TELEMETRY stream from now-window so the
//     dashboard recovers without external storage.
//   - Updates are coalesced behind a sync.RWMutex: the read path
//     (Snapshot + Series + Subscribe) is non-blocking against itself;
//     the write path runs a single goroutine off the NATS subscription.
//   - All loops are bounded; all functions stay under TigerStyle's
//     70-line limit.
//
// The exported surface is intentionally small: NewAggregator builds
// one; Start spins up the NATS consumer; Snapshot returns the latest
// gauge / counter values; Series returns the timestamped history for a
// metric; Subscribe / Unsubscribe drive live updates for the console's
// SSE handlers.
package metrics
