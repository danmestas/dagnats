package metrics

import (
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"
)

// Window is the maximum age a Point survives in the ring buffer.
// Tuned for the dashboard's 24h time-range option; the Prometheus
// exporter only ever reads the latest point so a shorter window would
// still satisfy it.
const Window = 24 * time.Hour

// PointsPerSeriesMax caps memory per metric. 24h at 1 sample/minute
// would be 1440 points; we double that to absorb metric-burst spikes
// without unbounded growth. Excess points are pruned oldest-first.
const PointsPerSeriesMax = 2880

// MaxSeries caps the total distinct (name, labels) tuples the
// aggregator will hold. The engine + console emit <50 metric names;
// 1024 leaves abundant headroom even with high-cardinality labels.
const MaxSeries = 1024

// MaxSubscribers bounds the live-update fan-out. Each subscriber buys
// one buffered channel; a wedged consumer drops events with a Warn
// rather than blocking the ingester.
const MaxSubscribers = 64

// ErrCapacity is returned by ingest paths when MaxSeries would be
// exceeded. The caller logs and continues — better to drop a new
// series than blow memory.
var ErrCapacity = errors.New("metrics aggregator: series capacity exceeded")

// seriesEntry is the per-series storage inside the aggregator. The
// LabelKey index resolves a Point's labels to a slot; identical label
// sets share one entry so the dashboard table groups them naturally.
type seriesEntry struct {
	meta   Series
	points []Point // bounded by PointsPerSeriesMax
}

// subscriber is one consumer of live ingest events. The Aggregator
// fans every successful ingestion out to every subscriber whose filter
// matches; non-blocking sends drop on a full buffer (slog.Warn).
type subscriber struct {
	ch     chan Update
	filter string // empty == match all
	once   sync.Once
}

// Update is the payload subscribers receive on each accepted ingest.
// Name is the canonical metric name; LabelsKey is the LabelKey output
// for the point; Point is the new observation. Subscribers use Name +
// LabelsKey to look the affected series up via Snapshot.
type Update struct {
	Name      string
	LabelsKey string
	Kind      Kind
	Point     Point
}

// Aggregator is the central in-memory metric store. Goroutine-safe
// behind a single RWMutex. Created via NewAggregator; pumped by a
// caller-owned goroutine that invokes Ingest for each metric record.
type Aggregator struct {
	mu         sync.RWMutex
	logger     *slog.Logger
	now        func() time.Time // injected for tests
	series     map[string]*seriesEntry
	subs       []*subscriber
	closedFlag bool
}

// NewAggregator constructs an empty Aggregator. The logger is required
// — observability stays provider-agnostic via slog. Pass slog.Default
// in production; pass a discard logger in tests.
func NewAggregator(logger *slog.Logger) *Aggregator {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Aggregator{
		logger: logger,
		now:    time.Now,
		series: make(map[string]*seriesEntry, 64),
		subs:   make([]*subscriber, 0, 8),
	}
}

// WithClock replaces the wall-clock source. Only the test suite uses
// this; production callers leave the default time.Now.
func (a *Aggregator) WithClock(now func() time.Time) {
	if a == nil {
		panic("Aggregator.WithClock: a is nil")
	}
	if now == nil {
		panic("Aggregator.WithClock: now is nil")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.now = now
}

// SeriesNames returns every metric name currently held, sorted for
// stable output. Used by /metrics to walk the registry in deterministic
// order so diff-watchers don't false-alarm on re-orderings.
func (a *Aggregator) SeriesNames() []string {
	if a == nil {
		panic("Aggregator.SeriesNames: a is nil")
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	names := make([]string, 0, len(a.series))
	for name := range a.series {
		names = append(names, name)
	}
	sortStrings(names)
	return names
}

// Snapshot returns a defensive copy of the series for one metric name.
// Returns the zero Series + false when the name is unknown — callers
// render an empty-state placeholder, not a 500.
func (a *Aggregator) Snapshot(name string) (Series, bool) {
	if a == nil {
		panic("Aggregator.Snapshot: a is nil")
	}
	if name == "" {
		panic("Aggregator.Snapshot: name is empty")
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	entry, ok := a.series[name]
	if !ok {
		return Series{}, false
	}
	out := entry.meta
	out.Points = make([]Point, len(entry.points))
	copy(out.Points, entry.points)
	return out, true
}
