package metrics

import (
	"time"
)

// Ingest accepts one Series + Point pair and folds it into the
// aggregator. Returns ErrCapacity when MaxSeries would be exceeded for
// a fresh name; the caller logs + continues. Returns nil otherwise.
//
// Concurrency: takes the write lock, runs the fan-out under it. The
// fan-out is non-blocking (drops on full buffers) so the lock holds
// for an O(subscribers) bounded slice walk.
func (a *Aggregator) Ingest(meta Series, p Point) error {
	if a == nil {
		panic("Aggregator.Ingest: a is nil")
	}
	if meta.Name == "" {
		panic("Aggregator.Ingest: meta.Name is empty")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closedFlag {
		return nil
	}
	if p.Timestamp.IsZero() {
		p.Timestamp = a.now()
	}
	entry, fresh, err := a.lookupOrCreate(meta)
	if err != nil {
		return err
	}
	a.appendPoint(entry, p)
	a.fanOut(Update{
		Name: meta.Name, LabelsKey: LabelKey(p.Labels),
		Kind: meta.Kind, Point: p,
	})
	if fresh {
		a.logger.Debug("metrics: new series",
			"name", meta.Name, "kind", string(meta.Kind))
	}
	return nil
}

// lookupOrCreate returns the seriesEntry for the metric name, creating
// it when missing. Returns fresh=true on creation so the caller can
// emit a debug log. Returns ErrCapacity when MaxSeries is hit and the
// metric is new.
func (a *Aggregator) lookupOrCreate(
	meta Series,
) (*seriesEntry, bool, error) {
	entry, ok := a.series[meta.Name]
	if ok {
		// Update mutable metadata in case description / unit landed
		// on a later record than the first observation.
		if meta.Description != "" {
			entry.meta.Description = meta.Description
		}
		if meta.Unit != "" {
			entry.meta.Unit = meta.Unit
		}
		if meta.Service != "" {
			entry.meta.Service = meta.Service
		}
		if entry.meta.Kind == KindUnknown {
			entry.meta.Kind = meta.Kind
		}
		return entry, false, nil
	}
	if len(a.series) >= MaxSeries {
		return nil, false, ErrCapacity
	}
	entry = &seriesEntry{
		meta:   meta,
		points: make([]Point, 0, 32),
	}
	a.series[meta.Name] = entry
	return entry, true, nil
}

// appendPoint pushes p onto entry.points and prunes both by-count and
// by-age. Eviction is from the head (oldest) since points always
// arrive in monotonic-or-near order on the ingest path; out-of-order
// arrivals get inserted in place via a bounded right-scan.
func (a *Aggregator) appendPoint(entry *seriesEntry, p Point) {
	if entry == nil {
		panic("appendPoint: entry is nil")
	}
	entry.points = appendOrdered(entry.points, p)
	entry.points = pruneByCount(entry.points)
	entry.points = pruneByAge(entry.points, a.now(), Window)
}

// appendOrdered inserts p so the slice stays sorted by Timestamp. The
// fast path (p is newer than the tail) is a single append; the rare
// out-of-order path scans backward up to 32 positions before falling
// back to append-and-warn behaviour. Bounded scan keeps worst case
// cheap.
func appendOrdered(xs []Point, p Point) []Point {
	if len(xs) == 0 {
		return append(xs, p)
	}
	tail := xs[len(xs)-1]
	if !tail.Timestamp.After(p.Timestamp) {
		return append(xs, p)
	}
	const scanMax = 32
	end := len(xs) - 1
	start := end - scanMax
	if start < 0 {
		start = 0
	}
	for i := end; i >= start; i-- {
		if !xs[i].Timestamp.After(p.Timestamp) {
			xs = append(xs, Point{})
			copy(xs[i+2:], xs[i+1:])
			xs[i+1] = p
			return xs
		}
	}
	// Older than every scanned point — prepend.
	xs = append(xs, Point{})
	copy(xs[1:], xs)
	xs[0] = p
	return xs
}

// pruneByCount drops oldest points until len(points) <= max.
func pruneByCount(points []Point) []Point {
	max := PointsPerSeriesMax
	if len(points) <= max {
		return points
	}
	overflow := len(points) - max
	return points[overflow:]
}

// pruneByAge drops points whose Timestamp + window < now.
func pruneByAge(
	points []Point, now time.Time, window time.Duration,
) []Point {
	if window <= 0 {
		return points
	}
	cutoff := now.Add(-window)
	keepFrom := 0
	const scanMax = PointsPerSeriesMax
	for i := 0; i < len(points) && i < scanMax; i++ {
		if !points[i].Timestamp.Before(cutoff) {
			keepFrom = i
			break
		}
		keepFrom = i + 1
	}
	if keepFrom == 0 {
		return points
	}
	return points[keepFrom:]
}

// fanOut sends u to every subscriber whose filter matches. Filter
// empty == match all; otherwise a name prefix match. Send is
// non-blocking; full buffers drop with a slog.Warn so a wedged
// dashboard tab doesn't backpressure the ingest loop.
func (a *Aggregator) fanOut(u Update) {
	for _, s := range a.subs {
		if s.filter != "" && !matchesFilter(u.Name, s.filter) {
			continue
		}
		select {
		case s.ch <- u:
		default:
			a.logger.Warn("metrics: subscriber buffer full, dropping",
				"name", u.Name)
		}
	}
}

// matchesFilter is the prefix match the subscribe path uses. Kept
// simple: callers pass either the empty string (all metrics) or an
// exact metric name. Future wildcard support would extend this here.
func matchesFilter(name, filter string) bool {
	return name == filter
}

// Close marks the aggregator closed and tears down every subscriber
// channel. Idempotent. Safe to call from shutdown paths.
func (a *Aggregator) Close() {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closedFlag {
		return
	}
	a.closedFlag = true
	for _, s := range a.subs {
		s.once.Do(func() { close(s.ch) })
	}
	a.subs = nil
}
