// aggregator_test.go exercises the in-memory metric aggregator.
//
// Methodology:
//   - Unit tests only — aggregator has no external dependencies. The
//     NATS pump is tested separately in nats_pump_test.go where a real
//     embedded JetStream is required.
//   - Each test allocates its own Aggregator; nothing shared.
//   - Bounded waits via time.After on all subscription assertions so a
//     wedged channel fails the test rather than wedging the suite.
//   - Minimum 2 assertions per test (positive + negative space).
package metrics

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

// silentLogger returns a /dev/null slog logger so test runs don't
// litter stdout with the aggregator's drop warnings.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fixedClock returns a clock that returns the same instant on every
// call. Used by tests that need deterministic point timestamps.
func fixedClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

func TestAggregator_IngestStoresAndSnapshots(t *testing.T) {
	a := NewAggregator(silentLogger())
	defer a.Close()
	meta := Series{Name: "runs_total", Kind: KindCounter}
	pt := Point{Value: 5, Timestamp: time.Now()}
	if err := a.Ingest(meta, pt); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	got, ok := a.Snapshot("runs_total")
	if !ok {
		t.Fatal("Snapshot reported missing series")
	}
	if got.Kind != KindCounter {
		t.Fatalf("Kind = %q, want %q", got.Kind, KindCounter)
	}
	if len(got.Points) != 1 || got.Points[0].Value != 5 {
		t.Fatalf("Points = %+v, want one point with Value=5", got.Points)
	}
}

func TestAggregator_SnapshotMissingReturnsFalse(t *testing.T) {
	a := NewAggregator(silentLogger())
	defer a.Close()
	if _, ok := a.Snapshot("nope"); ok {
		t.Fatal("Snapshot must report false for unknown name")
	}
	// Defensive: SeriesNames must be empty too.
	if got := a.SeriesNames(); len(got) != 0 {
		t.Fatalf("SeriesNames = %v, want empty", got)
	}
}

func TestAggregator_IngestPreservesMetadataOnLaterRecord(t *testing.T) {
	a := NewAggregator(silentLogger())
	defer a.Close()
	first := Series{Name: "m", Kind: KindCounter}
	if err := a.Ingest(first, Point{Value: 1, Timestamp: time.Now()}); err != nil {
		t.Fatalf("ingest 1: %v", err)
	}
	second := Series{
		Name: "m", Kind: KindCounter,
		Description: "explained", Unit: "ms",
	}
	if err := a.Ingest(second, Point{Value: 2, Timestamp: time.Now()}); err != nil {
		t.Fatalf("ingest 2: %v", err)
	}
	got, _ := a.Snapshot("m")
	if got.Description != "explained" {
		t.Fatalf("Description = %q, want %q", got.Description, "explained")
	}
	if got.Unit != "ms" {
		t.Fatalf("Unit = %q, want %q", got.Unit, "ms")
	}
}

func TestAggregator_PruneByCountKeepsMostRecent(t *testing.T) {
	a := NewAggregator(silentLogger())
	defer a.Close()
	meta := Series{Name: "rate", Kind: KindGauge}
	const n = PointsPerSeriesMax + 5
	for i := 0; i < n; i++ {
		pt := Point{
			Value:     float64(i),
			Timestamp: time.Now().Add(time.Duration(i) * time.Millisecond),
		}
		if err := a.Ingest(meta, pt); err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
	}
	got, ok := a.Snapshot("rate")
	if !ok {
		t.Fatal("snapshot missing")
	}
	if len(got.Points) != PointsPerSeriesMax {
		t.Fatalf("len Points = %d, want %d", len(got.Points), PointsPerSeriesMax)
	}
	// Latest must be the last value we ingested.
	if got.Latest().Value != float64(n-1) {
		t.Fatalf("Latest = %v, want %v", got.Latest().Value, n-1)
	}
}

func TestAggregator_PruneByAgeDropsStalePoints(t *testing.T) {
	now := time.Date(2025, 5, 15, 12, 0, 0, 0, time.UTC)
	a := NewAggregator(silentLogger())
	a.WithClock(fixedClock(now))
	defer a.Close()
	meta := Series{Name: "g", Kind: KindGauge}
	// Stale point: older than the window.
	stale := Point{
		Value:     10,
		Timestamp: now.Add(-Window - time.Hour),
	}
	if err := a.Ingest(meta, stale); err != nil {
		t.Fatalf("ingest stale: %v", err)
	}
	fresh := Point{Value: 20, Timestamp: now.Add(-time.Minute)}
	if err := a.Ingest(meta, fresh); err != nil {
		t.Fatalf("ingest fresh: %v", err)
	}
	got, _ := a.Snapshot("g")
	if len(got.Points) != 1 {
		t.Fatalf("len Points = %d, want 1 (stale pruned)", len(got.Points))
	}
	if got.Points[0].Value != 20 {
		t.Fatalf("Points[0].Value = %v, want 20", got.Points[0].Value)
	}
}

func TestAggregator_CapacityRejectsNewSeries(t *testing.T) {
	a := NewAggregator(silentLogger())
	defer a.Close()
	// Fill to capacity with distinct names.
	for i := 0; i < MaxSeries; i++ {
		meta := Series{
			Name: "m_" + intToStr(i), Kind: KindGauge,
		}
		if err := a.Ingest(meta, Point{Value: float64(i)}); err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
	}
	// MaxSeries+1 must hit ErrCapacity.
	err := a.Ingest(
		Series{Name: "overflow", Kind: KindGauge},
		Point{Value: 1},
	)
	if err == nil {
		t.Fatal("Ingest at MaxSeries+1 must return error")
	}
	if err != ErrCapacity {
		t.Fatalf("err = %v, want ErrCapacity", err)
	}
}

func TestAggregator_SeriesNamesSortedAndDistinct(t *testing.T) {
	a := NewAggregator(silentLogger())
	defer a.Close()
	for _, name := range []string{"c", "a", "b"} {
		_ = a.Ingest(Series{Name: name, Kind: KindGauge}, Point{Value: 1})
	}
	got := a.SeriesNames()
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestAggregator_SubscribeDeliversIngest(t *testing.T) {
	a := NewAggregator(silentLogger())
	defer a.Close()
	ch, cancel := a.Subscribe("")
	defer cancel()
	if a.SubscriberCount() != 1 {
		t.Fatalf("SubscriberCount = %d, want 1", a.SubscriberCount())
	}
	go func() {
		_ = a.Ingest(
			Series{Name: "rt", Kind: KindGauge},
			Point{Value: 7, Timestamp: time.Now()},
		)
	}()
	select {
	case u := <-ch:
		if u.Name != "rt" {
			t.Fatalf("Name = %q, want rt", u.Name)
		}
		if u.Point.Value != 7 {
			t.Fatalf("Value = %v, want 7", u.Point.Value)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber received nothing within 1s")
	}
}

func TestAggregator_SubscribeFilterDropsOtherMetrics(t *testing.T) {
	a := NewAggregator(silentLogger())
	defer a.Close()
	ch, cancel := a.Subscribe("only_me")
	defer cancel()
	_ = a.Ingest(Series{Name: "other", Kind: KindGauge}, Point{Value: 1})
	select {
	case <-ch:
		t.Fatal("filter must drop non-matching name")
	case <-time.After(50 * time.Millisecond):
	}
	_ = a.Ingest(Series{Name: "only_me", Kind: KindGauge}, Point{Value: 2})
	select {
	case u := <-ch:
		if u.Name != "only_me" {
			t.Fatalf("Name = %q", u.Name)
		}
		if u.Point.Value != 2 {
			t.Fatalf("Value = %v, want 2", u.Point.Value)
		}
	case <-time.After(time.Second):
		t.Fatal("filtered subscriber got nothing")
	}
}

func TestAggregator_CloseStopsFanOutAndIngest(t *testing.T) {
	a := NewAggregator(silentLogger())
	ch, _ := a.Subscribe("")
	a.Close()
	// Channel must be closed.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel must be closed after Close()")
		}
	case <-time.After(time.Second):
		t.Fatal("Close() did not close subscriber channel")
	}
	// Ingest after Close must not panic; should be a no-op.
	if err := a.Ingest(
		Series{Name: "x", Kind: KindGauge},
		Point{Value: 1},
	); err != nil {
		t.Fatalf("ingest after close: %v", err)
	}
	if got := a.SubscriberCount(); got != 0 {
		t.Fatalf("SubscriberCount after Close = %d, want 0", got)
	}
}

func TestAggregator_LabelKeyDeterministic(t *testing.T) {
	a := LabelKey(map[string]string{"b": "1", "a": "2"})
	b := LabelKey(map[string]string{"a": "2", "b": "1"})
	if a != b {
		t.Fatalf("LabelKey not deterministic: %q vs %q", a, b)
	}
	// Defensive: empty map renders empty.
	if got := LabelKey(nil); got != "" {
		t.Fatalf("LabelKey(nil) = %q, want empty", got)
	}
}

// intToStr is a small helper for the capacity test; avoids dragging
// in strconv at the top of every short test.
func intToStr(i int) string {
	if i == 0 {
		return "0"
	}
	const base = 10
	var buf [12]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%base)
		i /= base
	}
	return string(buf[pos:])
}
