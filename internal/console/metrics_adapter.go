package console

import (
	"github.com/danmestas/dagnats/internal/observe/metrics"
)

// AdaptAggregator wraps a *metrics.Aggregator in a MetricsSource so
// console.Config can hold a provider-agnostic handle while the
// production wiring keeps the concrete aggregator implementation.
// Returns nil when agg is nil so callers can pass through without a
// pre-check.
func AdaptAggregator(agg *metrics.Aggregator) MetricsSource {
	if agg == nil {
		return nil
	}
	return &aggregatorAdapter{agg: agg}
}

// aggregatorAdapter is the unexported MetricsSource implementation
// backed by metrics.Aggregator. The translation layer is thin — most
// methods are name-for-name passthroughs with a defensive copy of
// nested types so callers don't accidentally mutate aggregator state.
type aggregatorAdapter struct {
	agg *metrics.Aggregator
}

// MetricNames mirrors aggregator.SeriesNames.
func (a *aggregatorAdapter) MetricNames() []string {
	if a == nil || a.agg == nil {
		return nil
	}
	return a.agg.SeriesNames()
}

// MetricSnapshot mirrors aggregator.Snapshot, translating the typed
// metrics.Series into the console-local MetricSeries.
func (a *aggregatorAdapter) MetricSnapshot(
	name string,
) (MetricSeries, bool) {
	if a == nil || a.agg == nil {
		return MetricSeries{}, false
	}
	if name == "" {
		return MetricSeries{}, false
	}
	src, ok := a.agg.Snapshot(name)
	if !ok {
		return MetricSeries{}, false
	}
	return seriesFromMetric(src), true
}

// SubscribeMetric mirrors aggregator.Subscribe, fanning each Update
// out through a translator goroutine that converts metrics.Update to
// the console-local MetricEvent shape. The translator exits when the
// upstream channel closes; cancel() unsubscribes upstream and tears
// down the translator.
func (a *aggregatorAdapter) SubscribeMetric(
	filter string,
) (<-chan MetricEvent, func()) {
	if a == nil || a.agg == nil {
		ch := make(chan MetricEvent)
		close(ch)
		return ch, func() {}
	}
	upstream, cancelUpstream := a.agg.Subscribe(filter)
	out := make(chan MetricEvent, subscriberChanBuffer)
	go translateUpdates(upstream, out)
	return out, func() {
		cancelUpstream()
	}
}

// subscriberChanBuffer is the per-subscriber buffer size in the
// adapter. Matches metrics.SubscriberBufferSize so backpressure
// behaviour is uniform; a wedged dashboard tab drops events at the
// translator stage rather than blocking the upstream aggregator.
const subscriberChanBuffer = 64

// translateUpdates copies upstream metrics.Update values into the
// console's MetricEvent shape. Closes out when upstream closes so
// callers can range over the result safely. Bounded loop count.
func translateUpdates(
	upstream <-chan metrics.Update, out chan<- MetricEvent,
) {
	if upstream == nil {
		close(out)
		return
	}
	if out == nil {
		panic("translateUpdates: out is nil")
	}
	const maxIter = 1_000_000_000
	defer close(out)
	for i := 0; i < maxIter; i++ {
		u, ok := <-upstream
		if !ok {
			return
		}
		select {
		case out <- MetricEvent{
			Name: u.Name, LabelsKey: u.LabelsKey,
			Kind: string(u.Kind), Point: pointFromMetric(u.Point),
		}:
		default:
			// Drop the event when the downstream consumer is slow.
			// The dashboard re-reads the snapshot on the next tick,
			// so dropped events don't corrupt state — they just
			// delay the visual update by one beat.
		}
	}
}

// seriesFromMetric is the pure translator from metrics.Series to
// MetricSeries. Allocates fresh slices so callers don't share the
// aggregator's backing storage.
func seriesFromMetric(src metrics.Series) MetricSeries {
	out := MetricSeries{
		Name:        src.Name,
		Kind:        string(src.Kind),
		Description: src.Description,
		Unit:        src.Unit,
		Service:     src.Service,
	}
	if len(src.Points) == 0 {
		return out
	}
	out.Points = make([]MetricPoint, len(src.Points))
	for i, p := range src.Points {
		out.Points[i] = pointFromMetric(p)
	}
	return out
}

// pointFromMetric is the pure translator from metrics.Point to
// MetricPoint. Labels are copied so caller mutations don't reach the
// aggregator.
func pointFromMetric(p metrics.Point) MetricPoint {
	out := MetricPoint{
		Timestamp: p.Timestamp,
		Value:     p.Value,
		Count:     p.Count,
		Sum:       p.Sum,
	}
	if len(p.Buckets) > 0 {
		out.Buckets = make([]MetricBucket, len(p.Buckets))
		for i, b := range p.Buckets {
			out.Buckets[i] = MetricBucket{
				UpperBound: b.UpperBound, Count: b.Count,
			}
		}
	}
	if len(p.Labels) > 0 {
		out.Labels = make(map[string]string, len(p.Labels))
		for k, v := range p.Labels {
			out.Labels[k] = v
		}
	}
	return out
}
