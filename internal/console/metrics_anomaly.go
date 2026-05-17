// internal/console/metrics_anomaly.go
// Anomaly detection over latency histograms. The dashboard renders
// muted-rust open circles at points where the tail latency exceeds
// a configured multiple of the median. Pure functions — no state.
//
// Threshold rationale: p99 > 3 * p50 is a coarse but widely-used
// "tail event" heuristic. A workflow running healthily with p50 in
// the single-digit milliseconds and p99 in the same band trips this
// only when a real outlier lands. The threshold lives in a named
// constant so the Norman audit's "what counts as anomalous?" gap is
// closed visibly — the dashboard renders the same number in its
// inline <details> glossary.
package console

import "math"

// AnomalyP99OverP50Ratio is the dashboard's anomaly threshold. A
// histogram point whose p99 exceeds AnomalyP99OverP50Ratio * p50 is
// flagged as anomalous. 3x mirrors common SRE practice (tail-latency
// distance from median); easy to reason about; easy to test.
const AnomalyP99OverP50Ratio = 3.0

// AnomalyMinP50Ms guards against false positives on histograms whose
// p50 is essentially zero. A histogram with p50=0.01ms and p99=1ms
// is mathematically a 100x ratio but operationally a non-event. The
// floor: we only mark anomalies once p50 crosses 1ms.
const AnomalyMinP50Ms = 1.0

// AnomalyMarker is one (timestamp, value, reason, window) tuple the
// dashboard renders. Reason carries the human-readable explanation
// the tooltip shows on hover; the µPlot point + paper-rust dot fills
// the visual signifier. WindowStartSecs / WindowEndSecs bracket the
// time slice the marker covers — the click handler turns these into
// /console/runs?since=…&until=… so an operator can land on the runs
// that overlap the anomaly without retyping a filter.
type AnomalyMarker struct {
	TimestampSecs   float64
	ValueMs         float64
	Reason          string
	WindowStartSecs float64
	WindowEndSecs   float64
}

// AnomalyWindowHalfSecs is the half-width the click handler uses when
// translating an anomaly marker into a runs filter window. 90 seconds
// either side captures the runs that completed inside or adjacent to
// the anomalous bucket without dragging in unrelated activity. Kept
// as a named constant so the test, the renderer, and the JS client
// all agree.
const AnomalyWindowHalfSecs = 90

// DetectAnomalies walks a histogram series and emits one marker per
// point whose p99 / p50 ratio exceeds AnomalyP99OverP50Ratio. Bounded
// loop on len(points). Points without buckets are skipped (no shape
// to evaluate).
func DetectAnomalies(points []MetricPoint) []AnomalyMarker {
	if points == nil {
		return nil
	}
	out := make([]AnomalyMarker, 0, len(points))
	const maxIter = 4096
	for i := 0; i < len(points) && i < maxIter; i++ {
		p := points[i]
		if p.Count == 0 || len(p.Buckets) == 0 {
			continue
		}
		p50 := percentileFromBuckets(p, 0.50)
		p99 := percentileFromBuckets(p, 0.99)
		if !isAnomalous(p50, p99) {
			continue
		}
		ts := float64(p.Timestamp.Unix())
		out = append(out, AnomalyMarker{
			TimestampSecs: ts,
			ValueMs:       p99,
			Reason: "p99 latency was " +
				formatRatio(p99/p50) + "× p50 — click to inspect runs",
			WindowStartSecs: ts - AnomalyWindowHalfSecs,
			WindowEndSecs:   ts + AnomalyWindowHalfSecs,
		})
	}
	return out
}

// isAnomalous applies the threshold + the p50-floor guard. Centralised
// so the dashboard renderer and the test suite use the same predicate.
func isAnomalous(p50, p99 float64) bool {
	if math.IsNaN(p50) || math.IsNaN(p99) {
		return false
	}
	if p50 < AnomalyMinP50Ms {
		return false
	}
	if p99 <= 0 {
		return false
	}
	ratio := p99 / p50
	return ratio > AnomalyP99OverP50Ratio
}

// formatRatio renders a multiplier like "3.2" or "11" for tooltip
// display. One decimal for small ratios; whole-number for large; the
// "x" suffix is appended by the caller for layout flexibility.
func formatRatio(r float64) string {
	if math.IsNaN(r) || math.IsInf(r, 0) {
		return "?"
	}
	if r >= 10 {
		return formatFloat(r, 0)
	}
	return formatFloat(r, 1)
}

// formatFloat is a small helper that respects the decimal hint. We
// re-implement to avoid pulling fmt.Sprintf into this hot path; the
// signature mirrors strconv.FormatFloat.
func formatFloat(v float64, decimals int) string {
	if decimals == 0 {
		return strconvI(int64(math.Round(v)))
	}
	// Two-step: integer part + first-decimal digit.
	whole := int64(v)
	frac := int64(math.Round((v - float64(whole)) * 10))
	if frac < 0 {
		frac = -frac
	}
	return strconvI(whole) + "." + strconvI(frac)
}

// strconvI is a tiny int64-to-string for the formatter. Avoids
// pulling strconv.FormatInt for one site.
func strconvI(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := false
	if v < 0 {
		neg = true
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
