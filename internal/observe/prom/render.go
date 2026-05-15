package prom

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/danmestas/dagnats/internal/observe/metrics"
)

// ContentType is the Prometheus text-exposition content-type header
// value. Version 0.0.4 is the stable text format documented at
// https://prometheus.io/docs/instrumenting/exposition_formats/.
const ContentType = "text/plain; version=0.0.4; charset=utf-8"

// Render writes the Prometheus text-format representation of every
// series held by the aggregator into w. Returns the first I/O error;
// callers map that to http.Error.
//
// Output is deterministic: metrics are walked in sorted name order so
// repeated calls produce byte-identical output when the aggregator is
// idle.
func Render(w io.Writer, agg *metrics.Aggregator) error {
	if w == nil {
		panic("prom.Render: w is nil")
	}
	if agg == nil {
		panic("prom.Render: agg is nil")
	}
	names := agg.SeriesNames()
	if len(names) == 0 {
		if _, err := io.WriteString(w, noDataBanner); err != nil {
			return err
		}
		return nil
	}
	const maxNames = 4096
	if len(names) > maxNames {
		names = names[:maxNames]
	}
	for _, name := range names {
		series, ok := agg.Snapshot(name)
		if !ok {
			continue
		}
		if err := renderSeries(w, series); err != nil {
			return err
		}
	}
	return nil
}

// noDataBanner is rendered when the aggregator has no series yet. The
// blank `# no metrics yet` line tells a scraper we're alive but cold,
// so dashboards don't false-alarm on a fresh restart.
const noDataBanner = "# no metrics yet\n"

// renderSeries renders one Series's HELP / TYPE lines plus its samples
// into w. Counter / gauge / histogram have distinct sample shapes; we
// dispatch on kind.
func renderSeries(w io.Writer, s metrics.Series) error {
	promName := promMetricName(s.Name, s.Kind)
	if promName == "" {
		return nil
	}
	help := s.Description
	if help == "" {
		help = s.Name
	}
	if err := writeHelpType(w, promName, s.Kind, help); err != nil {
		return err
	}
	latest := s.Latest()
	if len(latest.Buckets) > 0 {
		return renderHistogramSample(w, promName, latest)
	}
	if math.IsNaN(latest.Value) || math.IsInf(latest.Value, 0) {
		return nil
	}
	return renderScalarSample(w, promName, latest)
}

// writeHelpType emits the canonical HELP + TYPE lines.
func writeHelpType(
	w io.Writer, name string, kind metrics.Kind, help string,
) error {
	if _, err := fmt.Fprintf(
		w, "# HELP %s %s\n", name, escapeHelp(help),
	); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(
		w, "# TYPE %s %s\n", name, promTypeFor(kind),
	); err != nil {
		return err
	}
	return nil
}

// renderScalarSample writes one `name{labels} value` line.
func renderScalarSample(
	w io.Writer, name string, p metrics.Point,
) error {
	labels := promLabels(p.Labels)
	_, err := fmt.Fprintf(
		w, "%s%s %s\n", name, labels, formatFloat(p.Value),
	)
	return err
}

// renderHistogramSample emits the _bucket / _sum / _count companions
// per Prometheus convention. Each bucket gets its own line with an
// `le="..."` label appended to the sample's natural label set.
func renderHistogramSample(
	w io.Writer, name string, p metrics.Point,
) error {
	const maxBuckets = 256
	limit := len(p.Buckets)
	if limit > maxBuckets {
		limit = maxBuckets
	}
	for i := 0; i < limit; i++ {
		b := p.Buckets[i]
		labels := mergeLE(p.Labels, b.UpperBound)
		if _, err := fmt.Fprintf(
			w, "%s_bucket%s %d\n", name, labels, b.Count,
		); err != nil {
			return err
		}
	}
	labels := promLabels(p.Labels)
	if _, err := fmt.Fprintf(
		w, "%s_sum%s %s\n", name, labels, formatFloat(p.Sum),
	); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(
		w, "%s_count%s %d\n", name, labels, p.Count,
	); err != nil {
		return err
	}
	return nil
}

// promTypeFor maps the aggregator's Kind to a Prometheus TYPE label.
func promTypeFor(k metrics.Kind) string {
	switch k {
	case metrics.KindCounter:
		return "counter"
	case metrics.KindGauge:
		return "gauge"
	case metrics.KindHistogram:
		return "histogram"
	default:
		return "untyped"
	}
}

// promMetricName translates an OTel-style metric name into a
// Prometheus-legal form. Dots become underscores; double-underscores
// collapse to one; counter-kind metrics get the canonical `_total`
// suffix when absent.
func promMetricName(name string, kind metrics.Kind) string {
	if name == "" {
		return ""
	}
	const maxLen = 256
	if len(name) > maxLen {
		name = name[:maxLen]
	}
	cleaned := strings.Map(promNameCharRune, name)
	cleaned = collapseUnderscores(cleaned)
	if cleaned == "" {
		return ""
	}
	if kind == metrics.KindCounter && !strings.HasSuffix(cleaned, "_total") {
		cleaned += "_total"
	}
	return cleaned
}

// promNameCharRune maps unsafe metric-name characters to underscores.
// Prometheus metric names match [a-zA-Z_:][a-zA-Z0-9_:]*; we use
// underscore for everything outside that set so the produced names
// remain valid.
func promNameCharRune(r rune) rune {
	switch {
	case r >= 'a' && r <= 'z':
		return r
	case r >= 'A' && r <= 'Z':
		return r
	case r >= '0' && r <= '9':
		return r
	case r == '_' || r == ':':
		return r
	default:
		return '_'
	}
}

// collapseUnderscores trims doubled underscores so "workflow__runs"
// renders as "workflow_runs". Bounded scan.
func collapseUnderscores(s string) string {
	if !strings.Contains(s, "__") {
		return s
	}
	const maxIter = 64
	for i := 0; i < maxIter; i++ {
		next := strings.ReplaceAll(s, "__", "_")
		if next == s {
			return next
		}
		s = next
	}
	return s
}

// promLabels renders a label map as a Prometheus label section
// (`{k1="v1",k2="v2"}`). Empty input returns the empty string so the
// scalar renderer doesn't emit a stray `{}`.
func promLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	return formatLabelSet(labels, "", "")
}

// mergeLE adds an `le="<bound>"` label to a label set, preserving the
// existing entries. Used by histogram bucket rendering.
func mergeLE(
	labels map[string]string, upper float64,
) string {
	leValue := "+Inf"
	if !math.IsInf(upper, +1) {
		leValue = strconv.FormatFloat(upper, 'g', -1, 64)
	}
	return formatLabelSet(labels, "le", leValue)
}

// formatLabelSet sorts the label keys and emits the Prometheus label
// section. extraKey/extraValue, when non-empty, are inserted in sorted
// order alongside the regular labels.
func formatLabelSet(
	labels map[string]string, extraKey, extraValue string,
) string {
	keys := make([]string, 0, len(labels)+1)
	for k := range labels {
		keys = append(keys, k)
	}
	if extraKey != "" {
		keys = append(keys, extraKey)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(promLabelName(k))
		b.WriteByte('=')
		b.WriteByte('"')
		if k == extraKey {
			b.WriteString(escapeLabel(extraValue))
		} else {
			b.WriteString(escapeLabel(labels[k]))
		}
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// promLabelName cleans a label key the same way as a metric name,
// minus the `:` allowance (Prometheus forbids `:` in label keys).
func promLabelName(name string) string {
	if name == "" {
		return "_"
	}
	const maxLen = 128
	if len(name) > maxLen {
		name = name[:maxLen]
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '_':
			return r
		default:
			return '_'
		}
	}, name)
}

// escapeLabel renders a label value with the required \ and " escapes
// plus newline escapes. Bounded loop on rune count.
func escapeLabel(v string) string {
	if !strings.ContainsAny(v, "\\\"\n") {
		return v
	}
	const maxLen = 1024
	if len(v) > maxLen {
		v = v[:maxLen]
	}
	var b strings.Builder
	for _, r := range v {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// escapeHelp escapes the HELP line's backslash + newline per the
// Prometheus spec. HELP values are free text but must be a single
// physical line.
func escapeHelp(v string) string {
	if !strings.ContainsAny(v, "\\\n") {
		return v
	}
	const maxLen = 1024
	if len(v) > maxLen {
		v = v[:maxLen]
	}
	var b strings.Builder
	for _, r := range v {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// formatFloat renders a sample value. Special-cases NaN and Inf to
// match Prometheus's text format. Otherwise uses 'g' for compact
// output.
func formatFloat(v float64) string {
	if math.IsNaN(v) {
		return "NaN"
	}
	if math.IsInf(v, +1) {
		return "+Inf"
	}
	if math.IsInf(v, -1) {
		return "-Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
