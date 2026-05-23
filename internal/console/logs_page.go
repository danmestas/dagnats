package console

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/starfederation/datastar-go/datastar"
)

// logs_page.go renders /console/logs (#342, R2): a live-tail view of
// engine slog records served from the in-memory logring.Handler the
// production server installs as slog.Default. The page handler does
// every bit of filtering (severity, trace-ID, free-text) and the
// top-sources aggregation; logring itself stays a dumb transport.
//
// SSE is served at /console/sse/logs. Each appended record triggers
// one datastar-patch-elements event that prepends a <tr> to the
// #logs-tbody table. Pause/resume is implemented in the page JS by
// pausing the SSE EventSource — server-side stays stateless.

// logsListMax bounds the initial-pageload row count. Snapshot may
// return up to logring.DefaultCapEntries (10k) records; we trim to
// this on render so the HTML payload doesn't bloat. The operator can
// further filter via query params.
const logsListMax = 500

// logsTopSourcesMax bounds how many footer "top sources" rows we
// render — matches the iii reference. Aggregation runs over the
// already-filtered slice so the footer reflects the operator's
// current view.
const logsTopSourcesMax = 8

// LogsPageView is the binding the logs.html template reads.
type LogsPageView struct {
	Header         PageHeader
	SeverityFilter string
	TraceIDFilter  string
	SearchFilter   string
	Total          int
	Rendered       int
	Rows           []LogRow
	SeverityCounts []LogSeverityCount
	TopSources     []LogSourceCount
	WiredUp        bool // false when cfg.LogRing is nil → empty state.
	WiredUpNote    string
	RetentionNote  string
}

// LogRow is one rendered log line. The fields mirror what the
// operator scans in the table — they're projections of slog.Record,
// not the raw record itself, so the template stays simple HTML.
type LogRow struct {
	ID        string // synthesised stable id per row (used by SSE patch).
	Time      string // RFC3339 with millisecond resolution.
	TimeRel   string // "12s ago"-style.
	Level     string // lowercase: debug/info/warn/error.
	LevelText string // uppercase: DEBUG/INFO/WARN/ERROR (badge text).
	Message   string
	Source    string // module / component, derived from the "logger" or "source" attr.
	TraceID   string
	AttrsJSON string // remaining attrs, JSON-encoded for the expanded view.
}

// LogSeverityCount is one chip in the per-severity counter strip.
type LogSeverityCount struct {
	Level string
	Count int
}

// LogSourceCount is one row in the footer top-sources strip. Derived
// at render time from Snapshot()-then-filter; never stored in the
// ring per ADR — the ring is a transport.
type LogSourceCount struct {
	Source string
	Count  int
}

// servePageLogs renders /console/logs.
func servePageLogs(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageLogs: w is nil")
	}
	if r == nil {
		panic("servePageLogs: r is nil")
	}
	view := buildLogsView(cfg, r.URL.Query())
	renderPage(w, r, ts, cfg, "logs", pageData{
		Title:   "Logs",
		Section: "logs",
		Page:    view,
	})
}

// buildLogsView pulls the snapshot, applies filters, projects rows,
// and assembles the severity + top-sources aggregates. cfg.LogRing
// may be nil — that path renders the empty-state without panicking
// so a console mounted before observability is wired still loads.
func buildLogsView(cfg Config, q map[string][]string) LogsPageView {
	severity := strings.ToLower(strings.TrimSpace(firstQueryValue(q, "severity")))
	traceID := strings.TrimSpace(firstQueryValue(q, "trace_id"))
	search := strings.TrimSpace(firstQueryValue(q, "q"))
	view := LogsPageView{
		Header: PageHeader{
			Title:    "Logs",
			Subtitle: "Live tail of engine output. Filters apply server-side.",
		},
		SeverityFilter: severity,
		TraceIDFilter:  traceID,
		SearchFilter:   search,
		RetentionNote:  "Retains the last 10,000 entries or 30 minutes, whichever fills first. Lossy — older entries are dropped.",
	}
	if cfg.LogRing == nil {
		view.WiredUp = false
		view.WiredUpNote = "Log tail not wired. The production console " +
			"installs an in-memory ring as slog.Default; tests or " +
			"local mounts without that wiring render this empty state."
		return view
	}
	view.WiredUp = true
	records := cfg.LogRing.Snapshot()
	view.Total = len(records)
	now := time.Now()
	rows := make([]LogRow, 0, len(records))
	severityCounts := newSeverityCountsZero()
	sources := make(map[string]int, 16)
	// records are oldest-first; render newest-first.
	for i := len(records) - 1; i >= 0; i-- {
		rec := records[i]
		row := projectLogRecord(rec, now)
		severityCounts[row.Level]++
		if !logRowMatchesFilters(row, severity, traceID, search) {
			continue
		}
		sources[row.Source]++
		if len(rows) < logsListMax {
			rows = append(rows, row)
		}
	}
	view.Rows = rows
	view.Rendered = len(rows)
	view.SeverityCounts = severityCountsSlice(severityCounts)
	view.TopSources = topSourcesSlice(sources, logsTopSourcesMax)
	return view
}

// projectLogRecord converts one slog.Record into the row shape the
// template binds. The attribute walk is bounded by slog's own
// internal cap (Record.NumAttrs is finite); we record trace_id and
// source / logger explicitly and JSON-encode the remainder.
func projectLogRecord(rec slog.Record, now time.Time) LogRow {
	row := LogRow{
		ID:        fmt.Sprintf("log-row-%d", rec.Time.UnixNano()),
		Time:      rec.Time.UTC().Format("2006-01-02T15:04:05.000Z"),
		TimeRel:   formatDuration(now.Sub(rec.Time)) + " ago",
		Level:     strings.ToLower(rec.Level.String()),
		LevelText: strings.ToUpper(rec.Level.String()),
		Message:   rec.Message,
	}
	extra := make(map[string]any, rec.NumAttrs())
	rec.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "trace_id", "traceID", "TraceID":
			row.TraceID = a.Value.String()
		case "source", "logger", "component":
			if row.Source == "" {
				row.Source = a.Value.String()
			}
		default:
			extra[a.Key] = a.Value.Any()
		}
		return true
	})
	if row.Source == "" {
		row.Source = "engine"
	}
	if len(extra) > 0 {
		if buf, err := json.Marshal(extra); err == nil {
			row.AttrsJSON = string(buf)
		}
	}
	return row
}

// logRowMatchesFilters returns true when row passes every active
// filter. Empty filters pass unconditionally — same convention as
// the audit log page.
func logRowMatchesFilters(row LogRow, severity, traceID, search string) bool {
	if severity != "" && row.Level != severity {
		return false
	}
	if traceID != "" && !strings.EqualFold(row.TraceID, traceID) {
		return false
	}
	if search != "" {
		needle := strings.ToLower(search)
		hay := strings.ToLower(row.Message + " " + row.Source + " " +
			row.TraceID + " " + row.AttrsJSON)
		if !strings.Contains(hay, needle) {
			return false
		}
	}
	return true
}

// newSeverityCountsZero returns a fresh map with a zero count for
// each rendered severity. Keeps the chip strip rendering deterministic
// even when no records of a level exist yet.
func newSeverityCountsZero() map[string]int {
	return map[string]int{
		"debug": 0, "info": 0, "warn": 0, "error": 0,
	}
}

// severityCountsSlice converts the count map into a deterministic
// slice ordered debug → info → warn → error.
func severityCountsSlice(m map[string]int) []LogSeverityCount {
	order := []string{"debug", "info", "warn", "error"}
	out := make([]LogSeverityCount, 0, len(order))
	for _, k := range order {
		out = append(out, LogSeverityCount{Level: k, Count: m[k]})
	}
	return out
}

// topSourcesSlice picks the n most-frequent sources from m, ordered
// by descending count then source name. Tied buckets fall through to
// alphabetical so a re-render with the same data emits the same
// payload (no SSE row reshuffle).
func topSourcesSlice(m map[string]int, n int) []LogSourceCount {
	if n <= 0 {
		return nil
	}
	out := make([]LogSourceCount, 0, len(m))
	for src, c := range m {
		out = append(out, LogSourceCount{Source: src, Count: c})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Source < out[j].Source
	})
	if len(out) > n {
		out = out[:n]
	}
	return out
}

// firstQueryValue is a local convenience matching the pattern in
// extra_pages.go — q[key][0] with empty-string fallback. We keep one
// per page (rather than exporting a shared helper) so test fixtures
// can substitute without colliding.

// serveSSELogs streams new records to the page over Datastar SSE.
// Each record produces one patchHTML call with a <tr> rendered via
// the same projection used by buildLogsView, so first-paint and tail
// rows share their structure 1:1.
//
// Filters are read once per connection from the query string; the
// operator changing filters via the page JS triggers a fresh SSE
// connection (the page handler closes the old one). This keeps the
// server stateless across reconnects.
func serveSSELogs(w http.ResponseWriter, r *http.Request, cfg Config) {
	if w == nil {
		panic("serveSSELogs: w is nil")
	}
	if r == nil {
		panic("serveSSELogs: r is nil")
	}
	if cfg.LogRing == nil {
		http.Error(w, "log tail not wired", http.StatusServiceUnavailable)
		return
	}
	q := r.URL.Query()
	severity := strings.ToLower(strings.TrimSpace(q.Get("severity")))
	traceID := strings.TrimSpace(q.Get("trace_id"))
	search := strings.TrimSpace(q.Get("q"))

	ctx := r.Context()
	ch, cleanup := cfg.LogRing.Subscribe(ctx)
	defer cleanup()
	sse := datastar.NewSSE(w, r)
	for {
		select {
		case <-ctx.Done():
			return
		case rec, ok := <-ch:
			if !ok {
				return
			}
			row := projectLogRecord(rec, time.Now())
			if !logRowMatchesFilters(row, severity, traceID, search) {
				continue
			}
			html := renderLogRowHTML(row)
			if err := sse.PatchElements(html,
				datastar.WithSelectorID("logs-tbody"),
				datastar.WithModePrepend(),
			); err != nil {
				cfg.Logger.Warn("console: logs sse patch", "err", err)
				return
			}
		}
	}
}

// renderLogRowHTML emits the same <tr> shape the initial pageload
// table renders. Kept hand-rolled (rather than ExecuteTemplate-d) so
// the SSE path stays allocation-light and so the markup contract for
// the row is defined in exactly one place.
func renderLogRowHTML(row LogRow) string {
	var b strings.Builder
	b.Grow(256 + len(row.Message))
	b.WriteString(`<tr id="`)
	b.WriteString(htmlEscape(row.ID))
	b.WriteString(`" class="logs-row logs-row-`)
	b.WriteString(htmlEscape(row.Level))
	b.WriteString(`" data-level="`)
	b.WriteString(htmlEscape(row.Level))
	b.WriteString(`" data-trace-id="`)
	b.WriteString(htmlEscape(row.TraceID))
	b.WriteString(`">`)
	b.WriteString(`<td class="logs-time mono">`)
	b.WriteString(htmlEscape(row.Time))
	b.WriteString(`</td>`)
	b.WriteString(`<td class="logs-level"><span class="logs-badge logs-badge-`)
	b.WriteString(htmlEscape(row.Level))
	b.WriteString(`">`)
	b.WriteString(htmlEscape(row.LevelText))
	b.WriteString(`</span></td>`)
	b.WriteString(`<td class="logs-source mono">`)
	b.WriteString(htmlEscape(row.Source))
	b.WriteString(`</td>`)
	b.WriteString(`<td class="logs-message">`)
	b.WriteString(htmlEscape(row.Message))
	if row.TraceID != "" {
		b.WriteString(` <span class="logs-trace mono">trace_id=`)
		b.WriteString(htmlEscape(row.TraceID))
		b.WriteString(`</span>`)
	}
	b.WriteString(`</td>`)
	b.WriteString(`</tr>`)
	return b.String()
}

// htmlEscape is the smallest possible escaper for the four characters
// the row payload must protect against in attribute and text contexts.
// We avoid html/template here because the SSE fast-path runs without a
// template invocation and we want both call sites (initial-paint and
// SSE) to see the same escape rules.
func htmlEscape(s string) string {
	if s == "" {
		return ""
	}
	r := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&#34;",
		"'", "&#39;",
	)
	return r.Replace(s)
}
