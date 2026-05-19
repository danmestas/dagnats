package console

import (
	"context"
	"net/http"
	"sort"
)

// ops_pages.go owns the PR 5b operator pages under /console/ops:
//
//   /console/ops             — index (tiles linking to sub-pages)
//   /console/ops/workers     — workers list (placeholder: engine
//                              doesn't surface heartbeats yet)
//   /console/ops/leases      — current leases (same placeholder gap)
//   /console/ops/kv          — KV inspector (read-only)
//
// Each list page mirrors the established pattern: handler → build
// view → render. Templates live alongside the other ops pages.

// OpsIndexView is the binding for /console/ops.
type OpsIndexView struct {
	Tiles []OpsTile
}

// OpsTile is one summary tile on the ops index. Hint is a short
// secondary string under the tile's metric so the operator gets
// orientation without needing to click through.
type OpsTile struct {
	Title      string
	Section    string
	Href       string
	MetricText string
	Hint       string
	Disabled   bool
}

// servePageOpsIndex renders /console/ops with tiles per sub-page.
func servePageOpsIndex(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageOpsIndex: w is nil")
	}
	if r == nil {
		panic("servePageOpsIndex: r is nil")
	}
	view := buildOpsIndex(r.Context(), cfg.Data)
	renderPage(w, r, ts, cfg, "ops-index", pageData{
		Title:   "Ops",
		Section: "ops",
		Page:    view,
	})
}

// buildOpsIndex assembles the four ops tiles. Counts come from the
// DataSource when present; absent data leaves the metric blank rather
// than throwing.
func buildOpsIndex(
	ctx context.Context, ds DataSource,
) OpsIndexView {
	tiles := []OpsTile{
		{
			Title: "Workers", Section: "ops-workers",
			Href: "/console/ops/workers",
			Hint: "active worker processes",
		},
		{
			Title: "Leases", Section: "ops-leases",
			Href: "/console/ops/leases",
			Hint: "in-flight task leases",
		},
		{
			Title: "KV inspector", Section: "ops-kv",
			Href: "/console/ops/kv",
			Hint: "browse engine state buckets",
		},
		{
			Title: "Audit log", Section: "ops-audit",
			Href: "/console/ops/audit",
			Hint: "operator-action history",
		},
	}
	if ds == nil {
		return OpsIndexView{Tiles: tiles}
	}
	enrichOpsTiles(ctx, ds, tiles)
	return OpsIndexView{Tiles: tiles}
}

// enrichOpsTiles fills the per-tile metric text from the DataSource.
// Workers / Leases are placeholders — engine doesn't track them yet —
// so we surface that fact explicitly rather than pretend with a 0.
func enrichOpsTiles(
	ctx context.Context, ds DataSource, tiles []OpsTile,
) {
	for i := range tiles {
		switch tiles[i].Section {
		case "ops-workers", "ops-leases":
			tiles[i].MetricText = "engine telemetry pending"
		case "ops-kv":
			buckets, err := ds.ListKVBuckets(ctx)
			if err == nil {
				tiles[i].MetricText = pluralize(len(buckets), "bucket")
			}
		case "ops-audit":
			events, err := ds.ListAuditEvents(ctx, 100)
			if err == nil {
				tiles[i].MetricText = pluralize(len(events), "event")
			}
		}
	}
}

// pluralize renders "1 bucket" / "5 buckets" without bringing in a
// dependency. Used for tile metric copy.
func pluralize(n int, singular string) string {
	if n == 1 {
		return "1 " + singular
	}
	return intToStr(n) + " " + singular + "s"
}

// intToStr is a tiny integer formatter to avoid pulling in fmt for
// the simple decimal case.
func intToStr(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	const digits = "0123456789"
	var buf [16]byte
	idx := len(buf)
	for n > 0 {
		idx--
		buf[idx] = digits[n%10]
		n /= 10
	}
	if neg {
		idx--
		buf[idx] = '-'
	}
	return string(buf[idx:])
}

// WorkersListView powers /console/ops/workers. Today the engine does
// not surface worker heartbeats; we render a clear "telemetry gap"
// banner + empty table rather than mislead with synthetic data.
type WorkersListView struct {
	Workers []WorkerRow
	Note    string
}

// WorkerRow shape kept for when the engine starts emitting
// heartbeats. PR 5b leaves Workers empty.
type WorkerRow struct {
	ID            string
	Status        string
	LastHeartbeat string
	CurrentLease  string
	Uptime        string
	TaskCount     int
}

// servePageWorkers renders /console/ops/workers.
func servePageWorkers(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageWorkers: w is nil")
	}
	if r == nil {
		panic("servePageWorkers: r is nil")
	}
	view := WorkersListView{
		Note: "Worker telemetry is not yet wired. " +
			"This page surfaces the planned shape; data populates once " +
			"the engine writes to the worker_heartbeats KV bucket.",
	}
	renderPage(w, r, ts, cfg, "ops-workers", pageData{
		Title:   "Workers",
		Section: "ops",
		Page:    view,
	})
}

// LeasesListView powers /console/ops/leases. Same telemetry gap as
// workers — surfaces the shape so operators recognise the eventual
// fill.
type LeasesListView struct {
	Leases []LeaseRow
	Note   string
}

// LeaseRow shape for when the engine surfaces leases. Empty for now.
type LeaseRow struct {
	ID         string
	Worker     string
	Workflow   string
	Step       string
	Acquired   string
	Expires    string
	NearExpiry bool
}

// servePageLeases renders /console/ops/leases.
func servePageLeases(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageLeases: w is nil")
	}
	if r == nil {
		panic("servePageLeases: r is nil")
	}
	view := LeasesListView{
		Note: "Lease telemetry is not yet wired. " +
			"Today, leases are tracked internally by the engine's admission " +
			"layer but not surfaced to the console.",
	}
	renderPage(w, r, ts, cfg, "ops-leases", pageData{
		Title:   "Leases",
		Section: "ops",
		Page:    view,
	})
}

// KVInspectorView powers /console/ops/kv. Buckets is the left-rail
// inventory; ActiveBucket is the currently-selected bucket; Keys is
// its key list; Entry is the materialised value when ?key=<k> is set.
type KVInspectorView struct {
	Buckets      []KVBucketInfo
	ActiveBucket string
	Keys         []string
	Entry        *KVInspectorEntry
}

// KVInspectorEntry is one rendered KV value pane. ValuePretty is the
// JSON-pretty-printed form when IsJSON; ValueRaw is the byte
// representation as text.
type KVInspectorEntry struct {
	Bucket      string
	Key         string
	ValuePretty string
	ValueRaw    string
	Revision    uint64
	IsJSON      bool
	NotFound    bool
}

// servePageKVInspector renders /console/ops/kv.
func servePageKVInspector(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageKVInspector: w is nil")
	}
	if r == nil {
		panic("servePageKVInspector: r is nil")
	}
	ds, ok := requireData(w, cfg, "ops-kv")
	if !ok {
		return
	}
	view := buildKVInspectorView(r.Context(), ds, r.URL.Query())
	renderPage(w, r, ts, cfg, "ops-kv", pageData{
		Title:   "KV inspector",
		Section: "ops",
		Page:    view,
	})
}

// buildKVInspectorView fetches the bucket list + active bucket's keys
// + entry detail when ?bucket=&key= are present. Mistakes are
// rendered as zero-state rather than 500s — KV inspector is
// observational, never blocking.
func buildKVInspectorView(
	ctx context.Context, ds DataSource, q map[string][]string,
) KVInspectorView {
	buckets, _ := ds.ListKVBuckets(ctx)
	view := KVInspectorView{Buckets: buckets}
	active := firstQueryValue(q, "bucket")
	if active == "" && len(buckets) > 0 {
		active = buckets[0].Name
	}
	view.ActiveBucket = active
	if active == "" {
		return view
	}
	const keyLimit = 200
	keys, _, _ := ds.ListKVKeys(ctx, active, "", keyLimit)
	sort.Strings(keys)
	view.Keys = keys
	key := firstQueryValue(q, "key")
	if key == "" {
		return view
	}
	view.Entry = buildKVEntry(ctx, ds, active, key)
	return view
}

// buildKVEntry pulls one entry from the DataSource and converts it
// into the render shape. Pretty-prints JSON when present.
func buildKVEntry(
	ctx context.Context, ds DataSource, bucket, key string,
) *KVInspectorEntry {
	entry, err := ds.GetKVEntry(ctx, bucket, key)
	if err != nil {
		return &KVInspectorEntry{Bucket: bucket, Key: key, NotFound: true}
	}
	out := &KVInspectorEntry{
		Bucket:   bucket,
		Key:      key,
		ValueRaw: string(entry.Value),
		Revision: entry.Revision,
		IsJSON:   entry.IsJSON,
	}
	if entry.IsJSON {
		out.ValuePretty = prettyJSON(entry.Value)
	}
	return out
}

// firstQueryValue returns the first value for key in q, or "" when
// missing. Pulled here so ops pages don't import pages.go's helpers.
// (Already defined in pages.go; reuse via shared package scope.)
