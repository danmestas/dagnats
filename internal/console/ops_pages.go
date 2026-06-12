package console

import (
	"context"
	"net/http"
	"sort"
)

// ops_pages.go owns the operator pages. After the B3 nav/IA pass the
// Ops hub is gone; its children are promoted to top-level routes:
//
//   /console/workers         — workers list (placeholder telemetry)
//   /console/kv              — KV inspector (read-only)
//   /console/streams         — JetStream stream inventory (placeholder)
//   /console/leases          — current leases (placeholder telemetry)
//
// Each list page mirrors the established pattern: handler → build
// view → render. Templates live alongside the other pages.

// WorkersListView powers /console/workers. Today the engine does
// not surface worker heartbeats; we render a clear "telemetry gap"
// banner + empty table rather than mislead with synthetic data.
type WorkersListView struct {
	Header  PageHeader
	Workers []WorkerRow
	Note    string
}

// WorkerRow shape kept for when the engine starts emitting
// heartbeats. Workers slice empty until that lands.
type WorkerRow struct {
	ID            string
	Status        string
	LastHeartbeat string
	CurrentLease  string
	Uptime        string
	TaskCount     int
}

// servePageWorkers renders /console/workers.
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
		Header: buildWorkersHeader(nil),
		Note: "Worker telemetry is not yet wired. " +
			"This page surfaces the planned shape; data populates once " +
			"the engine writes to the worker_heartbeats KV bucket.",
	}
	renderPage(w, r, ts, cfg, "workers-list", pageData{
		Title:   "Workers",
		Section: "workers",
		Page:    view,
	})
}

// buildWorkersHeader projects the workers row set into count tiles.
// While telemetry is pending every count is 0 and the tile strip
// reads as honest zero-state; once heartbeats land the math here
// becomes meaningful without any template churn.
func buildWorkersHeader(rows []WorkerRow) PageHeader {
	active := 0
	idle := 0
	const rowsMax = 10_000
	for i := 0; i < len(rows) && i < rowsMax; i++ {
		switch rows[i].Status {
		case "active", "running":
			active++
		case "idle":
			idle++
		}
	}
	tiles := []Tile{
		{Label: "workers", Count: len(rows), Tone: ToneDefault},
		{Label: "active", Count: active, Tone: ToneSuccess,
			Tooltip: "Workers reporting heartbeats in the last cycle"},
		{Label: "idle", Count: idle, Tone: ToneInfo,
			Tooltip: "Connected workers with no lease in progress"},
	}
	h, err := NewPageHeader(PageHeader{
		Title:    "Workers",
		Subtitle: "Connected worker processes.",
		Tiles:    tiles,
	})
	if err != nil {
		return PageHeader{Title: "Workers"}
	}
	return h
}

// LeasesListView powers /console/leases. Same telemetry gap as
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

// servePageLeases renders /console/leases.
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
		Section: "leases",
		Page:    view,
	})
}

// KVInspectorView powers /console/kv. Buckets is the left-rail
// inventory; ActiveBucket is the currently-selected bucket; Keys is
// its key list; Entry is the materialised value when ?key=<k> is set.
type KVInspectorView struct {
	Header       PageHeader
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

// servePageKVInspector renders /console/kv.
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
	ds, ok := requireData(w, cfg, "kv-list")
	if !ok {
		return
	}
	view := buildKVInspectorView(r.Context(), ds, r.URL.Query())
	renderPage(w, r, ts, cfg, "kv-list", pageData{
		Title:   "KV inspector",
		Section: "kv",
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
	view := KVInspectorView{
		Header:  buildKVHeader(buckets),
		Buckets: buckets,
	}
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

// buildKVHeader projects the bucket inventory into the count tiles
// shown above the bucket nav. "Keys" sums across every bucket — a
// rough indicator of how much state the engine is holding — and
// "buckets" is the cardinality.
func buildKVHeader(buckets []KVBucketInfo) PageHeader {
	totalKeys := 0
	const bucketsMax = 1024
	for i := 0; i < len(buckets) && i < bucketsMax; i++ {
		totalKeys += buckets[i].Keys
	}
	tiles := []Tile{
		{Label: "buckets", Count: len(buckets), Tone: ToneDefault},
		{Label: "keys", Count: totalKeys, Tone: ToneInfo,
			Tooltip: "Total keys across reachable buckets"},
	}
	h, err := NewPageHeader(PageHeader{
		Title:             "KV inspector",
		TitleGlossaryTerm: "KV",
		Subtitle:          "Read-only inspection of JetStream KV buckets the engine uses.",
		Tiles:             tiles,
	})
	if err != nil {
		return PageHeader{Title: "KV inspector", TitleGlossaryTerm: "KV"}
	}
	return h
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

// StreamsListView powers /console/streams. Today the console doesn't
// query JetStream stream metadata directly — the DataSource surface
// doesn't expose it — so we render the known engine streams from the
// natsutil topology as a placeholder, with a callout calling out the
// telemetry gap. Same shape as workers.
type StreamsListView struct {
	Header  PageHeader
	Streams []StreamRow
	Note    string
}

// StreamRow is one row on the streams list. Messages / Bytes /
// Consumers stay "—" until the DataSource exposes JetStream metadata.
type StreamRow struct {
	Name      string
	Subjects  string
	Messages  string
	Bytes     string
	Consumers string
	Purpose   string
}

// servePageStreams renders /console/streams.
func servePageStreams(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageStreams: w is nil")
	}
	if r == nil {
		panic("servePageStreams: r is nil")
	}
	rows := knownEngineStreams()
	view := StreamsListView{
		Header:  buildStreamsHeader(rows),
		Streams: rows,
		Note: "Stream metadata is not yet wired through the DataSource. " +
			"This page lists the JetStream streams the engine's natsutil " +
			"topology creates; live message + consumer counts populate " +
			"once a stream-info adapter lands.",
	}
	renderPage(w, r, ts, cfg, "streams-list", pageData{
		Title:   "Streams",
		Section: "streams",
		Page:    view,
	})
}

// knownEngineStreams returns the static inventory of JetStream
// streams the engine creates via natsutil.SetupAll. The list is
// hand-curated so operators have something to look at while the
// stream-info adapter is unbuilt; the names match the SetupX helpers
// in internal/natsutil/conn.go.
func knownEngineStreams() []StreamRow {
	return []StreamRow{
		{Name: "TASKS", Subjects: "task.>", Messages: "—",
			Bytes: "—", Consumers: "—",
			Purpose: "Work-stealing task delivery"},
		{Name: "STICKY_TASKS", Subjects: "sticky.>",
			Messages: "—", Bytes: "—", Consumers: "—",
			Purpose: "Worker-pinned task delivery"},
		{Name: "TELEMETRY", Subjects: "telemetry.>",
			Messages: "—", Bytes: "—", Consumers: "—",
			Purpose: "Run / step metric events"},
		{Name: "TRIGGER_HISTORY", Subjects: "trigger.history.>",
			Messages: "—", Bytes: "—", Consumers: "—",
			Purpose: "Trigger firing audit log"},
		{Name: "HISTORY", Subjects: "history.>",
			Messages: "—", Bytes: "—", Consumers: "—",
			Purpose: "Per-run event timeline"},
	}
}

// buildStreamsHeader assembles the count tile for the streams page.
// While metadata is pending we have only the static cardinality;
// once the adapter lands the tile gains "with consumers" / "lagging"
// counts without any template churn.
func buildStreamsHeader(rows []StreamRow) PageHeader {
	tiles := []Tile{
		{Label: "streams", Count: len(rows), Tone: ToneDefault,
			Tooltip: "JetStream streams declared by the engine topology"},
	}
	h, err := NewPageHeader(PageHeader{
		Title:    "Streams",
		Subtitle: "JetStream streams backing the engine.",
		Tiles:    tiles,
	})
	if err != nil {
		return PageHeader{Title: "Streams"}
	}
	return h
}

// serveOpsWorkersRedirect 308-redirects /console/ops/workers to the
// promoted top-level /console/workers. Operators who bookmarked the
// old path get the new one without breaking; 308 preserves method +
// body in case something other than a GET hits the URL.
func serveOpsWorkersRedirect(
	w http.ResponseWriter, r *http.Request,
) {
	if w == nil {
		panic("serveOpsWorkersRedirect: w is nil")
	}
	if r == nil {
		panic("serveOpsWorkersRedirect: r is nil")
	}
	http.Redirect(w, r, "/console/workers", http.StatusPermanentRedirect)
}

// serveOpsKVRedirect 308-redirects /console/ops/kv to /console/kv,
// preserving any ?bucket= and ?key= query parameters so deep links
// continue to drop the operator in the right place.
func serveOpsKVRedirect(
	w http.ResponseWriter, r *http.Request,
) {
	if w == nil {
		panic("serveOpsKVRedirect: w is nil")
	}
	if r == nil {
		panic("serveOpsKVRedirect: r is nil")
	}
	target := "/console/kv"
	if raw := r.URL.RawQuery; raw != "" {
		target += "?" + raw
	}
	http.Redirect(w, r, target, http.StatusPermanentRedirect)
}

// firstQueryValue returns the first value for key in q, or "" when
// missing. Pulled here so ops pages don't import pages.go's helpers.
// (Already defined in pages.go; reuse via shared package scope.)
