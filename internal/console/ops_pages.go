package console

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ops_pages.go owns the operator pages. After the B3 nav/IA pass the
// Ops hub is gone; its children are promoted to top-level routes:
//
//   /console/workers         — workers list (placeholder telemetry)
//   /console/kv              — KV inspector (read-only)
//   /console/streams         — JetStream stream inventory (placeholder)
//
// Each list page mirrors the established pattern: handler → build
// view → render. Templates live alongside the other pages.

// WorkersListView powers /console/workers. Rows are read live from
// the `workers` KV directory (the engine's heartbeat surface, #289).
// When zero workers are registered the page paints an honest empty
// state rather than a synthetic placeholder.
type WorkersListView struct {
	Header  PageHeader
	Workers []WorkerStatusRow
}

// servePageWorkers renders /console/workers off the live worker
// directory. A read miss degrades to the empty state — the page is
// observational and never blocks.
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
	ds, ok := requireData(w, cfg, "workers-list")
	if !ok {
		return
	}
	rows, _ := ds.ListWorkerRows(r.Context())
	view := WorkersListView{
		Header:  buildWorkersHeader(rows),
		Workers: rows,
	}
	renderPage(w, r, ts, cfg, "workers-list", pageData{
		Title:   "Workers",
		Section: "workers",
		Page:    view,
	})
}

// buildWorkersHeader projects the live worker rows into count tiles:
// total registered plus how many are reporting fresh heartbeats vs.
// stale. Both counts are read straight from the directory's liveness
// classification, so the strip never lies about who is alive.
func buildWorkersHeader(rows []WorkerStatusRow) PageHeader {
	active := 0
	stale := 0
	const rowsMax = 10_000
	for i := 0; i < len(rows) && i < rowsMax; i++ {
		switch rows[i].Status {
		case "active":
			active++
		case "stale":
			stale++
		}
	}
	tiles := []Tile{
		{Label: "workers", Count: len(rows), Tone: ToneDefault},
		{Label: "active", Count: active, Tone: ToneSuccess,
			Tooltip: "Workers whose heartbeat is within the staleness window"},
		{Label: "stale", Count: stale, Tone: ToneWarning,
			Tooltip: "Workers whose last heartbeat is past the staleness window"},
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

// KVInspectorView powers /console/kv. Buckets is the left-rail
// inventory; ActiveBucket is the currently-selected bucket; Keys is
// its key list; Entry is the materialised value when ?key=<k> is set.
type KVInspectorView struct {
	Header       PageHeader
	Buckets      []KVBucketInfo
	ActiveBucket string
	Keys         []string
	Entry        *KVInspectorEntry
	// Catalog is the flat bucket table rendered above the 3-pane
	// inspector. Every row is backed by a live ListKVBuckets read.
	Catalog []KVCatalogRow
}

// KVCatalogRow is one row of the KV catalog table. Bucket / Keys / TTL
// are read live; Purpose is the static per-bucket label. TTL is the
// pre-formatted display string ("24h", "—") so the template stays dumb.
type KVCatalogRow struct {
	Bucket  string
	TTL     string
	Keys    int
	Purpose string
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
		Catalog: buildKVCatalogRows(buckets),
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
	ttlBounded := 0
	const bucketsMax = 1024
	for i := 0; i < len(buckets) && i < bucketsMax; i++ {
		totalKeys += buckets[i].Keys
		if buckets[i].TTL > 0 {
			ttlBounded++
		}
	}
	tiles := []Tile{
		{Label: "buckets", Count: len(buckets), Tone: ToneDefault},
		{Label: "keys", Count: totalKeys, Tone: ToneInfo,
			Tooltip: "Total keys across reachable buckets"},
		{Label: "TTL set", Count: ttlBounded, Tone: ToneInfo,
			Tooltip: "Buckets with a TTL configured (reachable)"},
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

// buildKVCatalogRows projects the live bucket inventory into catalog
// rows. TTL is humanized (0 → "—"); Purpose is the static description
// (empty → "—" rather than a fabricated label). Bounded by len(buckets).
func buildKVCatalogRows(buckets []KVBucketInfo) []KVCatalogRow {
	const bucketsMax = 1024
	if len(buckets) > bucketsMax {
		panic("buildKVCatalogRows: buckets exceeds max bound")
	}
	out := make([]KVCatalogRow, 0, len(buckets))
	for _, b := range buckets {
		purpose := b.Description
		if purpose == "" {
			purpose = "—"
		}
		out = append(out, KVCatalogRow{
			Bucket:  b.Name,
			TTL:     humanDuration(b.TTL),
			Keys:    b.Keys,
			Purpose: purpose,
		})
	}
	return out
}

// humanDuration renders a TTL as a compact operator-readable string.
// Zero (no TTL configured / unreachable) renders an honest "—", never
// "0s". Otherwise it emits up to two coarse units (d/h/m/s), modelled on
// humanBytes' base-case + scaling shape in server_page.go.
func humanDuration(d time.Duration) string {
	if d <= 0 {
		return "—"
	}
	days := int(d / (24 * time.Hour))
	hours := int(d/time.Hour) % 24
	minutes := int(d/time.Minute) % 60
	seconds := int(d/time.Second) % 60
	// A lone day (24h, no remainder) reads more naturally as "24h" to an
	// operator; days only lead the string once they stack (>=2 days) or
	// carry an hour remainder. Below a day we fall through to h/m/s.
	if days >= 2 || (days == 1 && hours > 0) {
		out := strconv.Itoa(days) + "d"
		if hours > 0 {
			out += strconv.Itoa(hours) + "h"
		}
		return out
	}
	units := []struct {
		value int
		label string
	}{{int(d / time.Hour), "h"}, {minutes, "m"}, {seconds, "s"}}
	out := ""
	emitted := 0
	for _, u := range units {
		if u.value == 0 || emitted >= 2 {
			continue
		}
		out += strconv.Itoa(u.value) + u.label
		emitted++
	}
	return out
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

// StreamsListView powers /console/streams. Rows are read live from
// ConfigSnapshot — the same JetStream stream-info read that backs the
// /config page — so message / byte / consumer counts are real. An
// unprovisioned (planned-but-absent) stream renders muted "—" cells
// rather than a synthetic zero.
type StreamsListView struct {
	Header  PageHeader
	Streams []StreamRow
}

// StreamRow is one row on the streams list. Subjects / Messages /
// Bytes / Consumers carry the live values from StreamSnapshot;
// unprovisioned streams render "—" for the count cells so they don't
// lie about zero state.
type StreamRow struct {
	Name      string
	Subjects  string
	Messages  string
	Bytes     string
	Consumers string

	// Retention / Storage are the human tokens ("workqueue" | "limits"
	// | "interest", "memory" | "file") rendered as pills. RetentionHot /
	// StorageHot flag the load-bearing variants (workqueue retention,
	// memory storage) so the template can highlight them. Seq is the
	// "firstSeq–lastSeq" range. Deleted is the NumDeleted count;
	// DeletedNonZero drives the danger tone when the stream has holes.
	// All carry "—" on unprovisioned rows so a planned-but-absent stream
	// never lies about zero state.
	Retention      string
	RetentionHot   bool
	Storage        string
	StorageHot     bool
	Seq            string
	Deleted        string
	DeletedNonZero bool

	// Policy is the stream's retention-window summary the mockup's
	// "Policy" column renders. It carries the humanized MaxAge ("168h0m0s")
	// when the stream is age-bounded and "—" when unbounded. The mockup
	// pairs max-age with a dedup window, but the StreamSnapshot does not
	// carry a duplicate-window field, so only the max-age half is shown —
	// no fabricated dedup value.
	Policy string
}

// servePageStreams renders /console/streams off the live config
// snapshot. A snapshot miss degrades to the empty state — the page is
// observational and never blocks.
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
	ds, ok := requireData(w, cfg, "streams-list")
	if !ok {
		return
	}
	snap, _ := ds.ConfigSnapshot(r.Context())
	rows := streamRowsFromSnapshots(snap.Streams)
	view := StreamsListView{
		Header:  buildStreamsHeader(rows),
		Streams: rows,
	}
	renderPage(w, r, ts, cfg, "streams-list", pageData{
		Title:   "Streams",
		Section: "streams",
		Page:    view,
	})
}

// streamRowsFromSnapshots projects the live StreamSnapshot set into
// render rows. Provisioned streams show real counts; unprovisioned
// ones show "—" so the planned-but-absent state is honest. Bounded by
// len(snaps).
func streamRowsFromSnapshots(snaps []StreamSnapshot) []StreamRow {
	const snapsMax = 1024
	if len(snaps) > snapsMax {
		panic("streamRowsFromSnapshots: snaps exceeds max bound")
	}
	out := make([]StreamRow, 0, len(snaps))
	for _, s := range snaps {
		row := StreamRow{
			Name:      s.Name,
			Subjects:  strings.Join(s.Subjects, ", "),
			Messages:  "—",
			Bytes:     "—",
			Consumers: "—",
			Retention: "—",
			Storage:   "—",
			Seq:       "—",
			Deleted:   "—",
			Policy:    "—",
		}
		if s.Provisioned {
			row.Messages = strconv.FormatUint(s.Messages, 10)
			row.Bytes = humanBytes(s.Bytes)
			row.Consumers = strconv.Itoa(s.Consumers)
			row.Retention = s.Retention
			row.RetentionHot = s.Retention == "workqueue"
			row.Storage = s.Storage
			row.StorageHot = s.Storage == "memory"
			row.Seq = fmt.Sprintf("%d–%d", s.FirstSeq, s.LastSeq)
			row.Deleted = strconv.Itoa(s.NumDeleted)
			row.DeletedNonZero = s.NumDeleted > 0
			if s.MaxAge != "" {
				row.Policy = s.MaxAge
			}
		}
		out = append(out, row)
	}
	return out
}

// buildStreamsHeader assembles the count tiles for the streams page:
// total known streams plus how many are actually provisioned in the
// JetStream account right now. Both counts come straight from the
// live snapshot.
func buildStreamsHeader(rows []StreamRow) PageHeader {
	provisioned := 0
	const rowsMax = 1024
	for i := 0; i < len(rows) && i < rowsMax; i++ {
		if rows[i].Messages != "—" {
			provisioned++
		}
	}
	tiles := []Tile{
		{Label: "streams", Count: len(rows), Tone: ToneDefault,
			Tooltip: "JetStream streams declared by the engine topology"},
		{Label: "provisioned", Count: provisioned, Tone: ToneSuccess,
			Tooltip: "Streams confirmed present in the JetStream account"},
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

// StreamDetailView powers /console/streams/{name}. NotFound is true when
// the name isn't one of the engine's known streams (or the stream is
// known-but-unprovisioned). Config / State are the two shared stat-cards;
// Consumers is the filtered "consumers on this stream" table. Every value
// is read from the same ConfigSnapshot the list page uses plus a filtered
// ListConsumers — no second stream-info round-trip, no fabricated data.
type StreamDetailView struct {
	Name        string
	NotFound    bool
	Provisioned bool
	Config      StatCard
	State       StatCard
	Consumers   []ConsumerRow
}

// dispatchStreams routes /console/streams/<name> to the read-only detail
// view. The trailing-slash prefix lands here; an empty name 404s.
func dispatchStreams(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("dispatchStreams: w is nil")
	}
	if r == nil {
		panic("dispatchStreams: r is nil")
	}
	name := strings.TrimPrefix(r.URL.Path, "/console/streams/")
	if name == "" || strings.Contains(name, "/") {
		serveNotFound(w, r, ts, cfg)
		return
	}
	servePageStreamDetail(w, r, ts, cfg, name)
}

// servePageStreamDetail renders the read-only detail for one stream. A
// snapshot miss or unknown name degrades to the honest not-found state
// within the page chrome — the view is observational and never 500s.
func servePageStreamDetail(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config, name string,
) {
	if w == nil {
		panic("servePageStreamDetail: w is nil")
	}
	if name == "" {
		panic("servePageStreamDetail: name is empty")
	}
	ds, ok := requireData(w, cfg, "stream-detail")
	if !ok {
		return
	}
	snap, _ := ds.ConfigSnapshot(r.Context())
	view := buildStreamDetail(snap.Streams, name)
	if view.Provisioned {
		consumers, _ := ds.ListConsumers(r.Context())
		view.Consumers = consumersForStream(consumers, name)
	}
	renderPage(w, r, ts, cfg, "stream-detail", pageData{
		Title:   "Stream " + name,
		Section: "streams",
		Page:    view,
	})
}

// buildStreamDetail projects one named StreamSnapshot into the detail
// view. An absent or unprovisioned stream returns NotFound so the page
// renders the honest empty state rather than a fabricated row.
func buildStreamDetail(snaps []StreamSnapshot, name string) StreamDetailView {
	const snapsMax = 1024
	for i := 0; i < len(snaps) && i < snapsMax; i++ {
		if snaps[i].Name != name {
			continue
		}
		s := snaps[i]
		if !s.Provisioned {
			return StreamDetailView{Name: name, NotFound: true}
		}
		return StreamDetailView{
			Name:        name,
			Provisioned: true,
			Config:      streamConfigCard(s),
			State:       streamStateCard(s),
		}
	}
	return StreamDetailView{Name: name, NotFound: true}
}

// streamConfigCard builds the Config stat-card from a provisioned
// snapshot. Fields the snapshot can't supply render the honest dash via
// statValueOr; -1 ceilings render as the unlimited glyph.
func streamConfigCard(s StreamSnapshot) StatCard {
	return StatCard{
		Title: "Configuration",
		Stats: []StatRow{
			{Label: "Subjects", Value: statValueOr(strings.Join(s.Subjects, ", ")), Mono: true},
			{Label: "Retention", Value: statValueOr(s.Retention)},
			{Label: "Storage", Value: statValueOr(s.Storage)},
			{Label: "Replicas", Value: strconv.Itoa(s.Replicas)},
			{Label: "Max age", Value: statValueOr(s.MaxAge)},
			{Label: "Max bytes", Value: maxCountLabel(s.MaxBytes), Mono: true},
			{Label: "Max messages", Value: maxCountLabel(s.MaxMsgs), Mono: true},
		},
	}
}

// streamStateCard builds the State stat-card from a provisioned snapshot.
func streamStateCard(s StreamSnapshot) StatCard {
	return StatCard{
		Title: "State",
		Stats: []StatRow{
			{Label: "Messages", Value: strconv.FormatUint(s.Messages, 10), Mono: true},
			{Label: "Bytes", Value: humanBytes(s.Bytes), Mono: true},
			{Label: "First seq", Value: strconv.FormatUint(s.FirstSeq, 10), Mono: true},
			{Label: "Last seq", Value: strconv.FormatUint(s.LastSeq, 10), Mono: true},
			{Label: "Consumers", Value: strconv.Itoa(s.Consumers)},
		},
	}
}

// maxCountLabel renders a JetStream max-* ceiling: -1 (or any
// non-positive) means unlimited, shown as the infinity glyph rather than
// a misleading negative number.
func maxCountLabel(max int64) string {
	if max <= 0 {
		return "∞"
	}
	return strconv.FormatInt(max, 10)
}

// WorkerDetailView powers the read-only /console/workers/{id} page.
// NotFound flags an unknown worker id (renders the honest not-found
// state, still 200 with chrome). Identity is the registration stat-card;
// Functions is the registered task-type table. The mockup's counter
// tiles, in-flight tasks table, and Drain/Resume/Decommission actions
// are deliberately absent — no backing telemetry or mutation exists.
type WorkerDetailView struct {
	WorkerID  string
	NotFound  bool
	Identity  StatCard
	Functions []WorkerFunctionRow
}

// dispatchWorkers routes /console/workers/<id> to the read-only detail
// view. The trailing-slash prefix lands here; an empty or embedded-slash
// id 404s (mirrors dispatchStreams).
func dispatchWorkers(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("dispatchWorkers: w is nil")
	}
	if r == nil {
		panic("dispatchWorkers: r is nil")
	}
	id := strings.TrimPrefix(r.URL.Path, "/console/workers/")
	if id == "" || strings.Contains(id, "/") {
		serveNotFound(w, r, ts, cfg)
		return
	}
	servePageWorkerDetail(w, r, ts, cfg, id)
}

// servePageWorkerDetail renders the read-only detail for one worker. A
// read miss or unknown id degrades to the honest not-found state within
// the page chrome — the view is observational and never 500s.
func servePageWorkerDetail(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config, id string,
) {
	if w == nil {
		panic("servePageWorkerDetail: w is nil")
	}
	if id == "" {
		panic("servePageWorkerDetail: id is empty")
	}
	ds, ok := requireData(w, cfg, "worker-detail")
	if !ok {
		return
	}
	detail, _ := ds.WorkerDetail(r.Context(), id)
	view := buildWorkerDetail(detail, id)
	renderPage(w, r, ts, cfg, "worker-detail", pageData{
		Title:   "Worker " + id,
		Section: "workers",
		Page:    view,
	})
}

// buildWorkerDetail projects one WorkerDetail into the detail view. An
// absent worker returns NotFound so the page renders the honest empty
// state rather than a fabricated identity card.
func buildWorkerDetail(detail WorkerDetail, id string) WorkerDetailView {
	if id == "" {
		panic("buildWorkerDetail: id is empty")
	}
	if !detail.Found {
		return WorkerDetailView{WorkerID: id, NotFound: true}
	}
	return WorkerDetailView{
		WorkerID:  detail.WorkerID,
		Identity:  workerIdentityCard(detail),
		Functions: detail.Functions,
	}
}

// workerIdentityCard builds the Identity stat-card from a registration.
// Only real registration fields appear; empties render the honest dash
// via statValueOr. There is no "Group" field in the wire schema, so the
// task-type list is labelled "Task types" rather than inventing a group.
func workerIdentityCard(detail WorkerDetail) StatCard {
	return StatCard{
		Title: "Identity",
		Stats: []StatRow{
			{Label: "Worker id", Value: statValueOr(detail.WorkerID), Mono: true},
			{Label: "Task types", Value: statValueOr(detail.TaskTypes), Mono: true},
			{Label: "Host", Value: statValueOr(detail.Host), Mono: true},
			{Label: "Last heartbeat", Value: statValueOr(detail.LastSeen)},
			{Label: "Status", Value: statValueOr(detail.Status)},
			{Label: "Language", Value: statValueOr(detail.Language)},
			{Label: "Transport", Value: statValueOr(detail.Transport)},
			{Label: "Max tasks", Value: statValueOr(detail.MaxTasks)},
			{Label: "Pid", Value: statValueOr(detail.Pid), Mono: true},
			{Label: "Version", Value: statValueOr(detail.Version), Mono: true},
		},
	}
}

// consumersForStream filters the global consumer list to one stream.
// Bounded by len(rows); returns a freshly-allocated slice so the caller
// owns it.
func consumersForStream(rows []ConsumerRow, stream string) []ConsumerRow {
	const rowsMax = 10_000
	out := make([]ConsumerRow, 0, len(rows))
	for i := 0; i < len(rows) && i < rowsMax; i++ {
		if rows[i].Stream == stream {
			out = append(out, rows[i])
		}
	}
	return out
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
