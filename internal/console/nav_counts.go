// nav_counts.go owns the Batch-9 nav-badge count endpoint.
//
// The console nav (layout.html) renders on EVERY page. Putting ten
// ListX scans into every page render would make the nav the most
// expensive thing on the page. Instead the nav ships static badge
// placeholders (`<span data-nav-count="<key>">`) and a small client
// script (assets/sources/nav-counts.js) fetches this one JSON endpoint
// once after load and fills the badges.
//
// Honesty contract: a source that errors is OMITTED from the payload
// entirely. The client renders a badge only for a key that is present,
// so an unavailable count shows no badge rather than a fabricated 0.
// Services + Traces are intentionally absent — they have no data /
// route yet, and a zero-backed badge there would be a dead affordance.
package console

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// navCountsTimeout bounds the whole fan-out. Each source read shares
// the deadline; a slow source degrades to a missing key, never a
// hung request.
const navCountsTimeout = 1500 * time.Millisecond

// readNavCounts assembles the per-nav-item counts the badge endpoint
// returns. Every read is best-effort: an error omits that one key so
// the client never paints a count the engine couldn't confirm. Returns
// an empty (non-nil) map when no data source is wired.
func readNavCounts(ctx context.Context, cfg Config) map[string]int {
	if ctx == nil {
		panic("readNavCounts: ctx is nil")
	}
	counts := make(map[string]int, 10)
	if cfg.Data == nil {
		return counts
	}
	rctx, cancel := context.WithTimeout(ctx, navCountsTimeout)
	defer cancel()
	readNavListCounts(rctx, cfg.Data, counts)
	readNavSnapshotCounts(rctx, cfg.Data, counts)
	return counts
}

// readNavListCounts fills the counts driven by direct List* reads:
// workflows, runs, triggers, dlq, consumers, connections, kv. Each
// missing/errored read leaves its key unset.
func readNavListCounts(
	ctx context.Context, ds DataSource, counts map[string]int,
) {
	if counts == nil {
		panic("readNavListCounts: counts is nil")
	}
	if ds == nil {
		panic("readNavListCounts: ds is nil")
	}
	if v, err := ds.ListWorkflows(ctx); err == nil {
		counts["workflows"] = len(v)
	}
	if v, err := ds.ListRuns(ctx, ""); err == nil {
		counts["runs"] = len(v)
	}
	if v, err := ds.ListTriggers(ctx); err == nil {
		counts["triggers"] = len(v)
	}
	if v, err := ds.ListDeadLetters(ctx, dlqCountScanMax); err == nil {
		counts["dlq"] = len(v)
	}
	if v, err := ds.ListConsumers(ctx); err == nil {
		counts["consumers"] = len(v)
	}
	if v, err := ds.ListConnections(ctx); err == nil {
		counts["connections"] = len(v)
	}
	if v, err := ds.ListKVBuckets(ctx); err == nil {
		counts["kv"] = len(v)
	}
}

// dlqCountScanMax bounds the DLQ scan the badge issues. The DLQ badge
// only needs a count; 500 is the same ceiling readDashboardCounters
// uses so the two surfaces agree on "depth".
const dlqCountScanMax = 500

// readNavSnapshotCounts fills the counts that come from richer reads:
// workers + functions (AggregateTaskTypes) and streams (ConfigSnapshot).
// Each missing/errored read leaves its key unset.
func readNavSnapshotCounts(
	ctx context.Context, ds DataSource, counts map[string]int,
) {
	if counts == nil {
		panic("readNavSnapshotCounts: counts is nil")
	}
	if ds == nil {
		panic("readNavSnapshotCounts: ds is nil")
	}
	if rows, err := ds.ListWorkerRows(ctx); err == nil {
		counts["workers"] = len(rows)
	}
	if rows, err := ds.AggregateTaskTypes(ctx); err == nil {
		counts["functions"] = len(rows)
	}
	if snap, err := ds.ConfigSnapshot(ctx); err == nil {
		counts["streams"] = len(snap.Streams)
	}
}

// serveNavCounts answers GET /console/api/nav-counts with the per-item
// count map. Loopback-trusted via the console mount path. Always 200
// with a (possibly partial) object — the client renders only the keys
// present, so a degraded read surfaces as fewer badges, never an error.
func serveNavCounts(
	w http.ResponseWriter, r *http.Request, cfg Config,
) {
	if w == nil {
		panic("serveNavCounts: w is nil")
	}
	if r == nil {
		panic("serveNavCounts: r is nil")
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	counts := readNavCounts(r.Context(), cfg)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	encoded, err := json.Marshal(counts)
	if err != nil {
		http.Error(w, "encode failed", http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(encoded)
}
