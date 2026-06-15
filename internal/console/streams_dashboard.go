package console

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/starfederation/datastar-go/datastar"

	"github.com/danmestas/dagnats/internal/console/events"
)

// streams_dashboard.go owns /console/sse/dashboard — the live-update
// channel for the rebuilt Phase 2 dashboard. It subscribes to the
// event bus's run / dlq / audit topics, throttles per-tile patches to
// 4Hz (so a burst of completions can't flood the SSE writer), and
// emits PatchElements events targeting the matching #tile-<key> +
// #recent-failures + #recent-actions selectors.
//
// Design:
//
//   - One handler-scoped subscription per topic (run / dlq / audit).
//     Each subscription's channel feeds into a shared select loop.
//   - The select loop debounces with a per-tile minDelta. When an
//     event arrives the corresponding tile gets a "dirty" flag; the
//     debouncer ticks every 250ms and emits patches for whatever's
//     dirty. This avoids a flood while keeping the perceived latency
//     under a second.
//   - On each tile patch we re-read DashboardCounters and rebuild
//     just the affected tile. Re-reading the data source on every
//     event is cheap (the data source caches; the read is microseconds)
//     and lets us avoid keeping a shadow of the entire state machine
//     inside the SSE handler.
//   - The handler exits cleanly on r.Context().Done() — all
//     subscriptions get cancelled, the SSE writer is allowed to
//     flush its final state. Bounded loop on tick count.

// serveSSEDashboard subscribes to run / dlq / audit topics and patches
// the dashboard tiles + recent panels in place. The route is registered
// in handler.go alongside the other /console/sse/* endpoints.
//
// Run events arrive via two paths in parallel — the bus (in-process
// synthetic events from console mutation handlers) AND the live
// workflow_runs KV watch (durable engine-driven state changes). The
// KV watch covers the production "engine completed a run" case the
// bus can't see; the bus covers the synthetic test publish path and
// the future case where the engine emits an in-process "this is
// urgent" signal that hasn't yet hit JetStream.
func serveSSEDashboard(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveSSEDashboard: w is nil")
	}
	if r == nil {
		panic("serveSSEDashboard: r is nil")
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	subs := subscribeDashboardTopics(cfg)
	defer subs.close()
	subs.runUpdates = subscribeRunUpdates(r.Context(), cfg)
	sse := datastar.NewSSE(w, r)
	pumpDashboard(r.Context(), sse, ts.base, cfg, subs)
}

// subscribeRunUpdates opens a workflow_runs KV watcher when the data
// source supports it. Failures collapse to a pre-closed channel so
// the pump's select handles the missing source gracefully.
func subscribeRunUpdates(
	ctx context.Context, cfg Config,
) <-chan RunUpdate {
	if cfg.Data == nil {
		ch := make(chan RunUpdate)
		close(ch)
		return ch
	}
	ch, err := cfg.Data.WatchRuns(ctx)
	if err != nil {
		cfg.Logger.Warn("console: dashboard sse watch runs", "err", err)
		out := make(chan RunUpdate)
		close(out)
		return out
	}
	return ch
}

// dashboardSubs holds the per-topic subscriptions for the SSE handler.
// Each subscription returns a (chan, cancel) pair; close() invokes
// every cancel. runUpdates is the KV-watch channel that mirrors the
// engine-side workflow_runs bucket; cancellation of that stream is
// driven by the request context, not by a cancel func.
type dashboardSubs struct {
	runCh      <-chan events.Event
	dlqCh      <-chan events.Event
	auditCh    <-chan events.Event
	runUpdates <-chan RunUpdate
	cancels    []func()
}

// close releases every subscription. Idempotent; safe to call from a
// defer.
func (s *dashboardSubs) close() {
	if s == nil {
		return
	}
	for _, c := range s.cancels {
		c()
	}
	s.cancels = nil
}

// subscribeDashboardTopics opens one subscription per relevant topic.
// When cfg.bus is nil (no event bus configured) the channels are
// pre-closed so the pump exits on its own first iteration.
func subscribeDashboardTopics(cfg Config) *dashboardSubs {
	out := &dashboardSubs{}
	if cfg.bus == nil {
		ch := make(chan events.Event)
		close(ch)
		out.runCh, out.dlqCh, out.auditCh = ch, ch, ch
		return out
	}
	runCh, runCancel := cfg.bus.subscribe(events.TopicRun)
	dlqCh, dlqCancel := cfg.bus.subscribe(events.TopicDLQ)
	auCh, auCancel := cfg.bus.subscribe(events.TopicAudit)
	out.runCh, out.dlqCh, out.auditCh = runCh, dlqCh, auCh
	out.cancels = []func(){runCancel, dlqCancel, auCancel}
	return out
}

// dashboardThrottleInterval is the minimum gap between consecutive
// patches for the same tile. The plan calls for ≤4Hz (≥250ms); we
// pick 250ms exactly so a burst is visibly responsive but doesn't
// over-paint the page.
const dashboardThrottleInterval = 250 * time.Millisecond

// pumpDashboard is the SSE loop. Each incoming event marks the
// matching tile dirty; a ticker periodically flushes dirty tiles in
// one pass. The loop exits on ctx.Done().
func pumpDashboard(
	ctx context.Context,
	sse *datastar.ServerSentEventGenerator,
	tmpl *template.Template, cfg Config, subs *dashboardSubs,
) {
	if sse == nil {
		panic("pumpDashboard: sse is nil")
	}
	if tmpl == nil {
		panic("pumpDashboard: tmpl is nil")
	}
	if subs == nil {
		panic("pumpDashboard: subs is nil")
	}
	state := newDashboardPumpState()
	ticker := time.NewTicker(dashboardThrottleInterval)
	defer ticker.Stop()
	const maxIters = 1_000_000_000
	for i := 0; i < maxIters; i++ {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-subs.runCh:
			if !ok {
				subs.runCh = nil
				continue
			}
			state.markRunDirty(evt)
		case evt, ok := <-subs.dlqCh:
			if !ok {
				subs.dlqCh = nil
				continue
			}
			state.markDLQDirty(evt)
		case evt, ok := <-subs.auditCh:
			if !ok {
				subs.auditCh = nil
				continue
			}
			state.markAuditDirty(evt)
		case ru, ok := <-subs.runUpdates:
			if !ok {
				subs.runUpdates = nil
				continue
			}
			state.markRunUpdateDirty(ru)
		case <-ticker.C:
			if err := flushDirtyTiles(ctx, sse, tmpl, cfg, state); err != nil {
				cfg.Logger.Warn("console: sse dashboard flush",
					"err", err)
				return
			}
		}
	}
}

// dashboardPumpState tracks which tiles need re-rendering. Per-tile
// flags collapse repeated events into one render at the next tick.
type dashboardPumpState struct {
	tilesDirty      map[string]bool
	recentFailDirty bool
	recentActDirty  bool
}

func newDashboardPumpState() *dashboardPumpState {
	return &dashboardPumpState{
		tilesDirty: make(map[string]bool, 8),
	}
}

// markRunDirty marks every run-driven tile dirty when a run event
// arrives. We mark all three (in-flight, failed-1h, success-rate, p99)
// because any run completion can update any of them.
func (s *dashboardPumpState) markRunDirty(_ events.Event) {
	s.tilesDirty["failed-1h"] = true
	s.tilesDirty["in-flight"] = true
	s.tilesDirty["success-rate"] = true
	s.tilesDirty["p99-latency"] = true
	s.recentFailDirty = true
}

// markDLQDirty marks the dlq-depth tile and the recent-failures
// panel dirty.
func (s *dashboardPumpState) markDLQDirty(_ events.Event) {
	s.tilesDirty["dlq-depth"] = true
	s.recentFailDirty = true
}

// markAuditDirty marks the recent-actions panel dirty.
func (s *dashboardPumpState) markAuditDirty(_ events.Event) {
	s.recentActDirty = true
}

// markRunUpdateDirty marks every run-driven tile dirty when a KV
// run-update lands. Identical effect to markRunDirty — different
// argument types since one path arrives via the bus and the other
// via the workflow_runs KV watcher.
func (s *dashboardPumpState) markRunUpdateDirty(_ RunUpdate) {
	s.tilesDirty["failed-1h"] = true
	s.tilesDirty["in-flight"] = true
	s.tilesDirty["success-rate"] = true
	s.tilesDirty["p99-latency"] = true
	s.recentFailDirty = true
}

// flushDirtyTiles re-renders dirty tiles + recent panels via Datastar
// PatchElements events. Each patch targets a stable selector id;
// browsers handle the inner-mode replace.
func flushDirtyTiles(
	ctx context.Context,
	sse *datastar.ServerSentEventGenerator,
	tmpl *template.Template, cfg Config, state *dashboardPumpState,
) error {
	if state == nil {
		panic("flushDirtyTiles: state is nil")
	}
	if !state.anythingDirty() {
		return nil
	}
	view := buildDashboardView(ctx, cfg)
	for _, tile := range view.AllTiles() {
		if !state.tilesDirty[tile.Key] {
			continue
		}
		if err := patchOneTile(sse, tmpl, tile); err != nil {
			return fmt.Errorf("patch tile %s: %w", tile.Key, err)
		}
	}
	if state.recentFailDirty {
		if err := patchRecentFailures(sse, tmpl, view.RecentFailures); err != nil {
			return fmt.Errorf("patch recent failures: %w", err)
		}
	}
	if state.recentActDirty {
		if err := patchRecentActions(sse, tmpl, view.RecentActions); err != nil {
			return fmt.Errorf("patch recent actions: %w", err)
		}
	}
	state.reset()
	return nil
}

// anythingDirty reports whether any tile or panel needs re-rendering.
// Used by flushDirtyTiles to short-circuit when ticks fire on an idle
// connection.
func (s *dashboardPumpState) anythingDirty() bool {
	if s.recentFailDirty || s.recentActDirty {
		return true
	}
	for _, v := range s.tilesDirty {
		if v {
			return true
		}
	}
	return false
}

// reset clears every dirty flag. Called after a successful flush.
func (s *dashboardPumpState) reset() {
	for k := range s.tilesDirty {
		delete(s.tilesDirty, k)
	}
	s.recentFailDirty = false
	s.recentActDirty = false
}

// patchOneTile renders the dashboard_tile partial for one tile and
// patches it into the dashboard via Datastar's selector-by-id mode.
// The template ID matches the DOM id, so inner-mode replaces the tile
// contents while keeping the surrounding grid layout intact.
func patchOneTile(
	sse *datastar.ServerSentEventGenerator,
	tmpl *template.Template, tile DashboardTile,
) error {
	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "dashboard_tile", tile); err != nil {
		return fmt.Errorf("execute dashboard_tile: %w", err)
	}
	opts := []datastar.PatchElementOption{
		datastar.WithSelectorID(tile.DOMID()),
		datastar.WithModeOuter(),
	}
	return sse.PatchElements(buf.String(), opts...)
}

// patchRecentFailures renders the recent-failures list and patches it
// into the matching card.
func patchRecentFailures(
	sse *datastar.ServerSentEventGenerator,
	tmpl *template.Template, rows []RecentFailureRow,
) error {
	html, err := renderRecentFailuresList(tmpl, rows)
	if err != nil {
		return err
	}
	opts := []datastar.PatchElementOption{
		datastar.WithSelectorID("recent-failures"),
		datastar.WithModeOuter(),
	}
	return sse.PatchElements(html, opts...)
}

// patchRecentActions renders the recent-actions list and patches it
// into the matching card.
func patchRecentActions(
	sse *datastar.ServerSentEventGenerator,
	tmpl *template.Template, rows []RecentActionRow,
) error {
	html, err := renderRecentActionsList(tmpl, rows)
	if err != nil {
		return err
	}
	opts := []datastar.PatchElementOption{
		datastar.WithSelectorID("recent-actions"),
		datastar.WithModeOuter(),
	}
	return sse.PatchElements(html, opts...)
}

// renderRecentFailuresList renders the recent-failures partial.
// Honest empty state when rows is nil.
func renderRecentFailuresList(
	tmpl *template.Template, rows []RecentFailureRow,
) (string, error) {
	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "recent_failures_card", rows); err != nil {
		return "", fmt.Errorf("execute recent_failures_card: %w", err)
	}
	return buf.String(), nil
}

// renderRecentActionsList renders the recent-actions partial.
func renderRecentActionsList(
	tmpl *template.Template, rows []RecentActionRow,
) (string, error) {
	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "recent_actions_card", rows); err != nil {
		return "", fmt.Errorf("execute recent_actions_card: %w", err)
	}
	return buf.String(), nil
}
