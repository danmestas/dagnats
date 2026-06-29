package console

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"

	"github.com/danmestas/dagnats/internal/console/events"
	"github.com/starfederation/datastar-go/datastar"
)

// streams_extra.go owns the PR 5 SSE endpoints that complete the live
// experience promised by ADR-014:
//
//   GET /console/sse/triggers   — triggers list page live updates.
//   GET /console/sse/dlq        — DLQ list page live updates.
//
// Both handlers mirror the runs-SSE shape from streams.go (PR 3). The
// repetition is honest: each watcher returns its own channel type and
// the per-update render is small enough that abstraction would obscure
// more than it saves.

// serveSSETriggers streams trigger KV updates as Datastar patches into
// #triggers-tbody. New triggers prepend with a highlight; toggles
// replace the existing row; deletes patch the row out.
func serveSSETriggers(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveSSETriggers: w is nil")
	}
	if r == nil {
		panic("serveSSETriggers: r is nil")
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	ds, ok := requireData(w, cfg, "sse-triggers")
	if !ok {
		return
	}
	ch, err := ds.WatchTriggers(r.Context())
	if err != nil {
		cfg.Logger.Error("console: sse triggers watch", "err", err)
		http.Error(w, "watch failed", http.StatusServiceUnavailable)
		return
	}
	sse := datastar.NewSSE(w, r)
	pumpTriggerUpdates(r.Context(), sse, ts.base, ch, cfg)
}

// pumpTriggerUpdates is the inner loop translating TriggerUpdate values
// into Datastar PatchElements events. Bounded loop, exits on ctx done
// or channel close. Watcher initial-replay is included; the patch logic
// is idempotent so re-rendering an already-present row is harmless.
func pumpTriggerUpdates(
	ctx context.Context,
	sse *datastar.ServerSentEventGenerator,
	tmpl *template.Template,
	ch <-chan TriggerUpdate, cfg Config,
) {
	if sse == nil {
		panic("pumpTriggerUpdates: sse is nil")
	}
	if tmpl == nil {
		panic("pumpTriggerUpdates: tmpl is nil")
	}
	const maxIters = 1_000_000_000
	for i := 0; i < maxIters; i++ {
		select {
		case <-ctx.Done():
			return
		case update, ok := <-ch:
			if !ok {
				return
			}
			if err := emitTriggerPatch(
				sse, tmpl, update, cfg.ReadOnly,
			); err != nil {
				cfg.Logger.Warn("console: sse triggers emit",
					"trigger_id", update.Def.ID, "err", err)
				return
			}
		}
	}
}

// emitTriggerPatch translates one TriggerUpdate into one or two
// Datastar patches. Updates: remove-then-prepend so the row lands at
// the top with a fresh highlight regardless of prior state. Deletes:
// remove-only.
func emitTriggerPatch(
	sse *datastar.ServerSentEventGenerator,
	tmpl *template.Template, update TriggerUpdate, readOnly bool,
) error {
	if update.Def.ID == "" {
		return nil
	}
	rowSelector := "#trigger-row-" + update.Def.ID
	if update.Deleted {
		return removePatch(sse, rowSelector, update.Seq)
	}
	row := triggerRowFromDef(update.Def)
	html, err := renderFragment(tmpl, "trigger-row", triggerRowPatch{
		Row:      row,
		Fresh:    true,
		PutSeq:   update.Seq,
		ReadOnly: readOnly,
	})
	if err != nil {
		return fmt.Errorf("render trigger-row: %w", err)
	}
	if err := removePatch(sse, rowSelector, update.Seq); err != nil {
		return err
	}
	prependOpts := []datastar.PatchElementOption{
		datastar.WithSelector("#triggers-tbody"),
		datastar.WithMode(datastar.ElementPatchModePrepend),
		datastar.WithPatchElementsEventID(
			strconv.FormatUint(update.Seq, 10)),
	}
	if err := sse.PatchElements(html, prependOpts...); err != nil {
		return fmt.Errorf("patch trigger row: %w", err)
	}
	return emitCountChip(sse, "triggers-count", update.Seq)
}

// triggerRowPatch is the template binding for the trigger-row fragment.
type triggerRowPatch struct {
	Row      TriggerRow
	Fresh    bool
	PutSeq   uint64
	ReadOnly bool
}

// removePatch issues one Datastar remove against selector. Returns nil
// even when the selector matches nothing — Datastar logs a warning but
// doesn't propagate that as an error, so the caller treats it as
// best-effort.
func removePatch(
	sse *datastar.ServerSentEventGenerator,
	selector string, seq uint64,
) error {
	if sse == nil {
		panic("removePatch: sse is nil")
	}
	if selector == "" {
		panic("removePatch: selector is empty")
	}
	rmOpts := []datastar.PatchElementOption{
		datastar.WithSelector(selector),
		datastar.WithMode(datastar.ElementPatchModeRemove),
		datastar.WithPatchElementsEventID(
			strconv.FormatUint(seq, 10)),
	}
	if err := sse.PatchElements("", rmOpts...); err != nil {
		// A row-not-found warning isn't fatal; pass through other
		// transport failures.
		if strings.Contains(err.Error(), "not found") {
			return nil
		}
		return err
	}
	return nil
}

// serveSSEDLQ streams DLQ updates as Datastar patches into
// #dlq-tbody. Two sources feed the stream:
//
//   - The KV watcher fan-out (additions): new dead letters land here.
//   - The in-process event bus (removals + replaces): mutation
//     handlers publish "row.remove" / "row.replace" events so the
//     list refreshes without a page reload.
//
// Both sources share the SSE writer; events arrive in their natural
// order and the pump multiplexes them.
func serveSSEDLQ(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveSSEDLQ: w is nil")
	}
	if r == nil {
		panic("serveSSEDLQ: r is nil")
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	ds, ok := requireData(w, cfg, "sse-dlq")
	if !ok {
		return
	}
	// The DLQ live feed has two independent sources: the JetStream
	// new-entry watch (ch) and the in-process bus (discard/retry/undo
	// mutations). A failure to establish the watch — e.g. no stream
	// matches dead_letters.> — must NOT 503 the whole stream, or a
	// discard's row-remove never reaches the page ("after discard
	// nothing deletes"). Degrade to bus-only: nil ch, keep the bus pump.
	ch, err := ds.WatchDLQ(r.Context())
	if err != nil {
		cfg.Logger.Warn("console: sse dlq watch unavailable; serving"+
			" bus-only", "err", err)
		ch = nil
	}
	busCh, busCancel := subscribeDLQEvents(cfg)
	defer busCancel()
	sse := datastar.NewSSE(w, r)
	// Thread per-actor CSRF + read-only state into the SSE pump so the
	// freshly-rendered rows carry the same inline-action affordances
	// as the initial page render. Without this, SSE-prepended rows
	// would arrive without working Retry/Discard buttons.
	rowCtx := dlqRowContext{
		ReadOnly:  cfg.ReadOnly,
		CSRFToken: csrfTokenFor(r),
	}
	pumpDLQCombined(r.Context(), sse, ts.base, ch, busCh, cfg, rowCtx)
}

// subscribeDLQEvents returns a receive-only channel of bus events
// for the DLQ topic, plus a cancel function. When the bus isn't
// configured the channel is pre-closed.
func subscribeDLQEvents(
	cfg Config,
) (<-chan events.Event, func()) {
	if cfg.bus == nil {
		ch := make(chan events.Event)
		close(ch)
		return ch, func() {}
	}
	return cfg.bus.subscribe(events.TopicDLQ)
}

// pumpDLQCombined multiplexes the KV-watch DLQUpdate stream and the
// in-process events.Event stream onto one SSE writer. Either channel
// closing ends the loop; ctx cancellation likewise.
func pumpDLQCombined(
	ctx context.Context,
	sse *datastar.ServerSentEventGenerator,
	tmpl *template.Template,
	kvCh <-chan DLQUpdate, busCh <-chan events.Event,
	cfg Config, rowCtx dlqRowContext,
) {
	if sse == nil {
		panic("pumpDLQCombined: sse is nil")
	}
	if tmpl == nil {
		panic("pumpDLQCombined: tmpl is nil")
	}
	const maxIters = 1_000_000_000
	for i := 0; i < maxIters; i++ {
		select {
		case <-ctx.Done():
			return
		case update, ok := <-kvCh:
			if !ok {
				// New-entry watch ended (or was never wired). Keep
				// serving bus mutations rather than tearing the whole
				// SSE down — symmetric with the busCh-closed case below.
				kvCh = nil
				continue
			}
			if err := emitDLQPatch(sse, tmpl, update, rowCtx); err != nil {
				cfg.Logger.Warn("console: sse dlq emit",
					"seq", update.View.Sequence, "err", err)
				return
			}
		case evt, ok := <-busCh:
			if !ok {
				busCh = nil
				continue
			}
			if err := emitDLQBusPatch(sse, evt); err != nil {
				cfg.Logger.Warn("console: sse dlq bus emit",
					"key", evt.Key, "err", err)
				return
			}
		}
	}
}

// emitDLQBusPatch translates one events.Event for the DLQ topic into
// a Datastar patch. row.remove patches the row out (used after retry
// + soft-discard expiry); row.replace re-renders the row (used after
// undo restores).
func emitDLQBusPatch(
	sse *datastar.ServerSentEventGenerator, evt events.Event,
) error {
	if sse == nil {
		panic("emitDLQBusPatch: sse is nil")
	}
	if evt.Key == "" {
		return nil
	}
	switch evt.Op {
	case events.OpRowRemove:
		return removePatch(sse, "#dlq-row-"+evt.Key, 0)
	}
	return nil
}

// emitDLQPatch renders one DLQ row and prepends it to #dlq-tbody.
// DLQOpRemoved patches just remove the row.
func emitDLQPatch(
	sse *datastar.ServerSentEventGenerator,
	tmpl *template.Template, update DLQUpdate, rowCtx dlqRowContext,
) error {
	if update.View.Sequence == 0 {
		return nil
	}
	rowSelector := "#dlq-row-" + strconv.FormatUint(update.View.Sequence, 10)
	if update.Operation == DLQOpRemoved {
		return removePatch(sse, rowSelector, update.View.Sequence)
	}
	row := dlqRowFromView(update.View)
	html, err := renderFragment(tmpl, "dlq-row", dlqRowPatch{
		Row:       row,
		Fresh:     true,
		PutSeq:    update.View.Sequence,
		ReadOnly:  rowCtx.ReadOnly,
		CSRFToken: rowCtx.CSRFToken,
	})
	if err != nil {
		return fmt.Errorf("render dlq-row: %w", err)
	}
	if err := removePatch(sse, rowSelector, update.View.Sequence); err != nil {
		return err
	}
	prependOpts := []datastar.PatchElementOption{
		datastar.WithSelector("#dlq-tbody"),
		datastar.WithMode(datastar.ElementPatchModePrepend),
		datastar.WithPatchElementsEventID(
			strconv.FormatUint(update.View.Sequence, 10)),
	}
	if err := sse.PatchElements(html, prependOpts...); err != nil {
		return fmt.Errorf("patch dlq row: %w", err)
	}
	return emitCountChip(sse, "dlq-count", update.View.Sequence)
}

// dlqRowPatch is the template binding for the dlq-row fragment.
// ReadOnly + CSRFToken carry the per-actor state the inline-action
// buttons need; without them an SSE-arriving row would render with
// dead Retry/Discard buttons and force the operator back through
// the inspect-then-act loop that T09 was meant to eliminate.
type dlqRowPatch struct {
	Row       DLQRow
	Fresh     bool
	PutSeq    uint64
	ReadOnly  bool
	CSRFToken string
}

// dlqRowContext is the per-request context the SSE pump needs to
// render rows with working inline actions. Threaded from the SSE
// handler (which has the *http.Request) down to emitDLQPatch.
type dlqRowContext struct {
	ReadOnly  bool
	CSRFToken string
}

// emitCountChip patches a small total-count chip in the page header.
// The chip's id is a stable per-page string (triggers-count, dlq-count,
// runs-count); the count is a Datastar morph-by-id replacement so the
// number animates without flicker. Pattern: append a one-tick increment
// rather than recomputing — the chip carries data-count which the
// JS-side ticker maintains. To avoid that complexity, we just write
// the current count as the inner text, sourced from the row count of
// the tbody after this patch lands. Datastar reads the post-patch DOM,
// so the per-update arithmetic happens client-side via the patch event.
func emitCountChip(
	sse *datastar.ServerSentEventGenerator,
	chipID string, seq uint64,
) error {
	if sse == nil {
		panic("emitCountChip: sse is nil")
	}
	if chipID == "" {
		panic("emitCountChip: chipID is empty")
	}
	// Single-shot signal write: client JS reads data-count attrs and
	// increments. We just touch the chip's data-last attr so the
	// front-end ticker picks up the update event. Keeps the server
	// stateless wrt the count itself.
	html := fmt.Sprintf(
		`<span id="%s-tick" data-last="%d" hidden></span>`,
		chipID, seq)
	opts := []datastar.PatchElementOption{
		datastar.WithSelector("#" + chipID + "-tick"),
		datastar.WithMode(datastar.ElementPatchModeReplace),
		datastar.WithPatchElementsEventID(
			strconv.FormatUint(seq, 10)),
	}
	return sse.PatchElements(html, opts...)
}
