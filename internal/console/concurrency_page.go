package console

import "net/http"

// ConcurrencyView powers /console/concurrency: the engine's read-side
// admission-gate snapshot — singleton locks currently held and live
// per-task-type concurrency counters, read from KV. Note carries a
// read-failure explanation when the DataSource read degrades, empty
// otherwise — mirroring the connections / server pages' degrade-don't-500
// contract. Both gate buckets are empty on an idle engine, so the empty
// state is first-class, not an error.
type ConcurrencyView struct {
	Header PageHeader
	State  AdmissionState
	Note   string
	// Starved is true when at least one rate limiter's token bucket last
	// recorded an empty balance (Tokens <= 0). This is a lagging last-write
	// snapshot, not a live alarm: the engine persists the balance only on a
	// successful acquire and runs no background refiller, so a bucket that
	// drained once and went idle reads zero indefinitely even after it would
	// have refilled. The rate-limit callout renders only when it is set and is
	// worded as a snapshot, never as proof of live shedding.
	Starved bool
}

// servePageConcurrency renders /console/concurrency. An AdmissionState
// read failure degrades to an empty state + an explanatory Note rather
// than a 500, matching how the connections / server pages survive a KV
// read hiccup — operators see the page shell, not an error screen.
func servePageConcurrency(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageConcurrency: w is nil")
	}
	if r == nil {
		panic("servePageConcurrency: r is nil")
	}
	data, ok := requireData(w, cfg, "read admission state")
	if !ok {
		return
	}
	state, err := data.AdmissionState(r.Context())
	note := ""
	if err != nil {
		cfg.Logger.Error("console: admission state", "err", err)
		state = AdmissionState{}
		note = "Admission-gate state could not be read from KV right now; " +
			"the locks and counters are empty until the read succeeds."
	}
	view := ConcurrencyView{
		Header:  buildConcurrencyHeader(state),
		State:   state,
		Note:    note,
		Starved: anyRateLimitExhausted(state),
	}
	renderPage(w, r, ts, cfg, "concurrency", pageData{
		Title:   "Concurrency",
		Section: "concurrency",
		Page:    view,
	})
}

// anyRateLimitExhausted reports whether any rate limiter's last-recorded token
// balance is empty (Tokens <= 0). The rate_limits KV value is written only on a
// successful acquire (engine/ratelimit.go saveBucket) and no background
// refiller updates it, so a true result means "this limiter drained at its last
// acquire", NOT "tasks are being shed right now" — a drained-then-idle bucket
// reads zero indefinitely. Callers must word the signal as a lagging snapshot.
func anyRateLimitExhausted(s AdmissionState) bool {
	if s.RateLimits == nil {
		return false
	}
	for i := range s.RateLimits {
		if s.RateLimits[i].Tokens <= 0 {
			return true
		}
	}
	return false
}

// buildConcurrencyHeader assembles the count strip for the concurrency
// page: locks held and total tasks in-flight. Both are neutral facts
// (ToneInfo), not alarms — a held lock and an in-flight counter are the
// gates working as designed, never a failure to flag in red.
func buildConcurrencyHeader(s AdmissionState) PageHeader {
	var inFlightTotal int
	for i := range s.TaskSlots {
		inFlightTotal += s.TaskSlots[i].InFlight
	}
	tiles := []Tile{
		{Label: "locks held", Count: len(s.Locks), Tone: ToneInfo,
			Tooltip: "Singleton locks currently held across all workflows"},
		{Label: "tasks in-flight", Count: inFlightTotal, Tone: ToneInfo,
			Tooltip: "Sum of live per-task-type concurrency counters"},
		{Label: "rate-limited", Count: len(s.RateLimits), Tone: ToneInfo,
			Tooltip: "Task types with an active token-bucket rate limiter"},
	}
	header, err := NewPageHeader(PageHeader{
		Title:    "Concurrency",
		Subtitle: "Admission control — singleton locks held and live task-concurrency counters.",
		Tiles:    tiles,
	})
	if err != nil {
		return PageHeader{Title: "Concurrency"}
	}
	return header
}
