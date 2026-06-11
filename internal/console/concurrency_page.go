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
		Header: buildConcurrencyHeader(state),
		State:  state,
		Note:   note,
	}
	renderPage(w, r, ts, cfg, "concurrency", pageData{
		Title:   "Concurrency",
		Section: "concurrency",
		Page:    view,
	})
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
