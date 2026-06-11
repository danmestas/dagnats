package console

import "net/http"

// ConnectionsView powers /console/connections: the embedded NATS
// server's live client connections, read in-process via Connz(). Note
// carries a read-failure explanation when the DataSource read degrades,
// empty otherwise — mirroring the consumers page's degrade-don't-500
// contract.
type ConnectionsView struct {
	Header PageHeader
	Conns  []ConnRow
	Note   string
}

// servePageConnections renders /console/connections. A ListConnections
// read failure degrades to an empty list + an explanatory Note rather
// than a 500, matching how the consumers / server pages survive a
// monitoring read hiccup — operators see the page shell, not an error
// screen.
func servePageConnections(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageConnections: w is nil")
	}
	if r == nil {
		panic("servePageConnections: r is nil")
	}
	data, ok := requireData(w, cfg, "list connections")
	if !ok {
		return
	}
	rows, err := data.ListConnections(r.Context())
	note := ""
	if err != nil {
		cfg.Logger.Error("console: list connections", "err", err)
		rows = nil
		note = "Client connections could not be read from the embedded " +
			"server right now; the list is empty until the read succeeds."
	}
	view := ConnectionsView{
		Header: buildConnectionsHeader(rows),
		Conns:  rows,
		Note:   note,
	}
	renderPage(w, r, ts, cfg, "connections", pageData{
		Title:   "Connections",
		Section: "connections",
		Page:    view,
	})
}

// buildConnectionsHeader assembles the count strip for the connections
// page: total connections and total subscriptions across them. Both are
// neutral facts (ToneInfo), not alarms — pending bytes is the only
// slow-consumer signal and it's shown per-row, never aggregated into a
// danger tile that would false-alarm on a momentary queue.
func buildConnectionsHeader(rows []ConnRow) PageHeader {
	var subsTotal int
	for i := range rows {
		subsTotal += int(rows[i].Subs)
	}
	tiles := []Tile{
		{Label: "connections", Count: len(rows), Tone: ToneInfo,
			Tooltip: "Live client connections to the embedded NATS server"},
		{Label: "subscriptions", Count: subsTotal, Tone: ToneInfo,
			Tooltip: "Subscriptions held across all connections, summed"},
	}
	header, err := NewPageHeader(PageHeader{
		Title:    "Connections",
		Subtitle: "Live client connections to the embedded NATS server.",
		Tiles:    tiles,
	})
	if err != nil {
		return PageHeader{Title: "Connections"}
	}
	return header
}
