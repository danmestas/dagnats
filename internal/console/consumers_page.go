package console

import "net/http"

// ConsumersListView powers /console/consumers. Unlike the streams page,
// this one reads live JetStream consumer state through the DataSource —
// Note carries a read-failure explanation when the read degrades, empty
// otherwise.
type ConsumersListView struct {
	Header    PageHeader
	Consumers []ConsumerRow
	Note      string
}

// servePageConsumers renders /console/consumers. A DataSource read
// failure degrades to an empty list + an explanatory Note rather than a
// 500, mirroring how the streams / config pages survive a JetStream
// outage — operators see the page shell, not an error screen.
func servePageConsumers(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageConsumers: w is nil")
	}
	if r == nil {
		panic("servePageConsumers: r is nil")
	}
	data, ok := requirePort[OpsInventory](w, cfg, "list consumers")
	if !ok {
		return
	}
	rows, err := data.ListConsumers(r.Context())
	note := ""
	if err != nil {
		cfg.Logger.Error("console: list consumers", "err", err)
		rows = nil
		note = "Consumer metadata could not be read from JetStream right " +
			"now; the list is empty until the read succeeds."
	}
	view := ConsumersListView{
		Header:    buildConsumersHeader(rows),
		Consumers: rows,
		Note:      note,
	}
	renderPage(w, r, ts, cfg, "consumers-list", pageData{
		Title:   "Consumers",
		Section: "consumers",
		Page:    view,
	})
}

// buildConsumersHeader assembles the count strip for the consumers page:
// total consumers, total pending across all consumers, the worst lag,
// and a stalled count that flips to a danger tone when any consumer has
// a backlog with no waiting pulls.
func buildConsumersHeader(rows []ConsumerRow) PageHeader {
	var pendingTotal, maxLag uint64
	var stalled int
	for i := range rows {
		pendingTotal += rows[i].NumPending
		if rows[i].Lag > maxLag {
			maxLag = rows[i].Lag
		}
		if rows[i].Stalled {
			stalled++
		}
	}
	stalledTone := ToneDefault
	if stalled > 0 {
		stalledTone = ToneDanger
	}
	tiles := []Tile{
		{Label: "consumers", Count: len(rows), Tone: ToneInfo,
			Tooltip: "Durable JetStream consumers on the engine's streams"},
		{Label: "pending", Count: int(pendingTotal), Tone: ToneDefault,
			Tooltip: "Messages matched but not yet delivered, summed"},
		{Label: "max lag", Count: int(maxLag), Tone: ToneDefault,
			Tooltip: "Largest delivered − ack-floor gap across consumers"},
		{Label: "stalled", Count: stalled, Tone: stalledTone,
			Tooltip: "Consumers with pending work and no waiting pulls"},
	}
	h, err := NewPageHeader(PageHeader{
		Title:    "Consumers",
		Subtitle: "Durable JetStream consumers backing the engine.",
		Tiles:    tiles,
	})
	if err != nil {
		return PageHeader{Title: "Consumers"}
	}
	return h
}
