package console

import (
	"net/http"
	"strconv"
)

// services_page.go owns the /console/services roster page. It unions
// the `services` KV roster (worker/services.go) with LIVE $SRV.PING /
// $SRV.STATS discovery (#449 Phase 2a): the Version / Instances / Status
// columns are folded from real micro responders, and a service seen only
// via $SRV (e.g. dagnats-api, which does not self-register in KV) gets a
// synthesized row. HONESTY holds: a service with no STATS is "unknown"
// (never falsely "online"), and when discovery is unavailable every live
// column dashes rather than fabricating liveness.

// ServicesListView powers /console/services. Services is the roster read
// live from the `services` KV bucket. When zero services are registered
// the page paints an honest empty state.
type ServicesListView struct {
	Header   PageHeader
	Services []ServiceRow
}

// servePageServices renders /console/services off the `services` KV
// bucket. A read miss degrades to the empty state — the page is
// observational and never blocks.
func servePageServices(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageServices: w is nil")
	}
	if r == nil {
		panic("servePageServices: r is nil")
	}
	ds, ok := requirePort[WorkerDirectory](w, cfg, "services-list")
	if !ok {
		return
	}
	rows, _ := ds.ListServiceRows(r.Context())
	view := ServicesListView{
		Header:   buildServicesHeader(rows),
		Services: rows,
	}
	renderPage(w, r, ts, cfg, "services-list", pageData{
		Title:   "Services",
		Section: "services",
		Page:    view,
	})
}

// buildServicesHeader projects the roster into two honest count tiles:
// the service count and the live-instances total summed from the
// discovery-backed Instances column. The instances tile is now honest
// because Instances is real $SRV data — a dashed (unbacked) cell
// contributes nothing to the sum.
func buildServicesHeader(rows []ServiceRow) PageHeader {
	tiles := []Tile{
		{Label: "services", Count: len(rows), Tone: ToneDefault},
		{Label: "instances", Count: totalInstances(rows), Tone: ToneDefault},
	}
	h, err := NewPageHeader(PageHeader{
		Title: "Services",
		Subtitle: "Service roster unioned with live $SRV discovery. " +
			"Version, instances, and status are folded from real micro " +
			"responders; unbacked cells show a dash, not a guess.",
		Tiles: tiles,
	})
	if err != nil {
		return PageHeader{Title: "Services"}
	}
	return h
}

// totalInstances sums the numeric Instances cells across the roster.
// Dashed (discovery-unavailable / stale) cells are not numbers and so
// contribute nothing — the tile counts only live, attributable
// instances. Bounded by len(rows).
func totalInstances(rows []ServiceRow) int {
	const maxRows = 20000
	if len(rows) > maxRows {
		panic("totalInstances: rows exceeds max bound")
	}
	total := 0
	for _, row := range rows {
		n, err := strconv.Atoi(row.Instances)
		if err != nil {
			continue
		}
		total += n
	}
	return total
}
