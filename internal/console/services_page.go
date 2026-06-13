package console

import (
	"net/http"
)

// services_page.go owns the /console/services roster page. Services are
// a persistent metadata namespace (the `services` KV bucket, TTL=0, no
// heartbeat — worker/services.go), so this page is a pure read of that
// bucket projected into a roster table. It is deliberately NOT a
// liveness surface: there is no status pill, no instance count, and no
// per-service detail drill-in, because the registration carries none of
// that data and synthesizing it would lie. Per-endpoint $SRV.STATS
// arrive with nats-micro adoption.

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
	ds, ok := requireData(w, cfg, "services-list")
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

// buildServicesHeader projects the roster into a single honest count
// tile. There is no "instances" tile — the bucket carries no instance
// count, so fabricating one would lie. The subtitle states the honest
// constraint: services are a metadata namespace, not a heartbeat surface.
func buildServicesHeader(rows []ServiceRow) PageHeader {
	tiles := []Tile{
		{Label: "services", Count: len(rows), Tone: ToneDefault},
	}
	h, err := NewPageHeader(PageHeader{
		Title: "Services",
		Subtitle: "Registered service metadata. A persistent namespace, " +
			"not a heartbeat surface; per-endpoint $SRV stats arrive with " +
			"nats-micro adoption.",
		Tiles: tiles,
	})
	if err != nil {
		return PageHeader{Title: "Services"}
	}
	return h
}
