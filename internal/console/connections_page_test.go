// connections_page_test.go exercises the /console/connections page, the
// pure connRowFrom mapper, and the adapter's ListConnections degrade
// paths without standing up a real listening NATS server.
//
// Methodology:
//   - The page tests reuse the fakeDataSource + mountWithFake helpers
//     from pages_test.go. Seeding fake.connections drives the render so
//     the table layout gets coverage without an embedded server's
//     Connz(). Assertions look for stable substrings the template emits
//     (positive space) and confirm a never-seeded name is absent
//     (negative space).
//   - connRowFrom is pure over a *natsserver.ConnInfo; its test asserts
//     the field mapping directly and the nil panic-guard.
//   - ListConnections is exercised through a tiny fake NATSServerStats so
//     the adapter's Connz fold and its nil-stats degrade get covered
//     without a live server.
//   - Each page test creates its own console.Mount with the fake; tests
//     never share state.
package console

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

func TestServePageConnections_rendersRows(t *testing.T) {
	fake := newFakeDS()
	fake.connections = []ConnRow{
		{
			CID: 7, Name: "dagnats-engine", Kind: "Client", Lang: "go",
			Version: "1.50.0", RTT: "42µs", Uptime: "4h", Idle: "0s",
			Subs: 12, PendingBytes: 0, InMsgs: 1200, OutMsgs: 1400,
		},
		{
			CID: 22, Name: "", Kind: "Client", Lang: "go",
			RTT: "120µs", Uptime: "8s", Idle: "2s", Subs: 1,
		},
	}
	handler := mountWithFake(t, fake)

	req := httptest.NewRequest(http.MethodGet, "/console/connections", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"dagnats-engine", ">7<", "go"} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q", want)
		}
	}
	// The unnamed connection (CID 22) renders a fallback glyph plus its CID.
	if !strings.Contains(body, ">22<") {
		t.Errorf("body missing the unnamed connection's CID")
	}
	if !strings.Contains(body, "—") {
		t.Errorf("body missing the empty-name fallback glyph")
	}
	// Negative space: a client name we never seeded must not appear.
	if strings.Contains(body, "phantom-client") {
		t.Errorf("body unexpectedly contains a fabricated client name")
	}
}

func TestServePageConnections_emptyState(t *testing.T) {
	fake := newFakeDS()
	fake.connections = nil
	handler := mountWithFake(t, fake)

	req := httptest.NewRequest(http.MethodGet, "/console/connections", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "no client connections") {
		t.Errorf("body missing the empty-state copy")
	}
	if strings.Contains(body, "dagnats-engine") {
		t.Errorf("empty state fabricated a connection row")
	}
}

func TestConnRowFrom_mapsFields(t *testing.T) {
	info := &natsserver.ConnInfo{
		Cid: 7, Name: "x", Lang: "go", Version: "1.50.0", RTT: "42µs",
		Uptime: "4h", Idle: "0s", NumSubs: 12, Pending: 2048,
		InMsgs: 10, OutMsgs: 20, Kind: "Client",
	}
	row := connRowFrom(info)
	if row.CID != 7 {
		t.Errorf("CID: got %d, want 7", row.CID)
	}
	if row.Name != "x" {
		t.Errorf("Name: got %q, want x", row.Name)
	}
	if row.Kind != "Client" {
		t.Errorf("Kind: got %q, want Client", row.Kind)
	}
	if row.Lang != "go" || row.Version != "1.50.0" {
		t.Errorf("Lang/Version: got %q/%q", row.Lang, row.Version)
	}
	if row.RTT != "42µs" || row.Uptime != "4h" || row.Idle != "0s" {
		t.Errorf("RTT/Uptime/Idle: got %q/%q/%q", row.RTT, row.Uptime, row.Idle)
	}
	if row.Subs != 12 {
		t.Errorf("Subs: got %d, want 12", row.Subs)
	}
	if row.PendingBytes != 2048 {
		t.Errorf("PendingBytes: got %d, want 2048", row.PendingBytes)
	}
	if row.InMsgs != 10 || row.OutMsgs != 20 {
		t.Errorf("InMsgs/OutMsgs: got %d/%d", row.InMsgs, row.OutMsgs)
	}
}

func TestConnRowFrom_nilPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Errorf("connRowFrom(nil) did not panic")
		}
	}()
	_ = connRowFrom(nil)
}

// fakeServerStats is a tiny NATSServerStats that returns canned Connz /
// Varz / Jsz snapshots so the adapter's folds can be exercised without a
// live server. varz/jsz are nil unless a test seeds them.
type fakeServerStats struct {
	cz   *natsserver.Connz
	err  error
	varz *natsserver.Varz
	jsz  *natsserver.JSInfo
}

func (f fakeServerStats) Connz(*natsserver.ConnzOptions) (*natsserver.Connz, error) {
	return f.cz, f.err
}

func (f fakeServerStats) Varz(*natsserver.VarzOptions) (*natsserver.Varz, error) {
	return f.varz, nil
}

func (f fakeServerStats) Jsz(*natsserver.JSzOptions) (*natsserver.JSInfo, error) {
	return f.jsz, nil
}

func TestAdapterListConnections_viaFakeStats(t *testing.T) {
	stats := fakeServerStats{cz: &natsserver.Connz{
		NumConns: 1,
		Conns: []*natsserver.ConnInfo{
			{Cid: 7, Name: "x", NumSubs: 3},
		},
	}}
	// Build the adapter directly (in-package) so the fold can be tested
	// without standing up a real api.Service / NATS server.
	ds := WithServerStats(&apiServiceAdapter{}, stats)
	rows, err := ds.ListConnections(context.Background())
	if err != nil {
		t.Fatalf("ListConnections: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}
	if rows[0].CID != 7 {
		t.Errorf("CID: got %d, want 7", rows[0].CID)
	}

	// A nil-stats adapter degrades to (nil, nil) rather than panicking.
	bare := &apiServiceAdapter{}
	got, err := bare.ListConnections(context.Background())
	if err != nil {
		t.Fatalf("nil-stats ListConnections err: %v", err)
	}
	if got != nil {
		t.Errorf("nil-stats ListConnections: got %v, want nil", got)
	}
}

func TestAdapterServerHealth_viaFakeStats(t *testing.T) {
	// With a stats handle present, ServerHealth reads the REAL ceiling and
	// traffic from Varz/Jsz: HasStats is true, StoreMax comes from
	// Jsz.Config.MaxStore (not the unlimited account tier), and uptime
	// maps off Varz.
	stats := fakeServerStats{
		varz: &natsserver.Varz{
			Version: "2.12.6", Uptime: "4h12m", Connections: 7,
			Subscriptions: 128, SlowConsumers: 0, Mem: 180 << 20,
			CPU: 3.4, Cores: 8,
		},
		jsz: &natsserver.JSInfo{
			Streams:   5,
			Consumers: 6,
			Config:    natsserver.JetStreamConfig{MaxStore: 10 << 30, MaxMemory: 1 << 30},
		},
	}
	stats.jsz.Store = 2 << 30
	stats.jsz.Memory = 100 << 20

	ds := WithServerStats(&apiServiceAdapter{}, stats)
	a := ds.(*apiServiceAdapter)
	h, err := a.ServerHealth(context.Background())
	if err != nil {
		t.Fatalf("ServerHealth: %v", err)
	}
	if !h.HasStats {
		t.Errorf("HasStats: got false, want true")
	}
	if h.Uptime != "4h12m" {
		t.Errorf("Uptime: got %q, want 4h12m", h.Uptime)
	}
	if h.Connections != 7 {
		t.Errorf("Connections: got %d, want 7", h.Connections)
	}
	if h.StoreMax != 10<<30 {
		t.Errorf("StoreMax: got %d, want %d (Jsz ceiling)", h.StoreMax, int64(10<<30))
	}
	if h.StoreUsed != 2<<30 {
		t.Errorf("StoreUsed: got %d, want %d", h.StoreUsed, uint64(2<<30))
	}
	if h.StorePct != 20 {
		t.Errorf("StorePct: got %d, want 20", h.StorePct)
	}
	if h.Cores != 8 {
		t.Errorf("Cores: got %d, want 8", h.Cores)
	}
}
