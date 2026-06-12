// concurrency_page_test.go exercises the /console/concurrency page and the
// pure admission-state parsers/assembler without standing up NATS.
//
// Methodology:
//   - The page tests reuse the fakeDataSource + mountWithFake helpers from
//     pages_test.go. Seeding fake.admission drives the render so the locks /
//     task-slots layout gets coverage without a live nats.Conn. Assertions
//     look for stable substrings the template emits (positive space) and
//     confirm the empty state never fabricates a row (negative space).
//   - parseLockRunID / parseCounterValue / lockScopeOf / buildAdmissionState
//     are pure; their tests assert behaviour directly, including that a
//     malformed KV value is skipped rather than panicking or failing the
//     whole assembly.
//   - Each page test creates its own console.Mount with the fake; tests
//     never share state.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServePageConcurrency_rendersGates(t *testing.T) {
	fake := newFakeDS()
	fake.admission = AdmissionState{
		Locks: []LockRow{
			{Key: "nightly-report", Scope: "workflow", HeldBy: "4f1abc02"},
			{Key: "reindex.tenant-acme", Scope: "keyed", HeldBy: "c19f44de"},
		},
		TaskSlots: []SlotRow{
			{Name: "image-pipeline::fetch", InFlight: 3},
		},
		RateLimits: []RateLimitRow{
			{Key: "image-pipeline::fetch", Tokens: 0, Limit: 20, Period: "1m0s"},
		},
		Debouncers: []DebounceRow{
			{Trigger: "trg-photos-in", TimerSeq: 3},
		},
	}
	handler := mountWithFake(t, fake)

	req := httptest.NewRequest(http.MethodGet, "/console/concurrency", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"nightly-report", "4f1abc02", "reindex.tenant-acme", "c19f44de",
		"image-pipeline::fetch", ">3<",
		"trg-photos-in", ">20<",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q", want)
		}
	}
	// Negative space: a run id we never seeded must not appear.
	if strings.Contains(body, "deadbeef") {
		t.Errorf("body unexpectedly contains a fabricated run id")
	}
}

func TestServePageConcurrency_emptyState(t *testing.T) {
	fake := newFakeDS()
	fake.admission = AdmissionState{}
	handler := mountWithFake(t, fake)

	req := httptest.NewRequest(http.MethodGet, "/console/concurrency", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"No singleton locks held",
		"No task-concurrency contention",
		"No rate limiters active",
		"No open debounce windows",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("empty-state body missing %q", want)
		}
	}
}

func TestParseLockRunID(t *testing.T) {
	got, err := parseLockRunID([]byte(`{"run_id":"abc"}`))
	if err != nil {
		t.Fatalf("parseLockRunID: unexpected error %v", err)
	}
	if got != "abc" {
		t.Errorf("parseLockRunID: got %q, want %q", got, "abc")
	}

	// Malformed JSON returns an error, never panics.
	if _, err := parseLockRunID([]byte(`{not json`)); err == nil {
		t.Errorf("parseLockRunID: want error on malformed JSON, got nil")
	}

	// Empty run_id is valid: empty string + nil error.
	empty, err := parseLockRunID([]byte(`{"run_id":""}`))
	if err != nil {
		t.Fatalf("parseLockRunID(empty): unexpected error %v", err)
	}
	if empty != "" {
		t.Errorf("parseLockRunID(empty): got %q, want empty", empty)
	}
}

func TestParseCounterValue(t *testing.T) {
	if got, err := parseCounterValue([]byte("3")); err != nil || got != 3 {
		t.Errorf("parseCounterValue(3): got %d, err %v; want 3, nil", got, err)
	}
	if got, err := parseCounterValue([]byte("0")); err != nil || got != 0 {
		t.Errorf("parseCounterValue(0): got %d, err %v; want 0, nil", got, err)
	}
	if _, err := parseCounterValue([]byte("notanumber")); err == nil {
		t.Errorf("parseCounterValue(non-numeric): want error, got nil")
	}
}

func TestLockScopeOf(t *testing.T) {
	if got := lockScopeOf("nightly-report"); got != "workflow" {
		t.Errorf("lockScopeOf(plain): got %q, want workflow", got)
	}
	if got := lockScopeOf("reindex.tenant-acme"); got != "keyed" {
		t.Errorf("lockScopeOf(dotted): got %q, want keyed", got)
	}
}

func TestBuildAdmissionState(t *testing.T) {
	locks := map[string][]byte{
		"nightly-report":      []byte(`{"run_id":"4f1abc02"}`),
		"reindex.tenant-acme": []byte(`{"run_id":"c19f44de"}`),
		"broken-lock":         []byte(`{not json`), // malformed: skipped
	}
	counters := map[string][]byte{
		"task.image-pipeline::fetch": []byte("3"),
		"task.broken":                []byte("not-a-number"), // malformed: skipped
	}

	state := buildAdmissionState(
		locks, counters, map[string][]byte{}, map[string][]byte{},
	)

	if len(state.Locks) != 2 {
		t.Errorf("Locks: got %d rows, want 2 (malformed skipped)", len(state.Locks))
	}
	if len(state.TaskSlots) != 1 {
		t.Errorf("TaskSlots: got %d rows, want 1 (malformed skipped)", len(state.TaskSlots))
	}

	// Positive space: the well-formed rows carry the parsed values.
	var foundKeyed bool
	for _, l := range state.Locks {
		if l.Key == "reindex.tenant-acme" {
			foundKeyed = true
			if l.Scope != "keyed" {
				t.Errorf("keyed lock scope: got %q, want keyed", l.Scope)
			}
			if l.HeldBy != "c19f44de" {
				t.Errorf("keyed lock held_by: got %q, want c19f44de", l.HeldBy)
			}
		}
		// Negative space: the malformed lock never becomes a row.
		if l.Key == "broken-lock" {
			t.Errorf("malformed lock must be skipped, not rendered")
		}
	}
	if !foundKeyed {
		t.Errorf("keyed lock row missing from assembled state")
	}

	slot := state.TaskSlots[0]
	if slot.Name != "image-pipeline::fetch" {
		t.Errorf("slot name: got %q, want image-pipeline::fetch (task. stripped)", slot.Name)
	}
	if slot.InFlight != 3 {
		t.Errorf("slot in-flight: got %d, want 3", slot.InFlight)
	}
}

func TestParseRateLimit(t *testing.T) {
	tokens, limit, period, err := parseRateLimit(
		[]byte(`{"tokens":0,"limit":20,"period_ns":60000000000}`),
	)
	if err != nil {
		t.Fatalf("parseRateLimit: unexpected error %v", err)
	}
	if tokens != 0 {
		t.Errorf("tokens: got %d, want 0", tokens)
	}
	if limit != 20 {
		t.Errorf("limit: got %d, want 20", limit)
	}
	if period != "1m0s" {
		t.Errorf("period: got %q, want %q", period, "1m0s")
	}

	// Malformed JSON returns an error, never panics.
	if _, _, _, err := parseRateLimit([]byte(`{not json`)); err == nil {
		t.Errorf("parseRateLimit: want error on malformed JSON, got nil")
	}
}

func TestParseDebounce(t *testing.T) {
	seq, err := parseDebounce([]byte(`{"first_seen_ns":123,"timer_seq":7}`))
	if err != nil {
		t.Fatalf("parseDebounce: unexpected error %v", err)
	}
	if seq != 7 {
		t.Errorf("timer_seq: got %d, want 7", seq)
	}

	// Malformed JSON returns an error, never panics.
	if _, err := parseDebounce([]byte(`{not json`)); err == nil {
		t.Errorf("parseDebounce: want error on malformed JSON, got nil")
	}
}

func TestBuildAdmissionState_gates(t *testing.T) {
	rateLimits := map[string][]byte{
		"image-pipeline::fetch._global": []byte(
			`{"tokens":0,"limit":20,"period_ns":60000000000}`),
		"broken": []byte(`{not json`), // malformed: skipped
	}
	debouncers := map[string][]byte{
		"trg-photos-in": []byte(`{"first_seen_ns":1,"timer_seq":3}`),
		"broken":        []byte(`{not json`), // malformed: skipped
	}

	state := buildAdmissionState(
		map[string][]byte{}, map[string][]byte{}, rateLimits, debouncers,
	)

	if len(state.RateLimits) != 1 {
		t.Errorf("RateLimits: got %d rows, want 1 (malformed skipped)",
			len(state.RateLimits))
	}
	if len(state.Debouncers) != 1 {
		t.Errorf("Debouncers: got %d rows, want 1 (malformed skipped)",
			len(state.Debouncers))
	}

	rl := state.RateLimits[0]
	if rl.Key != "image-pipeline::fetch._global" {
		t.Errorf("rate-limit key: got %q, want image-pipeline::fetch._global", rl.Key)
	}
	if rl.Tokens != 0 || rl.Limit != 20 || rl.Period != "1m0s" {
		t.Errorf("rate-limit fields: got tokens=%d limit=%d period=%q",
			rl.Tokens, rl.Limit, rl.Period)
	}

	db := state.Debouncers[0]
	if db.Trigger != "trg-photos-in" {
		t.Errorf("debounce trigger: got %q, want trg-photos-in", db.Trigger)
	}
	if db.TimerSeq != 3 {
		t.Errorf("debounce timer_seq: got %d, want 3", db.TimerSeq)
	}
}
