// cli/trigger_type_test.go
// Tests for the `dagnats trigger-type` command (parent #273 Phase 2.5,
// #343).
//
// Methodology:
//   - Pure-function unit tests on printTriggerTypesTable cover the
//     four-state render matrix (empty / single live / single stale /
//     multiple mixed) as golden strings so a table-shape regression
//     fails loudly rather than silently shifting column order.
//   - JSON-mode unit tests guarantee the round-trip carries the
//     computed owner_status field and the underlying TriggerTypeDef
//     fields without re-encoding the schema bytes.
//   - End-to-end tests stand up an embedded NATS + real engine
//     TriggerService and drive `trigger-type list` / `describe`
//     against the same wire the production CLI uses; this exercises
//     env-var → NATS connect → KV read → table/detail render in one
//     hop, matching the cli/service.go shape called out in #343.
package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dagnatsext"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/worker"
)

// liveSet is a small helper so test bodies stay readable. The
// underlying type is the exact set shape ownerStatus expects.
func liveSet(ids ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		out[id] = struct{}{}
	}
	return out
}

// schemaBytes returns the canonical empty-object schema used by the
// table tests — exercising the OwnerStatus column shape, not the
// schema renderer.
var schemaBytes = json.RawMessage(`{"type":"object"}`)

// TestPrintTriggerTypesTable_SingleLive verifies the single-entry,
// owner-live render. Golden body asserts exact column layout —
// tabwriter trailing spaces matter.
func TestPrintTriggerTypesTable_SingleLive(t *testing.T) {
	defs := []trigger.TriggerTypeDef{
		{
			Name:          "fs.watch",
			OwnerWorkerID: "worker-1",
			Version:       "1.0.0",
			ConfigSchema:  schemaBytes,
			RegisteredAt:  time.Unix(1700000000, 0).UTC(),
		},
	}
	var buf bytes.Buffer
	printTriggerTypesTable(&buf, defs, liveSet("worker-1"))

	got := buf.String()
	// Positive: header row contains all four columns in order.
	if !strings.Contains(got,
		"NAME      OWNER_WORKER_ID  VERSION  OWNER_STATUS") {
		t.Errorf("header row not as expected; got:\n%s", got)
	}
	// Positive: data row contains live status.
	if !strings.Contains(got, "fs.watch  worker-1         1.0.0    live") {
		t.Errorf("live row not as expected; got:\n%s", got)
	}
	// Negative: no stray "stale" leakage.
	if strings.Contains(got, "stale") {
		t.Errorf("unexpected 'stale' in live render; got:\n%s", got)
	}
}

// TestPrintTriggerTypesTable_SingleStale verifies the single-entry,
// owner-not-in-live-set render.
func TestPrintTriggerTypesTable_SingleStale(t *testing.T) {
	defs := []trigger.TriggerTypeDef{
		{
			Name:          "fs.watch",
			OwnerWorkerID: "worker-gone",
			Version:       "1.0.0",
			ConfigSchema:  schemaBytes,
			RegisteredAt:  time.Unix(1700000000, 0).UTC(),
		},
	}
	var buf bytes.Buffer
	// Live set is empty — the owner cannot be live.
	printTriggerTypesTable(&buf, defs, liveSet())

	got := buf.String()
	// Positive: data row marked stale.
	if !strings.Contains(got, "fs.watch  worker-gone      1.0.0    stale") {
		t.Errorf("stale row not as expected; got:\n%s", got)
	}
	// Negative: must not falsely report live when owner is absent
	// from the live set. The unanchored "live" substring is a prefix
	// of "live\n" but isn't itself present (status is "stale"), so
	// look for the column-followed-by-newline pattern.
	if strings.Contains(got, "    live\n") {
		t.Errorf("unexpected 'live' in stale render; got:\n%s", got)
	}
}

// TestPrintTriggerTypesTable_MultipleMixed verifies a mixed render
// where one owner is live and another has aged out. This is the
// shape an operator hits in production with a single trigger-type
// fleet across multiple workers.
func TestPrintTriggerTypesTable_MultipleMixed(t *testing.T) {
	defs := []trigger.TriggerTypeDef{
		{
			Name:          "fs.watch",
			OwnerWorkerID: "worker-alive",
			Version:       "1.0.0",
			ConfigSchema:  schemaBytes,
		},
		{
			Name:          "http.poll",
			OwnerWorkerID: "worker-dead",
			Version:       "2.1.3",
			ConfigSchema:  schemaBytes,
		},
	}
	var buf bytes.Buffer
	printTriggerTypesTable(&buf, defs, liveSet("worker-alive"))

	got := buf.String()
	// Positive: row 1 marked live.
	if !strings.Contains(got, "fs.watch") ||
		!strings.Contains(got, "worker-alive") ||
		!strings.Contains(got, "live") {
		t.Errorf("missing live row content; got:\n%s", got)
	}
	// Positive: row 2 marked stale.
	if !strings.Contains(got, "http.poll") ||
		!strings.Contains(got, "worker-dead") ||
		!strings.Contains(got, "stale") {
		t.Errorf("missing stale row content; got:\n%s", got)
	}
	// Negative: row ordering must match defs order (no resort).
	if strings.Index(got, "fs.watch") >
		strings.Index(got, "http.poll") {
		t.Errorf("row order changed; got:\n%s", got)
	}
}

// TestPrintTriggerTypesTable_EmptyPanics codifies the empty-input
// contract: the renderer panics on empty input. Callers must check
// len() and emit the "No trigger types registered." sentinel —
// otherwise we'd silently print a bare header row.
func TestPrintTriggerTypesTable_EmptyPanics(t *testing.T) {
	defer func() {
		// Positive: a panic must have been raised.
		if r := recover(); r == nil {
			t.Fatal(
				"expected panic on empty defs slice")
		}
	}()
	var buf bytes.Buffer
	printTriggerTypesTable(&buf, []trigger.TriggerTypeDef{}, liveSet())
}

// TestOwnerStatus covers the three input states for the
// set-membership check: live, stale via missing ID, stale via empty
// ownerID. The third path catches a class of "trigger type with no
// owner is silently live because empty string matches" bug — we
// treat unowned as stale by definition.
func TestOwnerStatus(t *testing.T) {
	// Positive: present in live set → live.
	if got := ownerStatus("w-1", liveSet("w-1", "w-2")); got != "live" {
		t.Errorf("ownerStatus(present)=%q want live", got)
	}
	// Positive: absent from live set → stale.
	if got := ownerStatus("w-1", liveSet()); got != "stale" {
		t.Errorf("ownerStatus(absent)=%q want stale", got)
	}
	// Negative: empty ownerID never resolves to live, even if the
	// live set has an empty-string entry (defensive against future
	// data shapes where empty WorkerID could leak through).
	if got := ownerStatus("", liveSet("")); got != "stale" {
		t.Errorf("ownerStatus(empty)=%q want stale", got)
	}
}

// TestRenderTriggerTypesJSON_Roundtrip verifies the JSON mode wraps
// every def with its owner_status and round-trips back into a
// matching shape — proving the embedded TriggerTypeDef fields are
// not double-encoded or shadowed.
func TestRenderTriggerTypesJSON_Roundtrip(t *testing.T) {
	defs := []trigger.TriggerTypeDef{
		{
			Name:          "fs.watch",
			OwnerWorkerID: "w-1",
			Version:       "1.0.0",
			ConfigSchema:  schemaBytes,
			RegisteredAt:  time.Unix(1700000000, 0).UTC(),
		},
	}
	rendered := renderTriggerTypesJSON(defs, liveSet("w-1"))

	data, err := json.Marshal(rendered)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// Positive: serialized JSON includes the computed owner_status.
	if !strings.Contains(string(data), `"owner_status":"live"`) {
		t.Errorf(
			"owner_status field missing or wrong; got:\n%s",
			data,
		)
	}
	// Positive: embedded name field round-trips.
	if !strings.Contains(string(data), `"name":"fs.watch"`) {
		t.Errorf(
			"name field missing in roundtrip; got:\n%s", data,
		)
	}
	// Negative: nil input still yields an empty slice (`[]`) — not
	// `null` — so consumers can unconditionally iterate.
	empty := renderTriggerTypesJSON(nil, liveSet())
	emptyData, err := json.Marshal(empty)
	if err != nil {
		t.Fatalf("Marshal(empty): %v", err)
	}
	if string(emptyData) != "[]" {
		t.Errorf(
			"empty render should be [] not %s", emptyData,
		)
	}
}

// TestRunTriggerTypeCmd_UnknownSubcommand verifies an unknown
// subcommand calls exitFunc(1). Mirrors the service.go contract so
// the two commands behave identically when misused.
func TestRunTriggerTypeCmd_UnknownSubcommand(t *testing.T) {
	called := false
	code := 0
	restore := setExitFunc(func(c int) {
		called = true
		code = c
	})
	defer restore()

	runTriggerTypeCmd([]string{"nope"})

	if !called {
		t.Fatal("exitFunc was not called on unknown subcommand")
	}
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

// startTriggerTypeHarness boots an embedded NATS server, provisions
// the dagnats stream/KV set, starts the engine TriggerService (so
// the worker's RegisterTriggerType ack roundtrip resolves), and
// returns a worker plus the server URL the CLI tests point at.
//
// The triggers and trigger_state buckets are explicitly requested
// because SetupAll's defaults only provision trigger_types; the
// engine TriggerService refuses to start without all three.
func startTriggerTypeHarness(
	t *testing.T,
) (*worker.Worker, string) {
	t.Helper()
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc, err := trigger.NewTriggerService(nc)
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("svc.Start: %v", err)
	}
	t.Cleanup(svc.Stop)

	w := worker.NewWorker(nc)
	t.Cleanup(w.Stop)
	return w, srv.ClientURL()
}

// TestRunTriggerTypeListCmd_EndToEnd registers a trigger type via
// the real worker SDK (#337) and asserts the CLI list path renders
// the row with all four columns populated. Drives env-var → NATS
// connect → KV read → directory read → table render.
func TestRunTriggerTypeListCmd_EndToEnd(t *testing.T) {
	w, srvURL := startTriggerTypeHarness(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 2*time.Second)
	defer cancel()
	def := dagnatsext.TriggerTypeDef{
		Name:         "fs.watch",
		Description:  "Filesystem watcher",
		ConfigSchema: schemaBytes,
		Version:      "1.0.0",
	}
	if err := w.RegisterTriggerType(ctx, def); err != nil {
		t.Fatalf("RegisterTriggerType: %v", err)
	}

	t.Setenv("DAGNATS_NATS_URL", srvURL)
	t.Setenv("NATS_URL", srvURL)

	output := captureStdout(t, func() {
		runTriggerTypeListCmd([]string{})
	})

	// Positive: trigger type name present.
	if !strings.Contains(output, "fs.watch") {
		t.Errorf(
			"output missing fs.watch; got:\n%s", output,
		)
	}
	// Positive: version column populated.
	if !strings.Contains(output, "1.0.0") {
		t.Errorf(
			"output missing version 1.0.0; got:\n%s", output,
		)
	}
	// Negative: must not say "no trigger types" when one is
	// registered.
	if strings.Contains(strings.ToLower(output),
		"no trigger types") {
		t.Errorf(
			"unexpected empty-state message; got:\n%s", output,
		)
	}
}

// TestRunTriggerTypeListCmd_EmptyBucket verifies the empty-state
// message — the bucket exists (SetupAll provisions it) but contains
// no keys.
func TestRunTriggerTypeListCmd_EmptyBucket(t *testing.T) {
	_, srvURL := startTriggerTypeHarness(t)

	t.Setenv("DAGNATS_NATS_URL", srvURL)
	t.Setenv("NATS_URL", srvURL)

	output := captureStdout(t, func() {
		runTriggerTypeListCmd([]string{})
	})

	// Positive: empty-state message present.
	if !strings.Contains(output, "No trigger types registered") {
		t.Errorf(
			"missing empty-state message; got:\n%s", output,
		)
	}
	// Negative: no table header on the empty bucket.
	if strings.Contains(output, "NAME") {
		t.Errorf(
			"unexpected table header for empty bucket: %s",
			output,
		)
	}
}

// TestRunTriggerTypeListCmd_JSON verifies the --json path emits the
// owner_status field and the underlying TriggerTypeDef name field.
func TestRunTriggerTypeListCmd_JSON(t *testing.T) {
	w, srvURL := startTriggerTypeHarness(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 2*time.Second)
	defer cancel()
	if err := w.RegisterTriggerType(ctx, dagnatsext.TriggerTypeDef{
		Name:         "fs.watch",
		Description:  "Filesystem watcher",
		ConfigSchema: schemaBytes,
		Version:      "1.0.0",
	}); err != nil {
		t.Fatalf("RegisterTriggerType: %v", err)
	}

	t.Setenv("DAGNATS_NATS_URL", srvURL)
	t.Setenv("NATS_URL", srvURL)

	output := captureStdout(t, func() {
		runTriggerTypeListCmd([]string{"--json"})
	})

	// Positive: JSON includes the trigger type name (snake_case tag).
	if !strings.Contains(output, `"name": "fs.watch"`) {
		t.Errorf("JSON missing name field; got:\n%s", output)
	}
	// Positive: JSON includes the computed owner_status.
	if !strings.Contains(output, `"owner_status":`) {
		t.Errorf(
			"JSON missing owner_status field; got:\n%s", output,
		)
	}
	// Negative: tabwriter header must not appear in JSON output.
	if strings.Contains(output, "NAME\t") {
		t.Errorf(
			"JSON output contained tabwriter header: %s", output,
		)
	}
}

// TestRunTriggerTypeDescribeCmd_EndToEnd registers a trigger type
// and confirms describe prints every field — including the indented
// schema body, which is the operator's primary use for this command.
func TestRunTriggerTypeDescribeCmd_EndToEnd(t *testing.T) {
	w, srvURL := startTriggerTypeHarness(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 2*time.Second)
	defer cancel()
	def := dagnatsext.TriggerTypeDef{
		Name:        "fs.watch",
		Description: "Filesystem watcher",
		ConfigSchema: json.RawMessage(
			`{"type":"object","required":["path"]}`),
		PayloadSchema: json.RawMessage(`{"type":"string"}`),
		Version:       "1.0.0",
	}
	if err := w.RegisterTriggerType(ctx, def); err != nil {
		t.Fatalf("RegisterTriggerType: %v", err)
	}

	t.Setenv("DAGNATS_NATS_URL", srvURL)
	t.Setenv("NATS_URL", srvURL)

	output := captureStdout(t, func() {
		runTriggerTypeDescribeCmd([]string{"fs.watch"})
	})

	// Positive: each field heading present.
	for _, want := range []string{
		"Name:",
		"Owner Worker ID:",
		"Version:",
		"Description:",
		"Registered At:",
		"Config Schema:",
		"Payload Schema:",
		"fs.watch",
		"Filesystem watcher",
		"1.0.0",
		`"required"`,
		`"path"`,
	} {
		if !strings.Contains(output, want) {
			t.Errorf(
				"missing %q in describe output; got:\n%s",
				want, output,
			)
		}
	}
	// Negative: schema must be re-indented, not printed as a single
	// JSON line — json.Indent inserts a newline between tokens.
	if !strings.Contains(output, "{\n") {
		t.Errorf(
			"schema not re-indented; got:\n%s", output,
		)
	}
}

// TestRunTriggerTypeDescribeCmd_NotFound verifies the clear error
// message + exit(1) when the requested name is absent from KV.
func TestRunTriggerTypeDescribeCmd_NotFound(t *testing.T) {
	_, srvURL := startTriggerTypeHarness(t)

	t.Setenv("DAGNATS_NATS_URL", srvURL)
	t.Setenv("NATS_URL", srvURL)

	called := false
	code := 0
	restore := setExitFunc(func(c int) {
		called = true
		code = c
	})
	defer restore()

	stderr := captureStderr(func() {
		runTriggerTypeDescribeCmd([]string{"does-not-exist"})
	})

	// Positive: exit hook fired with code 1.
	if !called {
		t.Fatal("exitFunc not called for missing trigger type")
	}
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	// Positive: error message names the missing trigger type.
	if !strings.Contains(stderr, "does-not-exist") {
		t.Errorf(
			"error message missing trigger name; got:\n%s",
			stderr,
		)
	}
	// Positive: error message phrased as a not-found surface so
	// operators don't confuse it with a wire-level failure.
	if !strings.Contains(stderr, "not found") {
		t.Errorf(
			"error message not phrased as not-found; got:\n%s",
			stderr,
		)
	}
}

// TestRunTriggerTypeDescribeCmd_MissingName verifies the usage
// guard when no name argument is provided.
func TestRunTriggerTypeDescribeCmd_MissingName(t *testing.T) {
	called := false
	restore := setExitFunc(func(int) { called = true })
	defer restore()

	stderr := captureStderr(func() {
		runTriggerTypeDescribeCmd([]string{})
	})

	if !called {
		t.Fatal("exitFunc not called for missing name argument")
	}
	if !strings.Contains(stderr, "requires a name") {
		t.Errorf(
			"missing 'requires a name' hint; got:\n%s", stderr,
		)
	}
}
