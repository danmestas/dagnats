// natsapi_relay_fuzz_test.go
// No program request can take a node down (#905 review): every relay
// and program-scoped op is sent empty, null, {}, malformed and boundary
// bodies, with missing, empty and metacharacter-laden headers, and the
// server must answer each one and keep answering afterwards. Before the
// boundary checks and the relay's recover, an inputs transition with
// revision 0 or request id "a.b" reached an inputs.Store assertion in a
// bare goroutine and crashed the process.
// Methodology: one embedded server, orchestrator and NATSAPI; a real
// dispatched step supplies valid run, step and nonce values so the
// combinations reach past the ownership checks into the stores.
package api

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestRelayOpsSurviveHostileRequests(t *testing.T) {
	nc := startScopedAPI(t)
	runID, nonce := dispatchedStep(t, nc)
	// A real submission on the step, so an inputs op reaches the store's
	// transition and claim, whose assertions a revision 0 would trip.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := inputs.Open(ctx, js)
	if err != nil {
		t.Fatalf("inputs.Open: %v", err)
	}
	if _, _, err := store.Submit(ctx, inputs.Submission{
		RunID: runID, StepID: "work", RequestID: "r1", Mode: inputs.ModeSteer,
		Content: json.RawMessage(`"hi"`),
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	subjects := []string{
		"events", "journal.append", "journal.view", "checkpoint", "signal", "inputs",
		"workflows.register", "runs.start", "runs.get", "runs.cancel",
	}
	ops := []string{"", "get", "put", "delete", "list", "transition", "claim", "bogus"}
	bodies := []string{
		"", "{}", "null", "not json", `"str"`, `[]`,
		`{"entry":{"run_id":"` + runID + `","step_id":"work","request_id":"a.b"},"revision":0}`,
		`{"entry":{"run_id":"` + runID + `","step_id":"work","request_id":""},"revision":0,` +
			`"status":"bogus","iteration":-1}`,
		`{"entry":{"run_id":"` + runID + `","step_id":"work","request_id":"r1"},"revision":7,` +
			`"status":"done","iteration":0}`,
		`{"entry":{"run_id":"` + runID + `","step_id":"work","request_id":"r1"},"revision":0,` +
			`"status":"done","iteration":0}`,
		`{"entry":{"run_id":"` + runID + `","step_id":"work","request_id":"r1"},"revision":1,` +
			`"status":"bogus","iteration":-5}`,
		`{"request_id":"*"}`, `{"request_id":">"}`,
	}
	ids := [][3]string{
		{runID, "work", nonce}, {"", "", ""}, {"a.b", "c.d", "x"}, {"*", ">", nonce},
		{runID, "work", ""}, {runID, "nope", nonce},
	}
	for _, subject := range subjects {
		for _, op := range ops {
			for _, id := range ids {
				for _, body := range bodies {
					header := nats.Header{}
					header.Set(natsutil.RelayOpHeader, op)
					header.Set(natsutil.RelayRunHeader, id[0])
					header.Set(natsutil.RelayStepHeader, id[1])
					header.Set(journal.HeaderNonce, id[2])
					header.Set(natsutil.RelayKeyHeader, "notify."+id[1])
					header.Set(natsutil.RelaySubjectHeader, "journal."+id[0]+"."+id[1])
					header.Set(natsutil.RelayFromSeqHeader, "x")
					msg := &nats.Msg{Subject: "api.p.notify." + subject, Header: header,
						Data: []byte(body)}
					if _, err := programConn(t, nc, "notify").RequestMsg(msg, 2*time.Second); err != nil {
						t.Fatalf("%s op=%q ids=%v body=%q: no answer: %v",
							subject, op, id, body, err)
					}
				}
			}
		}
	}
	if _, err := programConn(t, nc, "notify").Request(
		"api.p.notify.runs.get", []byte(runID), 2*time.Second); err != nil {
		t.Fatalf("the API stopped answering: %v", err)
	}
}
