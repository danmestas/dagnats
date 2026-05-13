// e2e/features/http_concurrency_test.go
//
// Methodology: end-to-end stress for the per-run correlation
// invariant in ADR-013. The HTTP handler subscribes to a
// per-run subject (dagnats.http.response.<runID>) BEFORE
// publishing workflow.started. If any path of the correlation
// machinery crossed responses — e.g., shared map keyed by
// trigger id, or subject collision under concurrency — 50
// parallel requests with distinct payloads would observe at
// least one response that does not match its request.
//
// We register one workflow with a real worker that echoes
// the incoming HTTP body verbatim, then issue 50 parallel
// requests each carrying a distinct UUID-shaped token. Each
// response body must contain its own token and no other
// token. Bounded waits — every goroutine has a hard 30s
// per-request budget.
//
// CI runs this single test with `-count=100 -timeout 1800s`
// per the brief to prove zero flakes. The run-id generator
// it once exercised is internal/runid.New (crypto-random).
package features

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// echoHTTPBodyHandler unmarshals the trigger envelope, pulls the
// inner HTTP body bytes out of envelope.Data.Body, and emits them as
// the step output. The respond step that depends on this step then
// emits those bytes verbatim back to the HTTP caller. This is the
// shortest path from "request body in" to "response body out" the
// engine supports without making the respond step do JSON decode work.
func echoHTTPBodyHandler(tc worker.TaskContext) error {
	if tc == nil {
		panic("echoHTTPBodyHandler: tc must not be nil")
	}
	if tc.Input() == nil {
		return tc.Complete([]byte(`{}`))
	}
	var env trigger.TriggerEnvelope
	if err := json.Unmarshal(tc.Input(), &env); err != nil {
		return fmt.Errorf("unmarshal TriggerEnvelope: %w", err)
	}
	var http struct {
		Body []byte `json:"body"`
	}
	if len(env.Data) > 0 {
		if err := json.Unmarshal(env.Data, &http); err != nil {
			return fmt.Errorf("unmarshal HTTP envelope: %w", err)
		}
	}
	if len(http.Body) == 0 {
		return tc.Complete([]byte(`{}`))
	}
	return tc.Complete(http.Body)
}

// TestHTTPTrigger_Concurrency_NoCrossover validates the per-run
// correlation invariant: 50 parallel requests with distinct
// payloads all see their own payload back, none other's.
//
// Originally surfaced a runID-generation bug where
// internal/trigger/http.go composed run ids from workflowID +
// time.Now().UnixNano(), allowing two requests in the same
// nanosecond to collide. JetStream dedup then dropped one
// workflow.started and the surviving run's response delivered
// to both waiting HTTP handlers (cross-client data leak). Fixed
// by routing both internal/api and internal/trigger through
// internal/runid.New (16 crypto-random bytes -> 32 lowercase hex).
func TestHTTPTrigger_Concurrency_NoCrossover(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		stack := startHTTPE2EStack(t, nc)

		// Echo worker: parses the trigger envelope's HTTP body and
		// returns it as the step output. The respond step then
		// emits that body verbatim (BodyFrom unset). If correlation
		// crossed responses, the token observed by goroutine i
		// would not be the token i sent.
		harness.SubscribeWorker(t, nc, "echo-task",
			echoHTTPBodyHandler)

		wfName := harness.UniqueName(t, "crossover-wf")
		wfDef := dag.WorkflowDef{
			Name:    wfName,
			Version: "v1",
			Steps: []dag.StepDef{
				{ID: "echo", Task: "echo-task",
					Type: dag.StepTypeNormal},
				respondStepDef(t, "respond",
					[]string{"echo"},
					dag.RespondConfig{Status: 200}),
			},
		}
		_, path := stack.registerHTTPTrigger(t, wfDef,
			&trigger.HTTPConfig{
				Path:         "/" + harness.UniqueName(t, "cross"),
				Method:       http.MethodPost,
				TimeoutMs:    30_000,
				MaxBodyBytes: 4096,
			})

		runCrossoverBatch(t, stack, path, 50)
	})
}

// runCrossoverBatch issues N parallel requests with distinct
// tokens. Each goroutine asserts its own response contains its
// own token and only its own token. Split out so the test
// function stays under the 70-line limit.
func runCrossoverBatch(
	t *testing.T, stack *httpE2EStack, path string, n int,
) {
	t.Helper()
	if n <= 0 {
		panic("runCrossoverBatch: n must be positive")
	}
	tokens := make([]string, n)
	for i := 0; i < n; i++ {
		tokens[i] = newToken(t)
	}

	var wg sync.WaitGroup
	var crossovers atomic.Int32
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			body := []byte(`{"token":"` + tokens[i] + `"}`)
			rec := postOnRouter(t, stack.router,
				http.MethodPost, path, body, nil)
			if rec.Code != 200 {
				t.Errorf("req %d: status = %d body=%s",
					i, rec.Code, rec.Body)
				crossovers.Add(1)
				return
			}
			respBody := rec.Body.String()
			if !strings.Contains(respBody, tokens[i]) {
				t.Errorf("req %d: response %q lacks own token %q",
					i, respBody, tokens[i])
				crossovers.Add(1)
				return
			}
			assertNoForeignToken(t, i, respBody, tokens)
			if crossovers.Load() > 0 {
				return
			}
		}()
	}
	wg.Wait()
	if crossovers.Load() != 0 {
		t.Fatalf("crossovers detected: %d / %d", crossovers.Load(), n)
	}
}

// assertNoForeignToken fails the test if respBody contains any
// token other than its own. Iteration cap is the slice length —
// bounded by construction.
func assertNoForeignToken(
	t *testing.T, ownIdx int,
	respBody string, tokens []string,
) {
	t.Helper()
	for j, other := range tokens {
		if j == ownIdx {
			continue
		}
		if strings.Contains(respBody, other) {
			t.Errorf("req %d: response %q contains foreign token %q",
				ownIdx, respBody, other)
			return
		}
	}
}

// newToken returns a 16-hex-char random token. crypto/rand is
// the obvious choice — math/rand would risk seeded collisions
// under -count loops.
func newToken(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("newToken: rand.Read: %v", err)
	}
	return hex.EncodeToString(b[:])
}
