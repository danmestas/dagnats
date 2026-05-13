// e2e/features/http_security_test.go
//
// Methodology: end-to-end coverage for ADR-013's request-time
// security knobs — HMAC validation and idempotency replay. Each
// test stands up its own embedded NATS server with the full
// SetupAll surface, a real engine, a real trigger service, and
// drives requests through the trigger.HTTPRouter via httptest.
// No fakes — the engine actually runs the respond step, the
// trigger handler actually validates the signature, and the
// idempotency KV bucket actually stores and replays results.
//
// Scenarios covered here:
//   - TestHTTPTrigger_HMAC: missing signature → 401, wrong
//     signature → 401, valid HMAC-SHA256(body) under
//     X-Signature-256 → 200.
//   - TestHTTPTrigger_IdempotencyReplay: two sequential requests
//     with the same Idempotency-Key value return byte-equal
//     responses and produce exactly one run id; ten concurrent
//     requests with the same key all receive the same payload
//     and converge on one run id.
package features

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sync"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// TestHTTPTrigger_HMAC drives a workflow whose HTTP trigger has a
// 16-char Secret. The handler maps the missing-signature and
// wrong-signature cases to 401 and accepts only an HMAC-SHA256 of
// the body under X-Signature-256 (the header name in
// trigger.httpHMACHeader). 200 happens only after the signature
// validates AND the engine publishes its respond payload.
func TestHTTPTrigger_HMAC(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		stack := startHTTPE2EStack(t, nc)

		// 16-char secret — matches httpConfigSecretMinLen.
		const secret = "abcdef0123456789"
		wfName := harness.UniqueName(t, "hmac-wf")
		wfDef := dag.WorkflowDef{
			Name:    wfName,
			Version: "v1",
			Steps: []dag.StepDef{
				respondStepDef(t, "respond", nil,
					dag.RespondConfig{Status: 200}),
			},
		}
		_, path := stack.registerHTTPTrigger(t, wfDef,
			&trigger.HTTPConfig{
				Path:         "/" + harness.UniqueName(t, "hmac"),
				Method:       http.MethodPost,
				TimeoutMs:    10_000,
				MaxBodyBytes: 1024,
				Secret:       secret,
			})

		body := []byte(`{"hmac":"yes"}`)

		// Negative 1: missing signature → 401.
		recMissing := postOnRouter(t, stack.router,
			http.MethodPost, path, body, nil)
		if recMissing.Code != http.StatusUnauthorized {
			t.Fatalf("missing sig: status = %d, want 401",
				recMissing.Code)
		}

		// Negative 2: wrong signature → 401.
		recWrong := postOnRouter(t, stack.router,
			http.MethodPost, path, body,
			map[string]string{
				"X-Signature-256": "sha256=deadbeef",
			})
		if recWrong.Code != http.StatusUnauthorized {
			t.Fatalf("wrong sig: status = %d, want 401",
				recWrong.Code)
		}

		// Positive: valid signature → 200.
		sig := signHMAC(secret, body)
		recGood := postOnRouter(t, stack.router,
			http.MethodPost, path, body,
			map[string]string{
				"X-Signature-256": sig,
			})
		if recGood.Code != 200 {
			t.Fatalf("valid sig: status = %d, want 200; body=%s",
				recGood.Code, recGood.Body)
		}
		if recGood.Header().Get("X-Dagnats-Run-Id") == "" {
			t.Fatal("X-Dagnats-Run-Id header missing on 200")
		}
	})
}

// signHMAC produces the value the trigger expects under
// X-Signature-256: "sha256=" + hex(HMAC-SHA256(body, secret)).
// Matches trigger.validHTTPSignature's verification path.
func signHMAC(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// TestHTTPTrigger_IdempotencyReplay drives a workflow with a
// real respond step that emits an UNIQUELY identifying payload
// per run (the run id baked into the body via run.Input echo).
// Sending the same Idempotency-Key twice replays the original
// run's stored response; a concurrent burst of 10 requests with
// the same key all converge on a single run id.
func TestHTTPTrigger_IdempotencyReplay(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		stack := startHTTPE2EStack(t, nc)

		// One task that returns a unique body per invocation. If
		// the dedup is honoured, only the FIRST call ever runs;
		// every subsequent same-key request replays the stored
		// payload from http_idempotency KV.
		var (
			mu      sync.Mutex
			invokes int
		)
		harness.SubscribeWorker(t, nc, "idem-task",
			func(tc worker.TaskContext) error {
				mu.Lock()
				invokes++
				body := []byte(`"call-` +
					string(rune('0'+invokes)) + `"`)
				mu.Unlock()
				return tc.Complete(body)
			})

		wfName := harness.UniqueName(t, "idem-wf")
		wfDef := dag.WorkflowDef{
			Name:    wfName,
			Version: "v1",
			Steps: []dag.StepDef{
				{ID: "produce", Task: "idem-task",
					Type: dag.StepTypeNormal},
				respondStepDef(t, "respond",
					[]string{"produce"},
					dag.RespondConfig{Status: 200}),
			},
		}
		_, path := stack.registerHTTPTrigger(t, wfDef,
			&trigger.HTTPConfig{
				Path:              "/" + harness.UniqueName(t, "idem"),
				Method:            http.MethodPost,
				TimeoutMs:         10_000,
				MaxBodyBytes:      1024,
				IdempotencyHeader: "Idempotency-Key",
			})

		runReplayAssertions(t, stack, path)
	})
}

// runReplayAssertions issues the sequential pair and a concurrent
// burst, asserting all responses are byte-equal and converge to a
// single run id. Split out so each function stays ≤ 70 lines.
func runReplayAssertions(
	t *testing.T, stack *httpE2EStack, path string,
) {
	t.Helper()
	const key = "key-replay-XYZ"
	hdr := map[string]string{"Idempotency-Key": key}

	// Sequential pair: first run + replay.
	rec1 := postOnRouter(t, stack.router,
		http.MethodPost, path, []byte(`{"n":1}`), hdr)
	if rec1.Code != 200 {
		t.Fatalf("seq 1: status = %d body=%s", rec1.Code,
			rec1.Body)
	}
	rec2 := postOnRouter(t, stack.router,
		http.MethodPost, path, []byte(`{"n":2}`), hdr)
	if rec2.Code != 200 {
		t.Fatalf("seq 2: status = %d body=%s", rec2.Code,
			rec2.Body)
	}
	if !bytesEqual(rec1.Body.Bytes(), rec2.Body.Bytes()) {
		t.Fatalf("replay body mismatch: r1=%q r2=%q",
			rec1.Body, rec2.Body)
	}
	r1 := rec1.Header().Get("X-Dagnats-Run-Id")
	r2 := rec2.Header().Get("X-Dagnats-Run-Id")
	if r1 == "" || r1 != r2 {
		t.Fatalf("run id mismatch: r1=%q r2=%q", r1, r2)
	}

	// Concurrent burst: 10 same-key requests → all the same body
	// and the same run id.
	const N = 10
	bodies := make([][]byte, N)
	runIDs := make([]string, N)
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			rec := postOnRouter(t, stack.router,
				http.MethodPost, path,
				[]byte(`{"n":99}`), hdr)
			bodies[i] = append([]byte(nil), rec.Body.Bytes()...)
			runIDs[i] = rec.Header().Get("X-Dagnats-Run-Id")
		}()
	}
	wg.Wait()
	for i := 1; i < N; i++ {
		if !bytesEqual(bodies[0], bodies[i]) {
			t.Fatalf("burst %d body diverged: %q vs %q",
				i, bodies[0], bodies[i])
		}
		if runIDs[0] != runIDs[i] {
			t.Fatalf("burst %d run id diverged: %q vs %q",
				i, runIDs[0], runIDs[i])
		}
	}
	// Both phases converged on the SAME run id as the first call.
	if runIDs[0] != r1 {
		t.Fatalf("burst run id %q != sequential %q",
			runIDs[0], r1)
	}
}

// bytesEqual returns true when the two byte slices are
// byte-identical. Inlining a trivial helper keeps the imports
// minimal — bytes.Equal would work but is unused elsewhere in
// this file.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
