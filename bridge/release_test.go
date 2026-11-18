// release_test.go
// Pins #1020: the "release" resolve action hands a held task back for
// immediate redelivery, without recording an outcome or a failed attempt.
//
// Methodology: real NATS, real orchestrator, real bridge over HTTP with
// minted worker tokens (tokenResolveFixture). Worker A claims a task and
// releases it; worker B's next poll gets it at once, far inside the
// 5-minute claim window a dropped task would wait out. Negative space:
//   - a release from a token that does not hold the claim is refused
//     and leaves the claim with its holder;
//   - the redelivered task carries the same attempt, and the run then
//     completes from it;
//   - the released claim is gone: resolving it again answers 404.
package bridge

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
)

// releaseRedeliveryMax is how long a released task may take to come
// back. Far below AckWait, so only an immediate redelivery passes.
const releaseRedeliveryMax = 5 * time.Second

func TestReleaseRedeliversTheTaskAtOnce(t *testing.T) {
	baseURL, svc, bearerA, bearerB, _ := tokenResolveFixture(t)
	task := startRunAndPollTask(t, baseURL, svc, bearerA)

	refused := resolveWithBearer(t, baseURL, task.TaskID, bearerB, `{"action":"release"}`)
	refused.Body.Close()
	if refused.StatusCode != http.StatusForbidden {
		t.Fatalf("release by a non-holder: status %d, want 403", refused.StatusCode)
	}

	resp := resolveWithBearer(t, baseURL, task.TaskID, bearerA, `{"action":"release"}`)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read release body: %v", err)
	}
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"released"`) {
		t.Fatalf("release by the holder: status %d body %s, want 200 released",
			resp.StatusCode, body)
	}

	stale := resolveWithBearer(t, baseURL, task.TaskID, bearerA, `{"action":"release"}`)
	stale.Body.Close()
	if stale.StatusCode != http.StatusNotFound {
		t.Fatalf("second release of the same claim: status %d, want 404", stale.StatusCode)
	}

	again := pollForTaskWithBearer(t, baseURL, bearerB, releaseRedeliveryMax)
	if again.RunID != task.RunID || again.StepID != task.StepID {
		t.Fatalf("redelivered %s/%s, want %s/%s",
			again.RunID, again.StepID, task.RunID, task.StepID)
	}
	if again.Attempt != task.Attempt {
		t.Fatalf("redelivered attempt %d, want %d: a release is not a failed attempt",
			again.Attempt, task.Attempt)
	}
	// The redelivery finishes the run: the release left no failure behind
	// for the engine to act on.
	done := resolveWithBearer(t, baseURL, again.TaskID, bearerB,
		`{"action":"complete","output":{"ok":true}}`)
	done.Body.Close()
	if done.StatusCode != http.StatusOK {
		t.Fatalf("complete after release: status %d", done.StatusCode)
	}
	waitRunStatus(t, svc, task.RunID, dag.RunStatusCompleted)
}

// waitRunStatus waits up to 10s for runID to read want.
func waitRunStatus(t *testing.T, svc *api.Service, runID string, want dag.RunStatus) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var got dag.RunStatus
	for time.Now().Before(deadline) {
		run, err := svc.GetRun(context.Background(), runID)
		if err == nil {
			got = run.Status
			if got == want {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("run %s reads %v, want %v within 10s", runID, got, want)
}
