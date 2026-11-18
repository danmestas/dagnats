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
//   - the redelivered task carries the same attempt, and the step is
//     not failed;
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
	run, err := svc.GetRun(context.Background(), task.RunID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got := run.Steps[task.StepID].Status; got == dag.StepStatusFailed {
		t.Fatalf("step reads %v after a release", got)
	}
}
