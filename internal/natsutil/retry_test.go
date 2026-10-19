// Methodology: pure unit tests of provisionWithRetry with scripted create
// functions. Covers the three branches that matter: a no-answer attempt is
// retried on a fresh bounded context, a real error is returned at once, and
// persistent silence stops at the attempt cap.
package natsutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestProvisionWithRetry_RetriesNoAnswer(t *testing.T) {
	calls := 0
	err := provisionWithRetry(context.Background(), func(ctx context.Context) error {
		calls++
		if _, ok := ctx.Deadline(); !ok {
			t.Error("attempt context must carry a deadline")
		}
		if calls == 1 {
			return context.DeadlineExceeded
		}
		return nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("err=%v calls=%d, want nil/2", err, calls)
	}
}

func TestProvisionWithRetry_RealErrorNotRetried(t *testing.T) {
	boom := errors.New("insufficient storage")
	calls := 0
	err := provisionWithRetry(context.Background(), func(context.Context) error {
		calls++
		return boom
	})
	if !errors.Is(err, boom) || calls != 1 {
		t.Fatalf("err=%v calls=%d, want boom/1", err, calls)
	}
}

func TestProvisionWithRetry_StopsAtCap(t *testing.T) {
	calls := 0
	err := provisionWithRetry(context.Background(), func(context.Context) error {
		calls++
		return context.DeadlineExceeded
	})
	if !errors.Is(err, context.DeadlineExceeded) || calls != provisionAttemptsMax {
		t.Fatalf("err=%v calls=%d, want deadline/%d", err, calls, provisionAttemptsMax)
	}
}

// TestProvisionWithRetry_OnlyTransient503Retried uses real APIError values:
// 10008 (no meta leader yet) is retried; other 503s and server-side
// resource errors are returned at once.
func TestProvisionWithRetry_OnlyTransient503Retried(t *testing.T) {
	cases := []struct {
		name      string
		apiErr    *jetstream.APIError
		wantCalls int
	}{
		{"temporarily unavailable 10008",
			&jetstream.APIError{Code: 503, ErrorCode: 10008}, 2},
		{"no suitable peers 10005",
			&jetstream.APIError{Code: 400, ErrorCode: 10005}, 2},
		{"insufficient storage 10047",
			&jetstream.APIError{Code: 500, ErrorCode: 10047}, 1},
		{"jetstream not enabled 10076",
			&jetstream.APIError{
				Code:      503,
				ErrorCode: jetstream.JSErrCodeJetStreamNotEnabled,
			}, 1},
		{"jetstream not enabled for account 10039",
			&jetstream.APIError{
				Code:      503,
				ErrorCode: jetstream.JSErrCodeJetStreamNotEnabledForAccount,
			}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			err := provisionWithRetry(context.Background(),
				func(context.Context) error {
					calls++
					if calls == 1 {
						return tc.apiErr
					}
					return nil
				})
			if calls != tc.wantCalls {
				t.Fatalf("calls = %d, want %d (err=%v)", calls, tc.wantCalls, err)
			}
			if tc.wantCalls == 1 && err == nil {
				t.Error("non-retryable error must be returned")
			}
		})
	}
}

// TestProvisionRetryPause_GrowsAndCaps checks the pause between attempts
// backs off: a peer that has not joined yet needs seconds, not the
// 2.5s that five fixed 500ms pauses add up to.
func TestProvisionRetryPause_GrowsAndCaps(t *testing.T) {
	if got := provisionRetryPause(0); got != 500*time.Millisecond {
		t.Fatalf("pause(0) = %v, want 500ms", got)
	}
	if got := provisionRetryPause(2); got != 2*time.Second {
		t.Fatalf("pause(2) = %v, want 2s", got)
	}
	if got := provisionRetryPause(10); got != provisionRetryPauseMax {
		t.Fatalf("pause(10) = %v, want cap %v", got, provisionRetryPauseMax)
	}
}
