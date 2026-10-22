// journal/transient_test.go
// Methodology: pure unit tests of retryTransient, the bounded retry a
// journal read takes when the stream briefly has no leader (a leader
// election or a node restart makes JetStream answer "no responders").
// No NATS server: the operation is a stub that fails a set number of
// times, so the retry count and the pass-through of real errors are
// exact.
package journal

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestRetryTransient_RetriesNoResponders(t *testing.T) {
	calls := 0
	err := retryTransient(context.Background(), func() error {
		calls++
		if calls < 3 {
			return nats.ErrNoResponders
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retryTransient = %v, want success after two transient failures", err)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestRetryTransient_PassesThroughRealErrors(t *testing.T) {
	calls := 0
	boom := errors.New("boom")
	err := retryTransient(context.Background(), func() error {
		calls++
		return boom
	})
	if !errors.Is(err, boom) || calls != 1 {
		t.Fatalf("err=%v calls=%d, want boom after one call", err, calls)
	}
}

func TestRetryTransient_IsBounded(t *testing.T) {
	calls := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := retryTransient(ctx, func() error {
		calls++
		if calls == 2 {
			cancel() // a cancelled ctx ends the retry early
		}
		return nats.ErrNoResponders
	})
	if !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("err = %v, want the last transient error", err)
	}
	if calls != 2 || calls > transientReadMax {
		t.Fatalf("calls = %d, want 2 (stopped by ctx), never above %d", calls, transientReadMax)
	}
}

// TestRetryTransient_RetriesJetStreamUnavailable: a leader election
// can also be answered with the server's own "JetStream system
// temporarily unavailable" (10008), which is retried like a missing
// responder; any other API error passes through on the first try.
func TestRetryTransient_RetriesJetStreamUnavailable(t *testing.T) {
	unavailable := &jetstream.APIError{
		Code: 503, ErrorCode: errCodeJetStreamUnavailable,
		Description: "JetStream system temporarily unavailable",
	}
	calls := 0
	err := retryTransient(context.Background(), func() error {
		calls++
		if calls < 2 {
			return unavailable
		}
		return nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("retryTransient = %v after %d calls, want success on the second", err, calls)
	}
	other := &jetstream.APIError{Code: 404, ErrorCode: jetstream.JSErrCodeStreamNotFound}
	calls = 0
	err = retryTransient(context.Background(), func() error {
		calls++
		return other
	})
	if !errors.Is(err, other) || calls != 1 {
		t.Fatalf("retryTransient = %v after %d calls, want the stream-not-found error once", err, calls)
	}
}
