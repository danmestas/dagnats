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
