// Methodology: pure unit tests of BindKeyValue against a scripted
// jetstream.JetStream whose KeyValue lookup fails a set number of times.
// A clustered STREAM.INFO sent while the bucket's stream elects a leader
// gets no reply, which surfaces as context.DeadlineExceeded; the bind must
// re-send it. A missing bucket is a real answer and must return at once.
package natsutil

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

// scriptedBindJS answers KeyValue with failErr for the first failCount
// calls, then with a nil-valued success. Every other method is unused and
// panics through the nil embedded interface.
type scriptedBindJS struct {
	jetstream.JetStream
	failErr   error
	failCount int
	calls     int
}

func (s *scriptedBindJS) KeyValue(
	ctx context.Context, bucket string,
) (jetstream.KeyValue, error) {
	s.calls++
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("bind attempt must carry a deadline")
	}
	if s.calls <= s.failCount {
		return nil, s.failErr
	}
	return nil, nil
}

func TestBindKeyValue_RetriesUnansweredLookup(t *testing.T) {
	js := &scriptedBindJS{failErr: context.DeadlineExceeded, failCount: 1}
	_, err := BindKeyValue(context.Background(), js, "workflow_runs")
	if err != nil {
		t.Fatalf("bind after one dropped lookup: %v", err)
	}
	if js.calls != 2 {
		t.Fatalf("calls=%d, want 2 (one drop, one answer)", js.calls)
	}
}

func TestBindKeyValue_MissingBucketNotRetried(t *testing.T) {
	js := &scriptedBindJS{failErr: jetstream.ErrBucketNotFound, failCount: 5}
	_, err := BindKeyValue(context.Background(), js, "triggers")
	if !errors.Is(err, jetstream.ErrBucketNotFound) {
		t.Fatalf("err=%v, want ErrBucketNotFound", err)
	}
	if js.calls != 1 {
		t.Fatalf("calls=%d, want 1: a missing bucket is an answer", js.calls)
	}
}
