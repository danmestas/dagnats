// bridge/poll_create_error_test.go
// Methodology: unit test of consumerCreateError's operator-facing wording.
// The work-queue "filter not unique" rejection must name the real cause
// (an overlapping consumer) and must not claim grouped task types cannot
// be polled, which stopped being true when worker_group polling landed.
package bridge

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestConsumerCreateErrorWorkQueueOverlapWording(t *testing.T) {
	cause := &jetstream.APIError{ErrorCode: jsErrCodeWorkQueueConsumerNotUnique}
	err := consumerCreateError(
		context.Background(), "task-x", "task.x.>", cause)
	if err == nil {
		t.Fatal("want an error")
	}
	msg := err.Error()
	if strings.Contains(msg, "not pollable") {
		t.Errorf("stale wording survives: %s", msg)
	}
	if !strings.Contains(msg, "worker_group") ||
		!strings.Contains(msg, "overlapping filter") {
		t.Errorf("message lacks the actionable cause: %s", msg)
	}
	if !errors.Is(err, cause) {
		t.Error("cause must stay wrapped")
	}
}
