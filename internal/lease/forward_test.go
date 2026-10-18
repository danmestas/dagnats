// lease/forward_test.go
// Methodology: pure unit tests of the forwarding contract's fixed parts
// -- the inbox subject shape and the timeout bounds -- with no NATS. The
// delivery, retry and error paths are exercised end to end over the wire
// in internal/api/natsapi_lease_forward_test.go.
package lease

import (
	"errors"
	"testing"
	"time"
)

func TestInboxSubjectIsPrefixHolderKey(t *testing.T) {
	got := InboxSubject("notify-a", "shard-07")
	if got != "dagnats.lease.inbox.notify-a.shard-07" {
		t.Fatalf("InboxSubject = %q", got)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("InboxSubject with empty holder must panic")
		}
	}()
	InboxSubject("", "shard-07")
}

func TestForwardTimeoutBounds(t *testing.T) {
	for _, ok := range []time.Duration{ForwardTimeoutMin, time.Second, ForwardTimeoutMax} {
		if err := validateForwardTimeout(ok); err != nil {
			t.Errorf("timeout %v rejected: %v", ok, err)
		}
	}
	bad := []time.Duration{0, ForwardTimeoutMin - time.Millisecond, ForwardTimeoutMax + 1}
	for _, timeout := range bad {
		if err := validateForwardTimeout(timeout); !errors.Is(err, ErrInvalid) {
			t.Errorf("timeout %v = %v, want ErrInvalid", timeout, err)
		}
	}
}
