// store_e2e_test.go
// Proves the KV-watch cache actually propagates across Store instances
// sharing one worker_tokens bucket: a mint in one Store's process must
// become visible in a second, independently-opened Store within a
// bounded wait, and a revoke must do the same. This is the behavior
// Authorize's "no NATS round-trip" design depends on.
// Methodology: one embedded NATS server, two Store instances opened
// against the same jetstream handle, bounded polling waits (no sleeps).
package workertoken

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

const watchPropagationWait = 5 * time.Second
const watchPropagationPoll = 50 * time.Millisecond

func TestStoreWatchPropagatesAcrossInstances(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	openCtx, openCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer openCancel()
	minter, err := Open(openCtx, js)
	if err != nil {
		t.Fatalf("Open(minter): %v", err)
	}
	defer minter.Close()
	reader, err := Open(openCtx, js)
	if err != nil {
		t.Fatalf("Open(reader): %v", err)
	}
	defer reader.Close()

	mintCtx, mintCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mintCancel()
	id, bearer, err := minter.Mint(mintCtx, "worker-a", []string{"echo"}, "tester")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	// Positive: the second Store instance sees the mint via its own
	// watch, without ever calling Mint itself.
	waitFor(t, "reader authorizes minted token", func() bool {
		_, err := reader.Authorize(bearer)
		return err == nil
	})

	revokeCtx, revokeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer revokeCancel()
	if err := minter.Revoke(revokeCtx, id); err != nil {
		t.Fatalf("Revoke: %v", err)
	}

	// Negative: the second Store instance also sees the revoke, again
	// purely via its watch.
	waitFor(t, "reader rejects revoked token", func() bool {
		_, err := reader.Authorize(bearer)
		return err != nil
	})
}

// waitFor polls cond until it returns true or watchPropagationWait
// elapses, failing the test on timeout. Bounded — never blocks forever.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(watchPropagationWait)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(watchPropagationPoll)
	}
	t.Fatalf("timed out waiting for: %s", what)
}
