// journal/delivery_order_test.go
// Claim order between deliveries of one task message (ADR-026,
// finding 3): a claim by a newer delivery of the same message (same
// stream sequence, higher delivery count) fences an older delivery
// that opens after it, so a delivery held in a stalled consumer's
// prefetch buffer cannot take the iteration back from its successor.
// A resend is a fresh message (another stream sequence, count 1
// again) and still claims in journal order, and a message of unknown
// sequence (0) is never compared.
//
// Methodology: one embedded server per case, through the public Open
// surface, with the task shapes the engine and worker produce. Every
// wait is bounded; each case asserts the fenced and the unfenced side.
package journal

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
)

func TestOpen_NewerDeliveryOfSameMessageFencesOlder(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	newer := open(t, c, task("r-order", 0, 1, 2))
	older := task("r-order", 0, 1, 1)
	if _, err := Open(ctx, c.js, c.pub, older); !errors.Is(err, ErrFenced) {
		t.Fatalf("Open of delivery 1 after delivery 2 claimed: err = %v, want ErrFenced", err)
	}
	if _, err := newer.Append(ctx, TypeEntry, []byte(`1`)); err != nil || newer.Fenced() {
		t.Fatalf("delivery 2 after the older open: err = %v fenced = %v, want to keep writing",
			err, newer.Fenced())
	}
	if n := countRecords(t, c, "r-order"); n != 2 {
		t.Fatalf("records = %d, want the claim and the entry only (no older claim)", n)
	}
}

func TestOpen_ResendAndUnknownSequenceClaimInJournalOrder(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	live := open(t, c, task("r-resend", 0, 1, 2))
	// A dispatch resend: a fresh message whose count restarts at 1.
	resend := task("r-resend", 0, 1, 1)
	resend.StreamSeq = live.Task().StreamSeq + 1
	if _, err := Open(ctx, c.js, c.pub, resend); err != nil {
		t.Fatalf("Open of a resend: %v, want a claim in journal order", err)
	}
	if _, err := live.Append(ctx, TypeEntry, []byte(`1`)); !errors.Is(err, ErrFenced) {
		t.Fatalf("live append after the resend's claim: err = %v, want ErrFenced", err)
	}
	// A message of unknown sequence is not compared to anything.
	unknown := task("r-unknown", 0, 1, 1)
	unknown.StreamSeq = 0
	first := open(t, c, task("r-unknown", 0, 1, 2))
	if _, err := Open(ctx, c.js, c.pub, unknown); err != nil {
		t.Fatalf("Open with stream seq 0: %v, want a claim", err)
	}
	if _, err := first.Append(ctx, TypeEntry, []byte(`1`)); !errors.Is(err, ErrFenced) {
		t.Fatalf("append after a later claim of unknown sequence: err = %v, want ErrFenced", err)
	}
}

// TestOpen_RacingClaimsOfSameMessageNewerWins: both deliveries read
// the tail before either claim lands, and the older one (held in a
// stalled consumer's prefetch buffer) publishes first. The newer
// delivery's claim is rejected on the sequence fence and must resync
// behind the older claim, not be fenced by it: it then wins by order
// and the older delivery is the one fenced.
func TestOpen_RacingClaimsOfSameMessageNewerWins(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	newer := task("r-race", 0, 1, 2)
	j := &Journal{task: newer, honoured: map[uint64]bool{}}
	j.attach(c.js, c.pub)
	if err := j.bind(ctx); err != nil {
		t.Fatalf("bind: %v", err)
	}
	tail, err := j.readTail(ctx)
	if err != nil || tail.Seq != 0 {
		t.Fatalf("readTail = %+v, %v; want an empty subject", tail, err)
	}
	j.seq = tail.Seq
	older := open(t, c, task("r-race", 0, 1, 1))
	claim, err := json.Marshal(Claim{
		Iteration: newer.Iteration, Attempt: newer.Attempt, DispatchNonce: newer.DispatchNonce,
		Delivered: newer.Delivered, StreamSeq: newer.StreamSeq,
	})
	if err != nil {
		t.Fatalf("marshal claim: %v", err)
	}
	if _, err := j.append(ctx, TypeClaim, claim); err != nil || j.Fenced() {
		t.Fatalf("newer claim after the older landed: err = %v fenced = %v; want it to resync and land",
			err, j.Fenced())
	}
	if _, err := older.Append(ctx, TypeEntry, []byte(`1`)); !errors.Is(err, ErrFenced) {
		t.Fatalf("older delivery's append: err = %v, want ErrFenced", err)
	}
	if _, err := j.Append(ctx, TypeEntry, []byte(`2`)); err != nil {
		t.Fatalf("newer delivery's append: %v, want it to keep writing", err)
	}
}
