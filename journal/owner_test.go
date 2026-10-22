// journal/owner_test.go
// A *Journal is single-goroutine (ADR-026): using one handle from two
// goroutines panics, while a tool function calling back into the
// journal from the goroutine that owns it (partial-output entries) is
// allowed.
//
// Methodology: one embedded server, the public surface; the second
// goroutine is a tool function blocked on a channel. Every wait is
// bounded.
package journal

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
)

func TestJournal_ConcurrentUsePanicsAndNestedUseDoesNot(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	j := open(t, c, task("run-owner", 1, 1, 1))
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := j.Tool(ctx, ToolCall{Name: "slow"}, Safe,
			func(ctx context.Context, _ ToolCall) ([]byte, error) {
				// Nested use from the owning goroutine is fine.
				if _, err := j.Append(ctx, TypeEntry, []byte(`"nested"`)); err != nil {
					return nil, err
				}
				close(entered)
				<-release
				return []byte(`"ok"`), nil
			})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(replayTimeout):
		t.Fatal("tool never started")
	}
	panicked := func() (recovered any) {
		defer func() { recovered = recover() }()
		_, _ = j.Append(ctx, TypeEntry, []byte(`"intruder"`))
		return nil
	}()
	if panicked == nil {
		t.Error("Append from a second goroutine did not panic")
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("owning goroutine's Tool err = %v after the intruder, want nil", err)
		}
	case <-time.After(replayTimeout):
		t.Fatal("tool never returned")
	}
	if _, err := j.Append(ctx, TypeEntry, []byte(`"after"`)); err != nil {
		t.Errorf("Append after the tool returned err = %v, want the handle released", err)
	}
	if n := countRecords(t, c, "run-owner"); n != 5 {
		t.Errorf("records = %d, want 5: claim, intent, nested, result, after", n)
	}
}
