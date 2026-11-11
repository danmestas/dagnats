package logring

// Methodology: pure unit tests over a real ring handler (no NATS). A
// handler derived with WithAttrs/WithGroup must write into the same ring
// as its root (#1002): the console reads the root's Snapshot and
// Subscribe, so records from logger.With(...) were silently missing, and
// copying the ring state without its lock raced with Handle. Positive:
// records from derived loggers appear in the root's Snapshot and reach
// its subscribers. Negative: a record from one ring never appears in
// another. The race test runs under -race in CI.

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func TestLogRing_DerivedHandlersShareTheRing(t *testing.T) {
	root := NewWithOptions(slog.NewTextHandler(io.Discard, nil), Options{CapEntries: 100})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	live, stop := root.Subscribe(ctx)
	defer stop()

	logger := slog.New(root)
	logger.With("component", "a").Info("from with")
	logger.WithGroup("g").Info("from group")
	logger.Info("from root")

	snap := root.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("root snapshot has %d records, want 3 (derived loggers diverged)", len(snap))
	}
	got := map[string]bool{}
	for _, r := range snap {
		got[r.Message] = true
	}
	for _, msg := range []string{"from with", "from group", "from root"} {
		if !got[msg] {
			t.Fatalf("root snapshot is missing %q: %v", msg, got)
		}
	}
	for i := 0; i < 3; i++ {
		select {
		case <-live:
		case <-time.After(2 * time.Second):
			t.Fatalf("root subscriber got %d of 3 records", i)
		}
	}

	other := NewWithOptions(slog.NewTextHandler(io.Discard, nil), Options{CapEntries: 100})
	if n := len(other.Snapshot()); n != 0 {
		t.Fatalf("an unrelated ring holds %d records, want 0", n)
	}
}

func TestLogRing_ConcurrentWithAndLogIsRaceFree(t *testing.T) {
	root := NewWithOptions(slog.NewTextHandler(io.Discard, nil), Options{CapEntries: 10_000})
	logger := slog.New(root)
	const writers, perWriter = 8, 200
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				logger.Info("root", "i", i)
			}
		}()
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				logger.With("w", w).Info("derived", "i", i)
			}
		}(w)
	}
	wg.Wait()
	if n := len(root.Snapshot()); n != 2*writers*perWriter {
		t.Fatalf("root snapshot has %d records, want %d", n, 2*writers*perWriter)
	}
}
