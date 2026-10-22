// internal/chaos/chaos_test.go
// The crash seam's production cost and its routing.
//
// Methodology: pure unit tests. With no hook installed At must neither
// allocate nor call anything; with one installed it must deliver the
// point and site and carry the hook's kill back.
package chaos

import (
	"context"
	"testing"
)

func TestAt_NoHookIsFreeAndProceeds(t *testing.T) {
	Install(nil)
	if Installed() {
		t.Fatal("Installed() = true with no hook")
	}
	site := Site{RunID: "r", StepID: "s", Iteration: 1, Attempt: 1, Delivered: 1}
	allocs := testing.AllocsPerRun(1000, func() {
		if err := At(context.Background(), WorkerAfterClaim, site); err != nil {
			t.Fatalf("At with no hook: %v", err)
		}
	})
	if allocs != 0 {
		t.Errorf("At with no hook allocates %.1f times per call, want 0", allocs)
	}
}

func TestAt_InstalledHookSeesSiteAndKills(t *testing.T) {
	var seen []Point
	Install(func(_ context.Context, p Point, site Site) error {
		seen = append(seen, p)
		if site.RunID != "r" || site.Iteration != 2 {
			t.Errorf("site = %+v, want run r iteration 2", site)
		}
		if p == WorkerInsideTool {
			return ErrKilled
		}
		return nil
	})
	t.Cleanup(func() { Install(nil) })
	if !Installed() {
		t.Fatal("Installed() = false with a hook")
	}
	site := Site{RunID: "r", StepID: "s", Iteration: 2, Attempt: 1, Delivered: 1}
	if err := At(context.Background(), WorkerAfterClaim, site); err != nil {
		t.Fatalf("proceeding point: %v", err)
	}
	err := At(context.Background(), WorkerInsideTool, site)
	if !Killed(err) {
		t.Fatalf("killing point: err = %v, want ErrKilled", err)
	}
	if len(seen) != 2 || seen[0] != WorkerAfterClaim || seen[1] != WorkerInsideTool {
		t.Errorf("hook saw %v, want [after_claim inside_tool]", seen)
	}
	if len(Points()) != int(pointMax)-1 {
		t.Errorf("Points() has %d entries, want %d", len(Points()), pointMax-1)
	}
}

func BenchmarkAt_NoHook(b *testing.B) {
	Install(nil)
	site := Site{RunID: "r", StepID: "s", Iteration: 1, Attempt: 1, Delivered: 1}
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := At(ctx, WorkerAfterClaim, site); err != nil {
			b.Fatal(err)
		}
	}
}
