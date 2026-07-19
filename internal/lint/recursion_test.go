// internal/lint/recursion_test.go
//
// Tests for the repo-wide recursion check. Two layers: fixture tests
// that pin the analyzer's own behavior against hand-written Go sources
// with known shapes, and one enforcement test that runs the analyzer
// over this module and fails on any unannotated cycle.
//
// Methodology: each fixture test writes a single source file to a temp
// dir, runs FindRecursion over it, and asserts both positive space (the
// expected cycle is reported) and negative space (nothing else is).
// The false-positive fixtures matter most -- an earlier name-only
// version of this check reported 80 cycles of which 67 were junk, so
// every shape that previously misfired is pinned here.
package lint

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func scanSource(t *testing.T, source string) []Cycle {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.go")
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	cycles, err := FindRecursion(dir)
	if err != nil {
		t.Fatalf("FindRecursion: %v", err)
	}
	return cycles
}

func TestDetectsDirectSelfCall(t *testing.T) {
	cycles := scanSource(t, `package fixture
func walk(n int) int {
	if n == 0 {
		return 0
	}
	return walk(n - 1)
}
`)
	if len(cycles) != 1 {
		t.Fatalf("got %d cycles, want 1: %v", len(cycles), cycles)
	}
	if len(cycles[0].Funcs) != 1 || cycles[0].Funcs[0] != "walk" {
		t.Errorf("Funcs = %v, want [walk]", cycles[0].Funcs)
	}
	if !strings.Contains(cycles[0].String(), "calls itself") {
		t.Errorf("diagnostic %q should say the function calls itself",
			cycles[0].String())
	}
}

func TestDetectsMutualRecursion(t *testing.T) {
	cycles := scanSource(t, `package fixture
func even(n int) bool {
	if n == 0 {
		return true
	}
	return odd(n - 1)
}
func odd(n int) bool {
	if n == 0 {
		return false
	}
	return even(n - 1)
}
`)
	if len(cycles) != 1 {
		t.Fatalf("got %d cycles, want 1: %v", len(cycles), cycles)
	}
	if len(cycles[0].Funcs) != 2 {
		t.Fatalf("Funcs = %v, want 2 participants", cycles[0].Funcs)
	}
	joined := strings.Join(cycles[0].Funcs, ",")
	if !strings.Contains(joined, "even") || !strings.Contains(joined, "odd") {
		t.Errorf("Funcs = %v, want both even and odd", cycles[0].Funcs)
	}
}

func TestMethodOnOtherObjectIsNotRecursion(t *testing.T) {
	// The false positive that made name-only matching unusable:
	// Error() calling a wrapped error's Error() is not self-recursion.
	cycles := scanSource(t, `package fixture
type wrapper struct{ err error }
func (w *wrapper) Error() string { return w.err.Error() }
type inner struct{ sub *inner }
func (i *inner) Stop() { i.sub.Stop() }
`)
	if len(cycles) != 0 {
		t.Fatalf("got %d cycles, want 0: %v", len(cycles), cycles)
	}
}

func TestMethodOnReceiverIsRecursion(t *testing.T) {
	cycles := scanSource(t, `package fixture
type tree struct{ kids []*tree }
func (t *tree) count() int {
	total := 1
	for _, k := range t.kids {
		_ = k
		total += t.count()
	}
	return total
}
`)
	if len(cycles) != 1 {
		t.Fatalf("got %d cycles, want 1: %v", len(cycles), cycles)
	}
	if cycles[0].Funcs[0] != "tree.count" {
		t.Errorf("Funcs = %v, want [tree.count]", cycles[0].Funcs)
	}
}

func TestGoStatementIsNotStackRecursion(t *testing.T) {
	// A goroutine does not grow the caller's stack, so it cannot cause
	// the unbounded growth this rule exists to prevent.
	cycles := scanSource(t, `package fixture
func serve(n int) {
	if n == 0 {
		return
	}
	go serve(n - 1)
}
`)
	if len(cycles) != 0 {
		t.Fatalf("got %d cycles, want 0: %v", len(cycles), cycles)
	}
}

func TestAllowMarkerSuppresses(t *testing.T) {
	cycles := scanSource(t, `package fixture
// walk descends a tree.
//
// recursion:allow bounded by tree depth.
func walk(n int) int {
	if n == 0 {
		return 0
	}
	return walk(n - 1)
}
`)
	if len(cycles) != 0 {
		t.Fatalf("got %d cycles, want 0: %v", len(cycles), cycles)
	}
	// Negative space: the marker must not disable the check globally.
	more := scanSource(t, `package fixture
// allowed is exempt.
//
// recursion:allow deliberate.
func allowed(n int) int { return allowed(n) }
func notAllowed(n int) int { return notAllowed(n) }
`)
	if len(more) != 1 {
		t.Fatalf("got %d cycles, want 1: %v", len(more), more)
	}
	if more[0].Funcs[0] != "notAllowed" {
		t.Errorf("Funcs = %v, want [notAllowed]", more[0].Funcs)
	}
}

func TestNonRecursiveCodeIsClean(t *testing.T) {
	cycles := scanSource(t, `package fixture
func a() int { return b() + c() }
func b() int { return c() }
func c() int { return 1 }
`)
	if len(cycles) != 0 {
		t.Fatalf("got %d cycles, want 0: %v", len(cycles), cycles)
	}
}

// TestModuleHasNoUnannotatedRecursion is the gate. CLAUDE.md forbids
// recursion, but no standard tool enforces it -- gofmt, go vet, and
// staticcheck all pass on recursive code, which is how issue #554 went
// unnoticed until a manual audit. Every known site carries a
// recursion:allow marker with a justification; anything new fails here.
func TestModuleHasNoUnannotatedRecursion(t *testing.T) {
	root, err := ModuleRoot(".")
	if err != nil {
		t.Fatalf("locate module root: %v", err)
	}
	cycles, err := FindRecursion(root)
	if err != nil {
		t.Fatalf("FindRecursion: %v", err)
	}
	for _, cycle := range cycles {
		t.Errorf("%s\n\tAdd %q with a justification if this recursion is "+
			"deliberate and bounded, or rewrite it iteratively.",
			cycle.String(), AllowMarker)
	}
	if t.Failed() {
		t.Logf("%d unannotated recursion cycle(s)", len(cycles))
	}
}

func TestModuleRootFindsGoMod(t *testing.T) {
	root, err := ModuleRoot(".")
	if err != nil {
		t.Fatalf("ModuleRoot: %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(root, "go.mod")); statErr != nil {
		t.Errorf("ModuleRoot returned %q with no go.mod: %v", root, statErr)
	}
	if _, err := ModuleRoot(string(filepath.Separator)); err == nil {
		t.Error("ModuleRoot on filesystem root should fail, got nil error")
	}
}
