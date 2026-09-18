// token_test.go
// Unit tests for Claims.AllowsTaskType's prefix matching -- pure Go,
// no NATS dependency.
// Methodology: table test pinning the segment-aware matching rule
// (security review follow-up on #627): a raw byte-prefix match let
// ["build"] match "builder.deploy" and ["echo"] match "echo-admin",
// authorizing task types the operator never intended to scope in. A
// prefix p must match a task type t iff t == p or t has p + "." as a
// literal prefix (i.e. p names a whole dot-segment, not a byte run).
package workertoken

import (
	"strings"
	"testing"
)

func TestClaimsAllowsTaskTypeIsSegmentAware(t *testing.T) {
	cases := []struct {
		name     string
		prefixes []string
		taskType string
		want     bool
	}{
		// Counterexamples from the security review: a byte-prefix
		// match previously let these through.
		{"build does not match builder.deploy", []string{"build"}, "builder.deploy", false},
		{"echo does not match echo-admin", []string{"echo"}, "echo-admin", false},
		// Positives: exact segment and a proper dot-child.
		{"build matches build.x", []string{"build"}, "build.x", true},
		{"build matches build exactly", []string{"build"}, "build", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claims := Claims{TaskTypePrefixes: tc.prefixes}
			got := claims.AllowsTaskType(tc.taskType)
			if got != tc.want {
				t.Fatalf("AllowsTaskType(%q) with prefixes %v = %v, want %v",
					tc.taskType, tc.prefixes, got, tc.want)
			}
		})
	}
}

func TestClaimsAllowsTaskTypeAdminBypassesSegmentCheck(t *testing.T) {
	claims := Claims{Admin: true}
	// Positive: admin matches anything, including a type that would
	// fail every segment-prefix check.
	if !claims.AllowsTaskType("builder.deploy") {
		t.Fatal("admin claims must allow any task type")
	}
	// Negative: a non-admin claim with no prefixes at all still denies
	// everything -- fail closed is unaffected by the segment-aware fix.
	empty := Claims{}
	if empty.AllowsTaskType("build") {
		t.Fatal("empty prefixes must deny every task type")
	}
}

// TestClaimsAllowsWorkerGroup pins the exact-match, fail-closed-only-
// when-scoped contract (#695): a token with no WorkerGroups entries is
// unscoped BY GROUP -- the opposite polarity from AllowsTaskType's
// empty-prefixes-deny-all, because an empty WorkerGroups list is
// today's pre-#695 behavior and must keep every existing token
// working. Once a token names at least one group, it must match
// EXACTLY -- including never matching the ungrouped queue ("") -- so a
// group-scoped token can never fall back to draining someone else's
// ungrouped work.
func TestClaimsAllowsWorkerGroup(t *testing.T) {
	cases := []struct {
		name         string
		workerGroups []string
		group        string
		want         bool
	}{
		{"unscoped_allows_named_group", nil, "alpha", true},
		{"unscoped_allows_ungrouped", nil, "", true},
		{"scoped_allows_its_own_group", []string{"alpha"}, "alpha", true},
		{"scoped_denies_other_group", []string{"alpha"}, "beta", false},
		{"scoped_denies_ungrouped", []string{"alpha"}, "", false},
		{"scoped_exact_match_only_no_prefix_widening",
			[]string{"alpha"}, "alpha-fast", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claims := Claims{WorkerGroups: tc.workerGroups}
			got := claims.AllowsWorkerGroup(tc.group)
			if got != tc.want {
				t.Fatalf("AllowsWorkerGroup(%q) with groups %v = %v, want %v",
					tc.group, tc.workerGroups, got, tc.want)
			}
		})
	}
}

// TestClaimsExplainTaskTypeRefusalEmptyScope pins case (a) of #711:
// a token minted with no task types at all must say so plainly,
// distinct from a token scoped to the wrong thing, because it is the
// one a holder cannot diagnose by looking at the queue.
func TestClaimsExplainTaskTypeRefusalEmptyScope(t *testing.T) {
	claims := Claims{}
	got := claims.ExplainTaskTypeRefusal("dantest-puzzles.26acbacb")
	if !strings.Contains(got, "dantest-puzzles.26acbacb") {
		t.Fatalf("explanation %q does not name the requested task type", got)
	}
	if !strings.Contains(got, "no task types") {
		t.Fatalf("explanation %q does not say the token is scoped to nothing", got)
	}
}

// TestClaimsExplainTaskTypeRefusalNearMiss pins case (b): the issue's
// own repro -- a token minted with the byte-prefix "dantest-" reads
// as scoped to the puzzles/html task types but matches none of them,
// because scopes name whole dot-segments, not byte runs.
func TestClaimsExplainTaskTypeRefusalNearMiss(t *testing.T) {
	claims := Claims{TaskTypePrefixes: []string{"dantest-"}}
	taskType := "dantest-puzzles.26acbacb9e1879580c89a231477106f4"
	got := claims.ExplainTaskTypeRefusal(taskType)
	if !strings.Contains(got, taskType) {
		t.Fatalf("explanation %q does not name the requested task type", got)
	}
	if !strings.Contains(got, "dantest-") {
		t.Fatalf("explanation %q does not name the offending scope", got)
	}
	if !strings.Contains(got, "'.'") && !strings.Contains(got, `"."`) {
		t.Fatalf("explanation %q does not mention the dot rule", got)
	}
}

// TestClaimsExplainTaskTypeRefusalPlainMismatch pins case (c): a
// token scoped to real task types that simply do not include the one
// requested -- the ordinary "wrong thing" case, listing every scope
// so the holder can tell at a glance.
func TestClaimsExplainTaskTypeRefusalPlainMismatch(t *testing.T) {
	claims := Claims{TaskTypePrefixes: []string{"dantest-puzzles", "dantest-html"}}
	taskType := "dantest-other.26acbacb"
	got := claims.ExplainTaskTypeRefusal(taskType)
	if !strings.Contains(got, taskType) {
		t.Fatalf("explanation %q does not name the requested task type", got)
	}
	if !strings.Contains(got, "dantest-puzzles") || !strings.Contains(got, "dantest-html") {
		t.Fatalf("explanation %q does not list every scope", got)
	}
}

// TestClaimsExplainTaskTypeRefusalPanicsWhenAllowed pins the
// precondition: the explainer is only meaningful for a rejection.
// Calling it for a type AllowsTaskType would have accepted is a
// programmer error, not a runtime condition to explain gracefully.
func TestClaimsExplainTaskTypeRefusalPanicsWhenAllowed(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic when taskType is actually allowed")
		}
	}()
	claims := Claims{TaskTypePrefixes: []string{"build"}}
	claims.ExplainTaskTypeRefusal("build")
}

// TestClaimsExplainTaskTypeRefusalPanicsForAdmin pins the second
// precondition: admin claims never reach a refusal, so calling the
// explainer for one is a caller bug.
func TestClaimsExplainTaskTypeRefusalPanicsForAdmin(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for admin claims")
		}
	}()
	claims := Claims{Admin: true}
	claims.ExplainTaskTypeRefusal("anything")
}

// TestClaimsExplainWorkerGroupRefusal pins the group-side symmetry
// pass of #711: the message names both the requested group and the
// token's own group scopes.
func TestClaimsExplainWorkerGroupRefusal(t *testing.T) {
	claims := Claims{WorkerGroups: []string{"alpha", "beta"}}
	got := claims.ExplainWorkerGroupRefusal("gamma")
	if !strings.Contains(got, "gamma") {
		t.Fatalf("explanation %q does not name the requested group", got)
	}
	if !strings.Contains(got, "alpha") || !strings.Contains(got, "beta") {
		t.Fatalf("explanation %q does not list every scoped group", got)
	}
}

// TestClaimsExplainWorkerGroupRefusalPanicsWhenAllowed pins the
// precondition, mirroring ExplainTaskTypeRefusal's.
func TestClaimsExplainWorkerGroupRefusalPanicsWhenAllowed(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic when group is actually allowed")
		}
	}()
	claims := Claims{WorkerGroups: []string{"alpha"}}
	claims.ExplainWorkerGroupRefusal("alpha")
}

func TestClaimsAllowsWorkerGroupAdminBypasses(t *testing.T) {
	claims := Claims{Admin: true, WorkerGroups: []string{"alpha"}}
	// Positive: admin matches any group, even one outside its own scope.
	if !claims.AllowsWorkerGroup("beta") {
		t.Fatal("admin claims must allow any worker group")
	}
	if !claims.AllowsWorkerGroup("") {
		t.Fatal("admin claims must allow the ungrouped queue")
	}
}
