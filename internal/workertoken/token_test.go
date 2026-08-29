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

import "testing"

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
