// poll_alias_test.go
// Tests for firstUnauthorizedAliasedReading, the review fix for the
// subject-aliasing bypass found in #695: consumername.FilterFor's
// (taskType, group) -> filter-subject mapping is not injective, so a
// dotted ungrouped task type ("a.b") derives the byte-identical filter
// subject as the grouped pair (task type "a", group "b"). Authorizing
// only the caller's chosen spelling let a token scoped to one reading
// silently reach the other.
//
// Methodology: pure unit tests for the alias math (no NATS), plus a
// real embedded-NATS/real-bridge HTTP round trip reproducing the
// reviewer's exact demonstrated attack end to end.
package bridge

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/workertoken"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSplitLastDot(t *testing.T) {
	cases := []struct {
		name           string
		in             string
		wantHead, want string
		wantOK         bool
	}{
		{"no_dot", "build", "", "", false},
		{"one_dot", "zz4.alpha", "zz4", "alpha", true},
		{"multi_dot_splits_only_last", "dagger.call.alpha", "dagger.call", "alpha", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			head, tail, ok := splitLastDot(tc.in)
			if ok != tc.wantOK {
				t.Fatalf("splitLastDot(%q) ok = %v, want %v", tc.in, ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if head != tc.wantHead || tail != tc.want {
				t.Fatalf("splitLastDot(%q) = (%q, %q), want (%q, %q)",
					tc.in, head, tail, tc.wantHead, tc.want)
			}
		})
	}
}

// TestFirstUnauthorizedAliasedReadingCatchesUngroupedAlias is the
// pure-function regression pin for the reviewer's exact demonstrated
// attack: a token scoped to task-type prefix "zz4.alpha" (no group
// scope, so unscoped-by-group) must be refused an UNGROUPED poll for
// task type "zz4.alpha" itself, because that request derives the same
// filter subject as the grouped reading (task "zz4", group "alpha"),
// and the token's prefixes do not cover task type "zz4" on its own.
func TestFirstUnauthorizedAliasedReadingCatchesUngroupedAlias(t *testing.T) {
	claims := workertoken.Claims{TaskTypePrefixes: []string{"zz4.alpha"}}
	reason, ok := firstUnauthorizedAliasedReading(
		claims, []string{"zz4.alpha"}, "",
	)
	if !ok {
		t.Fatal("firstUnauthorizedAliasedReading = false, want true (denied)")
	}
	if reason == "" {
		t.Fatal("firstUnauthorizedAliasedReading reason is empty")
	}
}

// TestFirstUnauthorizedAliasedReadingAllowsIntendedGroupedCase pins the
// case the fix must NOT break: a token scoped to task-type prefix
// "dagger" (segment-aware, so it already covers "dagger.alpha" as a
// dot-child) polling the grouped pair (task "dagger", group "alpha")
// must still pass. This is the exact case the reviewer flagged as
// something a future edit might "fix" into a wrongful denial.
func TestFirstUnauthorizedAliasedReadingAllowsIntendedGroupedCase(t *testing.T) {
	claims := workertoken.Claims{TaskTypePrefixes: []string{"dagger"}}
	if reason, ok := firstUnauthorizedAliasedReading(
		claims, []string{"dagger"}, "alpha",
	); ok {
		t.Fatalf("firstUnauthorizedAliasedReading = true (%q), want false (allowed)",
			reason)
	}
}

// TestFirstUnauthorizedAliasedReadingNoDotIsNeverDenied proves an
// undotted, ungrouped task type never triggers the alias check at all
// (splitLastDot reports ok==false), regardless of scoping.
func TestFirstUnauthorizedAliasedReadingNoDotIsNeverDenied(t *testing.T) {
	claims := workertoken.Claims{TaskTypePrefixes: []string{"unrelated"}}
	if reason, ok := firstUnauthorizedAliasedReading(
		claims, []string{"build"}, "",
	); ok {
		t.Fatalf("firstUnauthorizedAliasedReading = true (%q), want false", reason)
	}
}

// TestFirstUnauthorizedAliasedReadingAdminBypasses proves Admin claims
// bypass the alias check entirely, same as firstUnauthorizedTaskType.
func TestFirstUnauthorizedAliasedReadingAdminBypasses(t *testing.T) {
	claims := workertoken.Claims{Admin: true}
	if _, ok := firstUnauthorizedAliasedReading(
		claims, []string{"zz4.alpha"}, "",
	); ok {
		t.Fatal("firstUnauthorizedAliasedReading denied an admin claim")
	}
}

// TestFirstUnauthorizedAliasedReadingGroupedSideNeverDeniesOnceBaseCheckPasses
// documents (and pins) a mathematical property of the grouped-side half
// of the fix: because Claims.AllowsTaskType is segment-aware, if
// AllowsTaskType(T) is true via ANY prefix (which firstUnauthorizedTaskType
// already requires before this function ever runs), AllowsTaskType(T+"."+G)
// is ALWAYS true too -- T+"."+G is by construction a dot-child of T under
// that same prefix. This test exists as defense-in-depth documentation:
// the grouped-side check in firstUnauthorizedAliasedReading cannot
// currently deny anything a passing base check didn't already permit,
// but it stays in place against a future AllowsTaskType change that
// breaks this monotonicity.
func TestFirstUnauthorizedAliasedReadingGroupedSideNeverDeniesOnceBaseCheckPasses(t *testing.T) {
	cases := []struct {
		name     string
		prefixes []string
		taskType string
	}{
		{"exact_prefix", []string{"zz4"}, "zz4"},
		{"ancestor_prefix", []string{"dagger"}, "dagger"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claims := workertoken.Claims{TaskTypePrefixes: tc.prefixes}
			// The base check (firstUnauthorizedTaskType, exercised
			// elsewhere) already requires AllowsTaskType(taskType).
			if !claims.AllowsTaskType(tc.taskType) {
				t.Fatalf("test setup invalid: AllowsTaskType(%q) = false",
					tc.taskType)
			}
			if reason, ok := firstUnauthorizedAliasedReading(
				claims, []string{tc.taskType}, "alpha",
			); ok {
				t.Fatalf(
					"firstUnauthorizedAliasedReading = true (%q), want false",
					reason,
				)
			}
		})
	}
}

// TestFirstUnauthorizedAliasedReadingGroupScopedTokenCannotLeakIntoSiblingGroup
// is the group-scoped mirror of the primary exploit: a token scoped to
// task-type ANCESTOR prefix "a" (which segment-aware AllowsTaskType
// already lets cover every dotted child, "a.b", "a.c", ...) but scoped
// by WorkerGroups to ONLY group "c" must not reach an unrelated group
// "b" merely because the ancestor prefix happens to also segment-match
// the dotted ungrouped spelling "a.b". This function alone (independent
// of the standalone AllowsWorkerGroup(req.WorkerGroup) gate the bridge
// also runs, which already blocks any ungrouped request from a group-
// scoped token) must refuse it.
func TestFirstUnauthorizedAliasedReadingGroupScopedTokenCannotLeakIntoSiblingGroup(t *testing.T) {
	claims := workertoken.Claims{
		TaskTypePrefixes: []string{"a"},
		WorkerGroups:     []string{"c"},
	}
	// Positive: its own scoped group, expressed as a grouped request,
	// stays authorized.
	if reason, ok := firstUnauthorizedAliasedReading(
		claims, []string{"a"}, "c",
	); ok {
		t.Fatalf("grouped (a, c) = denied (%q), want allowed", reason)
	}
	// Negative: a sibling group "b" it was never scoped to, reached via
	// the ungrouped dotted spelling "a.b", is refused.
	if reason, ok := firstUnauthorizedAliasedReading(
		claims, []string{"a.b"}, "",
	); !ok {
		t.Fatal("ungrouped a.b = allowed, want denied (not scoped to group b)")
	} else if reason == "" {
		t.Fatal("firstUnauthorizedAliasedReading reason is empty")
	}
}

// postGroupedPoll issues a bearer-authenticated poll naming exactly one
// task type and an explicit worker_group (empty string for ungrouped),
// returning the HTTP status code.
func postGroupedPoll(
	t *testing.T, baseURL, bearer, taskType, group string,
) int {
	t.Helper()
	body := fmt.Sprintf(
		`{"task_types":["%s"],"worker_group":"%s","max_tasks":1,"timeout_ms":100}`,
		taskType, group,
	)
	req, err := http.NewRequest(
		"POST", baseURL+"/v1/tasks/poll", strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+bearer)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// TestBridgePollAliasAttackEndToEnd reproduces the reviewer's exact
// demonstrated attack over a real HTTP round trip: a token minted with
// task_type_prefixes ["zz4.alpha"] and no worker_groups scope is
// correctly refused the grouped spelling ({"task_types":["zz4"],
// "worker_group":"alpha"}), and, after this fix, is ALSO refused the
// ungrouped spelling ({"task_types":["zz4.alpha"]}) that derives the
// identical filter subject and durable consumer.
func TestBridgePollAliasAttackEndToEnd(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	store := openTokenStore(t, js)

	b := newTestBridge(t, nc)
	b.token = "admin-secret"
	b.SetTokenStore(store)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	mintCtx, mintCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mintCancel()
	_, bearer, err := store.Mint(
		mintCtx, "attacker", []string{"zz4.alpha"}, nil, "tester",
	)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	// Positive (unchanged by this fix): the grouped spelling is refused.
	groupedStatus := postGroupedPoll(t, ts.URL, bearer, "zz4", "alpha")
	if groupedStatus != 403 {
		t.Fatalf("grouped spelling status = %d, want 403 (already true pre-fix)",
			groupedStatus)
	}

	// Negative (the bug this fix closes): the ungrouped spelling of the
	// SAME derived subject must now ALSO be refused.
	ungroupedStatus := postGroupedPoll(t, ts.URL, bearer, "zz4.alpha", "")
	if ungroupedStatus != 403 {
		t.Fatalf(
			"ungrouped alias spelling status = %d, want 403 -- the "+
				"aliasing bypass is not closed", ungroupedStatus,
		)
	}
}

// TestBridgePollAliasReverseDirectionEndToEnd pins the "vice versa"
// direction the reviewer asked for: a token scoped ONLY to the exact
// dotted task-type prefix "a.b" -- not the ancestor "a" -- is denied
// BOTH the grouped reading (task "a", group "b") AND its own literal
// ungrouped spelling ("a.b"), because they derive the identical filter
// subject and the token's prefix list does not cover the ancestor task
// type "a" on its own. Before this fix, the base
// firstUnauthorizedTaskType check alone would have let the ungrouped
// spelling through (exact prefix match on "a.b"), even though the
// grouped spelling was correctly refused -- exactly the same shape of
// gap the primary exploit test closes, mirrored onto a self-inflicted
// exact-prefix scope rather than an attacker-chosen one. An operator
// who wants ungrouped access to a dotted task type that could alias a
// group must grant the ancestor prefix (or be unscoped by task type),
// not just the literal dotted value -- see
// TestBridgePollAliasIntendedGroupedCaseStillWorksEndToEnd for that
// shape.
func TestBridgePollAliasReverseDirectionEndToEnd(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	store := openTokenStore(t, js)

	b := newTestBridge(t, nc)
	b.token = "admin-secret"
	b.SetTokenStore(store)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	mintCtx, mintCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mintCancel()
	_, bearer, err := store.Mint(
		mintCtx, "scoped-to-a-dot-b-exactly", []string{"a.b"}, nil, "tester",
	)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	// Negative, direction 1: the grouped reading (a, b) is refused --
	// unchanged by this fix, AllowsTaskType("a") is false for a prefix
	// list containing only the dotted "a.b".
	if status := postGroupedPoll(t, ts.URL, bearer, "a", "b"); status != 403 {
		t.Fatalf("grouped (a, b) status = %d, want 403", status)
	}
	// Negative, direction 2 (what this fix closes): the token's own
	// literal ungrouped spelling of the SAME derived subject is ALSO
	// refused, not silently left reachable through the other spelling.
	if status := postGroupedPoll(t, ts.URL, bearer, "a.b", ""); status != 403 {
		t.Fatalf(
			"ungrouped a.b status = %d, want 403 -- exact-dotted-prefix-"+
				"only scoping must not retain access via its own literal "+
				"spelling once the equivalent grouped reading is refused",
			status,
		)
	}
}

// TestBridgePollAliasIntendedGroupedCaseStillWorksEndToEnd is the
// end-to-end pin for the "must not break" case: a token scoped to
// task-type prefix "dagger" polling the grouped pair (dagger, alpha)
// still succeeds after this fix.
func TestBridgePollAliasIntendedGroupedCaseStillWorksEndToEnd(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	store := openTokenStore(t, js)

	b := newTestBridge(t, nc)
	b.token = "admin-secret"
	b.SetTokenStore(store)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	mintCtx, mintCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mintCancel()
	_, bearer, err := store.Mint(
		mintCtx, "dagger-worker", []string{"dagger"}, nil, "tester",
	)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	if status := postGroupedPoll(t, ts.URL, bearer, "dagger", "alpha"); status != 200 {
		t.Fatalf("grouped (dagger, alpha) status = %d, want 200", status)
	}
}
