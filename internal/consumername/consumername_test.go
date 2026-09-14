// internal/consumername/consumername_test.go
// Pure unit tests for the consumer-naming helpers. No embedded NATS, no
// JetStream — these helpers are deliberately NATS-free so they can be
// exercised in isolation and reused by the collision precheck and the
// bridge poll path.
package consumername

import (
	"regexp"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
)

// natsKVValidKeyRe mirrors nats.go's own KV key validator
// (jetstream/kv.go:505, kv.go:392 as of nats.go v1.50.0-v1.53.1:
// `^[-/_=\.a-zA-Z0-9]+$`). Duplicated here deliberately, not imported
// (it is unexported in nats.go), specifically so a future sentinel
// change is caught by THIS test rather than discovered as a runtime
// "nats: invalid key" panic from github.com/synadia-io/orbit.go/pcgroups
// — see GroupSentinel's doc comment for the full four-domain constraint
// this exists to pin.
var natsKVValidKeyRe = regexp.MustCompile(`^[-/_=\.a-zA-Z0-9]+$`)

// TestNameFor_GroupedOutputIsLegalNATSKVKey is the regression guard for
// the #704 design bug found in review: worker/worker.go's partitioned/
// elastic consumer-group path (WithPartitions + WithGroups) hands
// NameFor's grouped output straight to pcgroups.CreateElastic, which
// keys its own group-state document in a NATS KV bucket by that exact
// string. A sentinel legal in a subject token and a consumer name but
// NOT in a KV key (the original "@" choice) passes every OTHER check in
// this package yet panics at consumer-group creation time. This test
// is the real fix per the pinned design correction — the character
// choice (GroupSentinel = "=") is just today's answer to it.
func TestNameFor_GroupedOutputIsLegalNATSKVKey(t *testing.T) {
	cases := []struct {
		name            string
		taskType, group string
	}{
		{"simple", "render", "gpu"},
		{"dotted_task", "dagger.call", "fast"},
		{"dotted_group_input", "render", "gpu.fast"},
		{"sanitizes_from_unsafe_group", "build.linux", "east"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := NameFor(tc.taskType, tc.group)
			if !natsKVValidKeyRe.MatchString(got) {
				t.Fatalf(
					"NameFor(%q, %q) = %q is not a legal NATS KV key "+
						"(must match %s) — pcgroups.CreateElastic will "+
						"panic on this at consumer-group creation time",
					tc.taskType, tc.group, got, natsKVValidKeyRe.String(),
				)
			}
		})
	}
}

func TestSanitize(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"identity_alphanumeric_dashes", "render", "render"},
		{"dot_collapses_to_dash", "render.gpu", "render-gpu"},
		{"hyphenated_preserved", "nasr-ingest", "nasr-ingest"},
		{"underscore_preserved", "nasr_ingest", "nasr_ingest"},
		{"colon_safe_escape", "vendor::ingest", "vendor__ingest"},
		{"whitespace_safe_escape", "a b c", "a_b_c"},
		{"only_dots_collapse", "....", "----"},
		{"mixed_classes", "Worker-1.2_x", "Worker-1-2_x"},
		// #704: "=" must stay OUTSIDE Sanitize's allowed set — it is the
		// sentinel NameFor/FilterFor use to disambiguate a dotted task
		// from an undotted task plus group, and must stay unreachable
		// from caller-supplied task/group input.
		{"sentinel_char_safe_escape", "=", "_"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Sanitize(tc.in)
			if got != tc.want {
				t.Fatalf("Sanitize(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if got == "" {
				t.Fatalf("Sanitize(%q) returned empty", tc.in)
			}
		})
	}
}

func TestSanitize_PanicsOnEmpty(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for empty input, got none")
		}
		msg, ok := r.(string)
		if !ok || msg == "" {
			t.Fatalf("expected non-empty string panic, got %#v", r)
		}
	}()
	Sanitize("")
}

// TestNameFor_IsLossy pins the collision the bridge's adopt path must
// defend against: sanitization is not injective, so a name match is not
// proof of a subject match.
func TestNameFor_IsLossy(t *testing.T) {
	dotted := NameFor("send.email", "")
	hyphenated := NameFor("send-email", "")
	if dotted != hyphenated {
		t.Fatalf("expected name collision, got %q vs %q",
			dotted, hyphenated)
	}
	if FilterFor("send.email", "") == FilterFor("send-email", "") {
		t.Fatal("filters must stay distinct where names collide")
	}
}

func TestDefaultAckWait_IsFiveMinutes(t *testing.T) {
	if DefaultAckWait != 5*time.Minute {
		t.Fatalf("DefaultAckWait = %v, want %v",
			DefaultAckWait, 5*time.Minute)
	}
	if DefaultAckWait <= 0 {
		t.Fatalf("DefaultAckWait must be positive, got %v", DefaultAckWait)
	}
}

func TestNameFor(t *testing.T) {
	cases := []struct {
		name            string
		taskType, group string
		want            string
	}{
		{"default_branch_simple", "render", "", "workers-render"},
		{"default_branch_dotted", "render.gpu", "", "workers-render-gpu"},
		{"default_branch_hyphenated", "nasr-ingest", "",
			"workers-nasr-ingest"},
		{"groups_branch_simple", "render", "gpu", "workers-render-=gpu"},
		{"groups_branch_dotted_group", "render", "gpu.fast",
			"workers-render-=gpu-fast"},
		{"groups_branch_safe_escape", "render", "gpu*1",
			"workers-render-=gpu_1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := NameFor(tc.taskType, tc.group)
			if got != tc.want {
				t.Fatalf("NameFor(%q, %q) = %q, want %q",
					tc.taskType, tc.group, got, tc.want)
			}
			if got == "" {
				t.Fatal("NameFor returned empty")
			}
		})
	}
}

func TestNameFor_RejectsEmptyTaskType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty taskType, got none")
		}
	}()
	NameFor("", "")
}

// TestNameFor_DottedTaskWithGroupNoLongerCollides is the load-bearing
// proof for #704: before the "=" sentinel, NameFor("a", "b") and
// NameFor("a.b", "") both sanitized to "workers-a-b" — a dotted task
// type combined with a group derived the SAME durable name (and, before
// #704's subject fix, the same filter) as an unrelated dotted-only task
// type. Without this distinction, worker/consumer_collision_xprocess.go's
// assertNoCrossProcessCollision would see a matching durable name with a
// DIFFERENT FilterSubject on the very next registration and panic at
// runtime instead of validation refusing the combination up front.
func TestNameFor_DottedTaskWithGroupNoLongerCollides(t *testing.T) {
	grouped := NameFor("a", "b")
	dottedTask := NameFor("a.b", "")
	if grouped == dottedTask {
		t.Fatalf(
			"NameFor(%q, %q) = %q must differ from NameFor(%q, %q) = %q",
			"a", "b", grouped, "a.b", "", dottedTask,
		)
	}
	if grouped != "workers-a-=b" {
		t.Fatalf(`NameFor("a", "b") = %q, want "workers-a-=b"`, grouped)
	}
	if dottedTask != "workers-a-b" {
		t.Fatalf(`NameFor("a.b", "") = %q, want "workers-a-b"`, dottedTask)
	}
}

func TestFilterFor(t *testing.T) {
	cases := []struct {
		name            string
		taskType, group string
		want            string
	}{
		{"default_branch", "render", "", "task.render.*"},
		{"default_branch_dotted_task", "render.gpu", "",
			"task.render.gpu.*"},
		{"groups_branch", "render", "gpu", "task.render.=gpu.*"},
		{"groups_branch_hyphenated", "nasr-ingest", "fastlane",
			"task.nasr-ingest.=fastlane.*"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := FilterFor(tc.taskType, tc.group)
			if got != tc.want {
				t.Fatalf("FilterFor(%q, %q) = %q, want %q",
					tc.taskType, tc.group, got, tc.want)
			}
			if got == "" {
				t.Fatal("FilterFor returned empty")
			}
		})
	}
}

// TestFilterFor_DottedTaskWithGroupNoLongerCollides is the subject-side
// half of the #704 proof (see TestNameFor_DottedTaskWithGroupNoLongerCollides
// for the durable-name half): before the "=" sentinel,
// FilterFor("a.b", "") and FilterFor("a", "b") were byte-identical
// ("task.a.b.*"), so a dotted task type could never carry a worker group
// without colliding with the unrelated dotted-only task type "a.b".
func TestFilterFor_DottedTaskWithGroupNoLongerCollides(t *testing.T) {
	grouped := FilterFor("a", "b")
	dottedTask := FilterFor("a.b", "")
	if grouped == dottedTask {
		t.Fatalf(
			"FilterFor(%q, %q) = %q must differ from FilterFor(%q, %q) = %q",
			"a", "b", grouped, "a.b", "", dottedTask,
		)
	}
	if grouped != "task.a.=b.*" {
		t.Fatalf(`FilterFor("a", "b") = %q, want "task.a.=b.*"`, grouped)
	}
	if dottedTask != "task.a.b.*" {
		t.Fatalf(`FilterFor("a.b", "") = %q, want "task.a.b.*"`, dottedTask)
	}
}

func TestFilterFor_RejectsEmptyTaskType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty taskType, got none")
		}
	}()
	FilterFor("", "")
}

// TestFilterFor_AnchorsToOneTrailingToken proves against a real NATS
// server (core pub/sub subject matching, identical wildcard semantics to
// JetStream's FilterSubject) that FilterFor("build", "") matches exactly
// what StepSubject publishes for the "build" task type — one trailing run
// ID token — and does NOT also match a differently-typed but
// dot-prefixed sibling like "build.linux". This is the regression guard
// for the #674 wildcard leak: the old "task.build.>" filter matched both.
func TestFilterFor_AnchorsToOneTrailingToken(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)

	sub, err := nc.SubscribeSync(FilterFor("build", ""))
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	defer sub.Unsubscribe()

	// Positive: a "build" task's own subject (task type + one run-ID
	// token) is delivered.
	ownSubject := "task.build." + "deadbeefdeadbeefdeadbeefdeadbeef"
	if err := nc.Publish(ownSubject, []byte("own")); err != nil {
		t.Fatalf("Publish(own): %v", err)
	}
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("expected delivery for %q, got: %v", ownSubject, err)
	}
	if string(msg.Data) != "own" {
		t.Fatalf("delivered payload = %q, want %q", msg.Data, "own")
	}

	// Negative: a differently-typed sibling that merely shares the
	// "build" dotted prefix must NOT be delivered to a "build" poller.
	siblingSubject := "task.build.linux." + "deadbeefdeadbeefdeadbeefdeadbeef"
	if err := nc.Publish(siblingSubject, []byte("sibling")); err != nil {
		t.Fatalf("Publish(sibling): %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if _, err := sub.NextMsg(300 * time.Millisecond); err == nil {
		t.Fatalf(
			"filter %q unexpectedly matched sibling subject %q",
			FilterFor("build", ""), siblingSubject,
		)
	}
}

func TestLegacyFilterFor(t *testing.T) {
	cases := []struct {
		name            string
		taskType, group string
		want            []string
	}{
		{"default_branch", "render", "", []string{"task.render.>"}},
		{"default_branch_dotted_task", "render.gpu", "",
			[]string{"task.render.gpu.>"}},
		// Grouped now has TWO legacy shapes (#704): the pre-#674 ">"
		// wildcard, and the sentinel-free "*"-anchored form FilterFor
		// itself produced between #674 and #704.
		{"groups_branch", "render", "gpu", []string{
			"task.render.gpu.>", "task.render.gpu.*",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := LegacyFilterFor(tc.taskType, tc.group)
			if len(got) != len(tc.want) {
				t.Fatalf("LegacyFilterFor(%q, %q) = %v, want %v",
					tc.taskType, tc.group, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("LegacyFilterFor(%q, %q) = %v, want %v",
						tc.taskType, tc.group, got, tc.want)
				}
			}
		})
	}
}

func TestLegacyNameFor(t *testing.T) {
	got := LegacyNameFor("render", "gpu")
	want := "workers-render-gpu"
	if got != want {
		t.Fatalf("LegacyNameFor(%q, %q) = %q, want %q",
			"render", "gpu", got, want)
	}
	// The whole point: LegacyNameFor must differ from today's NameFor for
	// the same pair, since #704 moved the sentinel into the name too.
	if got == NameFor("render", "gpu") {
		t.Fatal("LegacyNameFor must not equal NameFor for a grouped pair")
	}
}

func TestLegacyNameFor_RejectsEmptyGroup(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty group, got none")
		}
	}()
	LegacyNameFor("render", "")
}

func TestFilterIsLegacyUpgrade(t *testing.T) {
	cases := []struct {
		name      string
		new, old  string
		wantMatch bool
	}{
		{
			"legacy_twin_ungrouped",
			FilterFor("build", ""), LegacyFilterFor("build", "")[0],
			true,
		},
		{
			"legacy_twin_grouped_pre674",
			FilterFor("render", "gpu"), LegacyFilterFor("render", "gpu")[0],
			true,
		},
		{
			"legacy_twin_grouped_post674_pre704",
			FilterFor("render", "gpu"), LegacyFilterFor("render", "gpu")[1],
			true,
		},
		{
			"legacy_twin_dotted_task",
			FilterFor("dagger.call", ""), LegacyFilterFor("dagger.call", "")[0],
			true,
		},
		{
			"same_anchor_not_legacy",
			FilterFor("build", ""), FilterFor("build", ""),
			false,
		},
		{
			"different_task_type_not_legacy",
			FilterFor("build", ""), LegacyFilterFor("bar", "")[0],
			false,
		},
		{
			"different_group_not_legacy",
			FilterFor("render", "gpu"), LegacyFilterFor("render", "cpu")[0],
			false,
		},
		{
			// old is the legacy form of FilterFor("a", "") (no group),
			// not of FilterFor("a", "b") — an extra "b" token in new
			// must not be mistaken for old's legacy twin.
			"extra_group_token_not_legacy",
			"task.a.b.*", "task.a.>",
			false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := FilterIsLegacyUpgrade(tc.new, tc.old)
			if got != tc.wantMatch {
				t.Fatalf("FilterIsLegacyUpgrade(%q, %q) = %v, want %v",
					tc.new, tc.old, got, tc.wantMatch)
			}
		})
	}
}
