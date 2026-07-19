// internal/consumername/consumername_test.go
// Pure unit tests for the consumer-naming helpers. No embedded NATS, no
// JetStream — these helpers are deliberately NATS-free so they can be
// exercised in isolation and reused by the collision precheck and the
// bridge poll path.
package consumername

import (
	"testing"
	"time"
)

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
		{"groups_branch_simple", "render", "gpu", "workers-render-gpu"},
		{"groups_branch_dotted_group", "render", "gpu.fast",
			"workers-render-gpu-fast"},
		{"groups_branch_safe_escape", "render", "gpu*1",
			"workers-render-gpu_1"},
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

func TestFilterFor(t *testing.T) {
	cases := []struct {
		name            string
		taskType, group string
		want            string
	}{
		{"default_branch", "render", "", "task.render.>"},
		{"default_branch_dotted_task", "render.gpu", "",
			"task.render.gpu.>"},
		{"groups_branch", "render", "gpu", "task.render.gpu.>"},
		{"groups_branch_hyphenated", "nasr-ingest", "fastlane",
			"task.nasr-ingest.fastlane.>"},
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

func TestFilterFor_RejectsEmptyTaskType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty taskType, got none")
		}
	}()
	FilterFor("", "")
}
