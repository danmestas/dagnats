// worker/consumer_naming_test.go
// Pure unit tests for the consumer-naming helpers. No embedded NATS, no
// JetStream — these helpers are deliberately NATS-free so they can be
// exercised in isolation and reused by the collision precheck.
package worker

import (
	"testing"
	"time"
)

func TestSanitizeConsumerName(t *testing.T) {
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
			got := sanitizeConsumerName(tc.in)
			if got != tc.want {
				t.Fatalf("sanitizeConsumerName(%q) = %q, want %q",
					tc.in, got, tc.want)
			}
			if got == "" {
				t.Fatalf("sanitizeConsumerName(%q) returned empty", tc.in)
			}
		})
	}
}

func TestSanitizeConsumerName_PanicsOnEmpty(t *testing.T) {
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
	sanitizeConsumerName("")
}

func TestDefaultAckWait_IsFiveMinutes(t *testing.T) {
	if defaultAckWait != 5*time.Minute {
		t.Fatalf("defaultAckWait = %v, want %v", defaultAckWait, 5*time.Minute)
	}
	if defaultAckWait <= 0 {
		t.Fatalf("defaultAckWait must be positive, got %v", defaultAckWait)
	}
}
